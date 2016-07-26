#include "arch/io/network/buffered.hpp"

#include "arch/io/io_utils.hpp"
#include "arch/io/network/secure.hpp"
#include "arch/io/network/tcp.hpp"

scoped_ptr_t<bufferable_conn_t> make_conn(tls_ctx_t *tls_ctx, scoped_fd_t &&sock, signal_t *closer) {
#ifdef ENABLE_TLS
    if (tls_ctx != nullptr) {
        return make_scoped<secure_tcp_conn_t>(tls_ctx, std::move(sock), closer);
    }
#endif
    return make_scoped<tcp_conn_t>(std::move(sock));
}

buffered_conn_t::buffered_conn_t(tls_ctx_t *tls_ctx, scoped_fd_t &&sock, signal_t *closer) 
    THROWS_ONLY(crypto::openssl_error_t, interrupted_exc_t) :
        buffered_conn_t(make_conn(tls_ctx, std::move(sock), closer)) { }


buffered_conn_t::buffered_conn_t(scoped_ptr_t<bufferable_conn_t> conn) :
    base_conn(std::move(conn)),
    read_in_progress(false),
    write_in_progress(false),
    read_buffer(IO_BUFFER_SIZE),
    write_handler(this),
    write_queue_limiter(WRITE_QUEUE_MAX_SIZE),
    write_coro_pool(1, &write_queue, &write_handler),
    current_write_buffer(get_write_buffer()),
    drainer(new auto_drainer_t) { }

buffered_conn_t::write_buffer_t * buffered_conn_t::get_write_buffer() {
    write_buffer_t *buffer;

    if (unused_write_buffers.empty()) {
        buffer = new write_buffer_t;
    } else {
        buffer = unused_write_buffers.head();
        unused_write_buffers.pop_front();
    }
    buffer->size = 0;
    return buffer;
}

buffered_conn_t::write_queue_op_t * buffered_conn_t::get_write_queue_op() {
    write_queue_op_t *op;

    if (unused_write_queue_ops.empty()) {
        op = new write_queue_op_t;
    } else {
        op = unused_write_queue_ops.head();
        unused_write_queue_ops.pop_front();
    }
    return op;
}

void buffered_conn_t::release_write_buffer(write_buffer_t *buffer) {
    unused_write_buffers.push_front(buffer);
}

void buffered_conn_t::release_write_queue_op(write_queue_op_t *op) {
    op->keepalive = auto_drainer_t::lock_t();
    unused_write_queue_ops.push_front(op);
}

size_t buffered_conn_t::read_some(void *buf, size_t size, signal_t *closer) THROWS_ONLY(tcp_conn_read_closed_exc_t) {
    rassert(size > 0);
    read_op_wrapper_t sentry(this, closer);

    if (read_buffer.size()) {
        /* Return the data from the peek buffer */
        size_t read_buffer_bytes = std::min(read_buffer.size(), size);
        memcpy(buf, read_buffer.data(), read_buffer_bytes);
        read_buffer.erase_front(read_buffer_bytes);
        return read_buffer_bytes;
    } else {
        /* Go to the kernel _once_. */
        return base_conn->read_internal(buf, size);
    }
}

void buffered_conn_t::read(void *buf, size_t size, signal_t *closer) THROWS_ONLY(tcp_conn_read_closed_exc_t) {
    read_op_wrapper_t sentry(this, closer);

    /* First, consume any data in the peek buffer */
    int read_buffer_bytes = std::min(read_buffer.size(), size);
    memcpy(buf, read_buffer.data(), read_buffer_bytes);
    read_buffer.erase_front(read_buffer_bytes);
    buf = reinterpret_cast<void *>(reinterpret_cast<char *>(buf) + read_buffer_bytes);
    size -= read_buffer_bytes;

    /* Now go to the kernel for any more data that we need */
    while (size > 0) {
        size_t delta = base_conn->read_internal(buf, size);
        rassert(delta <= size);
        buf = reinterpret_cast<void *>(reinterpret_cast<char *>(buf) + delta);
        size -= delta;
    }
}

void buffered_conn_t::read_buffered(void *buf, size_t size, signal_t *closer)
        THROWS_ONLY(tcp_conn_read_closed_exc_t) {
    while (size > 0) {
        const_charslice read_data = peek();
        if (read_data.end == read_data.beg) {
            // We didn't get anything from the read buffer. Get some data from
            // the underlying socket...
            // For large reads, we read directly into buf to avoid an additional copy
            // and additional round trips.
            // For smaller reads, we use `read_more_buffered` to read into the
            // connection's internal buffer and then copy out whatever we can use
            // to satisfy the current request.
            if (size >= IO_BUFFER_SIZE) {
                return read(buf, size, closer);
            } else {
                read_more_buffered(closer);
                read_data = peek();
            }
        }
        size_t num_read = read_data.end - read_data.beg;
        if (num_read > size) {
            num_read = size;
        }
        rassert(num_read > 0);
        memcpy(buf, read_data.beg, num_read);
        // Remove the consumed data from the read buffer
        pop(num_read, closer);

        size -= num_read;
        buf = static_cast<char *>(buf) + num_read;
    }
}

void buffered_conn_t::read_more_buffered(signal_t *closer) THROWS_ONLY(tcp_conn_read_closed_exc_t) {
    read_op_wrapper_t sentry(this, closer);

    size_t old_size = read_buffer.size();
    read_buffer.resize(old_size + IO_BUFFER_SIZE);
    size_t delta = base_conn->read_internal(read_buffer.data() + old_size, IO_BUFFER_SIZE);

    read_buffer.resize(old_size + delta);
}

const_charslice buffered_conn_t::peek() const THROWS_ONLY(tcp_conn_read_closed_exc_t) {
    assert_thread();
    rassert(!read_in_progress);   // Is there a read already in progress?
    if (!is_read_open()) {
        throw tcp_conn_read_closed_exc_t();
    }

    return const_charslice(read_buffer.data(), read_buffer.data() + read_buffer.size());
}

const_charslice buffered_conn_t::peek(size_t size, signal_t *closer) THROWS_ONLY(tcp_conn_read_closed_exc_t) {
    while (read_buffer.size() < size) {
        read_more_buffered(closer);
    }
    return const_charslice(read_buffer.data(), read_buffer.data() + size);
}

void buffered_conn_t::pop(size_t len, signal_t *closer) THROWS_ONLY(tcp_conn_read_closed_exc_t) {
    assert_thread();
    rassert(!read_in_progress);
    if (!is_read_open()) {
        throw tcp_conn_read_closed_exc_t();
    }

    peek(len, closer);
    read_buffer.erase_front(len);
}

buffered_conn_t::write_handler_t::write_handler_t(buffered_conn_t *_parent) :
    parent(_parent)
{ }

void buffered_conn_t::write_handler_t::coro_pool_callback(write_queue_op_t *operation, UNUSED signal_t *interruptor) {
    if (operation->buffer != nullptr) {
        parent->base_conn->perform_write(operation->buffer, operation->size);
        if (operation->dealloc != nullptr) {
            parent->release_write_buffer(operation->dealloc);
            parent->write_queue_limiter.unlock(operation->size);
        }
    }

    if (operation->cond != nullptr) {
        operation->cond->pulse();
    }
    if (operation->dealloc != nullptr) {
        parent->release_write_queue_op(operation);
    }
}

void buffered_conn_t::internal_flush_write_buffer() {
    write_queue_op_t *op = get_write_queue_op();
    assert_thread();
    rassert(write_in_progress);

    /* Swap in a new write buffer, and set up the old write buffer to be
       released once the write is over. */
    op->buffer = current_write_buffer->buffer;
    op->size = current_write_buffer->size;
    op->dealloc = current_write_buffer.release();
    op->cond = nullptr;
    op->keepalive = auto_drainer_t::lock_t(drainer.get());
    current_write_buffer.init(get_write_buffer());

    /* Acquire the write semaphore so the write queue doesn't get too long
       to be released once the write is completed by the coroutine pool */
    rassert(op->size <= WRITE_CHUNK_SIZE);
    rassert(WRITE_CHUNK_SIZE < WRITE_QUEUE_MAX_SIZE);
    write_queue_limiter.co_lock(op->size);

    write_queue.push(op);
}

void buffered_conn_t::write(const void *buf, size_t size, signal_t *closer) THROWS_ONLY(tcp_conn_write_closed_exc_t) {
    write_op_wrapper_t sentry(this, closer);

    write_queue_op_t op;
    cond_t to_signal_when_done;

    /* Flush out any data that's been buffered, so that things don't get out of order */
    if (current_write_buffer->size > 0) {
        internal_flush_write_buffer();
    }

    /* Don't bother acquiring the write semaphore because we're going to block
       until the write is done anyway */

    /* Enqueue the write so it will happen eventually */
    op.buffer = buf;
    op.size = size;
    op.dealloc = nullptr;
    op.cond = &to_signal_when_done;
    write_queue.push(&op);

    /* Wait for the write to be done. If the write half of the network connection
       is closed before or during our write, then `perform_write()` will turn into a
       no-op, so the cond will still get pulsed. */
    to_signal_when_done.wait();

    if (!is_write_open()) {
        throw tcp_conn_write_closed_exc_t();
    }
}

void buffered_conn_t::write_buffered(const void *vbuf, size_t size, signal_t *closer) THROWS_ONLY(tcp_conn_write_closed_exc_t) {
    write_op_wrapper_t sentry(this, closer);

    /* Convert to `char` for ease of pointer arithmetic */
    const char *buf = reinterpret_cast<const char *>(vbuf);

    while (size > 0) {
        /* Stop putting more things on the write queue if it's already closed. */
        if (!is_write_open()) {
            throw tcp_conn_write_closed_exc_t();
        }

        /* Insert the largest chunk that fits in this block */
        size_t chunk = std::min(size, WRITE_CHUNK_SIZE - current_write_buffer->size);

        memcpy(current_write_buffer->buffer + current_write_buffer->size, buf, chunk);
        current_write_buffer->size += chunk;

        rassert(current_write_buffer->size <= WRITE_CHUNK_SIZE);
        if (current_write_buffer->size == WRITE_CHUNK_SIZE) {
            internal_flush_write_buffer();
        }

        buf += chunk;
        size -= chunk;
    }

    if (!is_write_open()) {
        throw tcp_conn_write_closed_exc_t();
    }
}

void buffered_conn_t::writef(signal_t *closer, const char *format, ...) THROWS_ONLY(tcp_conn_write_closed_exc_t) {
    va_list ap;
    va_start(ap, format);

    printf_buffer_t b(ap, format);
    write(b.data(), b.size(), closer);

    va_end(ap);
}

void buffered_conn_t::flush_buffer(signal_t *closer) THROWS_ONLY(tcp_conn_write_closed_exc_t) {
    write_op_wrapper_t sentry(this, closer);

    /* Flush the write buffer; it might be half-full. */
    if (current_write_buffer->size > 0) {
        internal_flush_write_buffer();
    }

    /* Wait until we know that the write buffer has gone out over the network.
       If the write half of the connection is closed, then the call to
       `perform_write()` that `internal_flush_write_buffer()` will turn into a no-op,
       but the queue will continue to be pumped and so our cond will still get
       pulsed. */
    write_queue_op_t op;
    cond_t to_signal_when_done;
    op.buffer = nullptr;
    op.dealloc = nullptr;
    op.cond = &to_signal_when_done;
    write_queue.push(&op);
    to_signal_when_done.wait();

    if (!is_write_open()) {
        throw tcp_conn_write_closed_exc_t();
    }
}

void buffered_conn_t::flush_buffer_eventually(signal_t *closer) THROWS_ONLY(tcp_conn_write_closed_exc_t) {
    write_op_wrapper_t sentry(this, closer);

    /* Flush the write buffer; it might be half-full. */
    if (current_write_buffer->size > 0) {
        internal_flush_write_buffer();
    }

    if (!is_write_open()) {
        throw tcp_conn_write_closed_exc_t();
    }
}

buffered_conn_t::~buffered_conn_t() THROWS_NOTHING {
    assert_thread();

    // Tell the readers and writers to stop.  The auto drainer will
    // wait for them to stop.
    if (is_read_open()) {
        shutdown_read();
    }
    if (is_write_open()) {
        shutdown_write();
    }
}

void buffered_conn_t::rethread(threadnum_t new_thread) {
    rassert(!read_in_progress);
    rassert(!write_in_progress);
    base_conn->rethread(new_thread);
    write_coro_pool.rethread(new_thread);

    if (drainer.has()) {
        drainer->rethread(new_thread);
    }

    real_home_thread = new_thread;
}

bool buffered_conn_t::getpeername(ip_and_port_t *ip_and_port) {
    return base_conn->getpeername(ip_and_port);
}

bool buffered_conn_t::is_read_open() const {
    return base_conn->is_read_open();
}

bool buffered_conn_t::is_write_open() const {
    return base_conn->is_read_open();
}

void buffered_conn_t::shutdown_read() {
    base_conn->shutdown_read();
}

void buffered_conn_t::shutdown_write() {
    base_conn->shutdown_write();
}

void buffered_conn_t::enable_keepalive() {
    base_conn->enable_keepalive();
}

scoped_signal_t buffered_conn_t::rdhup_watcher() {
    return base_conn->rdhup_watcher();
}

