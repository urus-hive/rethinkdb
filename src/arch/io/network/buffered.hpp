#ifndef ARCH_IO_NETWORK_BUFFERED_HPP_
#define ARCH_IO_NETWORK_BUFFERED_HPP_

#include "arch/types.hpp"
#include "arch/io/network/bufferable.hpp"
#include "concurrency/auto_drainer.hpp"
#include "concurrency/cond_var.hpp"
#include "concurrency/coro_pool.hpp"
#include "concurrency/queue/unlimited_fifo.hpp"
#include "concurrency/semaphore.hpp"
#include "containers/lazy_erase_vector.hpp"
#include "containers/scoped.hpp"
#include "threading.hpp"

// ATN TODO: inherit from bufferable
class buffered_conn_t : public home_thread_mixin_t {
public:

    explicit buffered_conn_t(scoped_ptr_t<conn_t> conn);

    void enable_keepalive();

    scoped_signal_t rdhup_watcher();

    /* Reading */

    /* If you know beforehand how many bytes you want to read, use read() with a
    byte buffer. Returns when the buffer is full, or throws tcp_conn_read_closed_exc_t.
    If `closer` is pulsed, throws `read_closed_exc_t` and also closes the read
    half of the connection. */
    void read(void *buf, size_t size, signal_t *closer)
        THROWS_ONLY(tcp_conn_read_closed_exc_t);

    /* This is a convenience function around `read_more_buffered`, `peek` and `pop` that
    behaved like a normal `read`, but uses buffering internally.
    In many cases you'll want to use this instead of `read`, especially when reading
    a lot of small values. */
    void read_buffered(void *buf, size_t size, signal_t *closer)
        THROWS_ONLY(tcp_conn_read_closed_exc_t);

    // If you don't know how many bytes you want to read, but still
    // masochistically want to handle buffering yourself.  Makes at
    // most one call to ::read(), reads some data or throws
    // read_closed_exc_t. read_some() is guaranteed to return at least
    // one byte of data unless it throws read_closed_exc_t.
    size_t read_some(void *buf, size_t size, signal_t *closer)
         THROWS_ONLY(tcp_conn_read_closed_exc_t);

    // If you don't know how many bytes you want to read, use peek()
    // and then, if you're satisfied, pop what you've read, or if
    // you're unsatisfied, read_more_buffered() and then try again.
    // Note that you should always call peek() before calling
    // read_more_buffered(), because there might be leftover data in
    // the peek buffer that might be enough for you.
    const_charslice peek() const THROWS_ONLY(tcp_conn_read_closed_exc_t);

    //you can also peek with a specific size (this is really just convenient
    //for some things and can in some cases avoid an unneeded copy
    const_charslice peek(size_t size, signal_t *closer)
        THROWS_ONLY(tcp_conn_read_closed_exc_t);

    void pop(size_t len, signal_t *closer) THROWS_ONLY(tcp_conn_read_closed_exc_t);

    void read_more_buffered(signal_t *closer) THROWS_ONLY(tcp_conn_read_closed_exc_t);

    /* Returns false if the half of the pipe that goes from the peer to us has been closed. */
    bool is_read_open() const;

    /* Call shutdown_read() to close the half of the pipe that goes from the peer to us. If there
    is an outstanding read() or peek_until() operation, it will throw tcp_conn_read_closed_exc_t. */
    void shutdown_read();

    /* Writing */

    /* write() writes 'size' bytes from 'buf' to the socket and blocks until it
    is done. Throws tcp_conn_write_closed_exc_t if the write half of the pipe is closed
    before we can finish. If `closer` is pulsed, closes the write half of the
    pipe and throws `tcp_conn_write_closed_exc_t`. */
    void write(const void *buf, size_t size, signal_t *closer)
        THROWS_ONLY(tcp_conn_write_closed_exc_t);

    /* write_buffered() is like write(), but it might not send the data until
    flush_buffer*() or write() is called. Internally, it bundles together the
    buffered writes; this may improve performance. */
    void write_buffered(const void *buf, size_t size, signal_t *closer)
        THROWS_ONLY(tcp_conn_write_closed_exc_t);

    void writef(signal_t *closer, const char *format, ...)
        THROWS_ONLY(tcp_conn_write_closed_exc_t) ATTR_FORMAT(printf, 3, 4);

    // Blocks until flush is done
    void flush_buffer(signal_t *closer)
        THROWS_ONLY(tcp_conn_write_closed_exc_t);
    // Blocks only if the queue is backed up
    void flush_buffer_eventually(signal_t *closer)
        THROWS_ONLY(tcp_conn_write_closed_exc_t);

    /* Call shutdown_write() to close the half of the pipe that goes from us to the peer. If there
    is a write currently happening, it will get tcp_conn_write_closed_exc_t. */
    void shutdown_write();

    /* Returns false if the half of the pipe that goes from us to the peer has been closed. */
    bool is_write_open() const;

    ~buffered_conn_t() THROWS_NOTHING;

    void rethread(threadnum_t thread);

    bool getpeername(ip_and_port_t *ip_and_port);

private:

    /* `read_op_wrapper_t` and `write_op_wrapper_t` are an attempt to factor out
    the boilerplate that would otherwise be at the top of every read- or write-
    related method on `linux_tcp_conn_t`. They check that the connection is
    open, handle `closer`, etc. */

    class read_op_wrapper_t : private signal_t::subscription_t {
    public:
        read_op_wrapper_t(buffered_conn_t *p, signal_t *closer) : parent(p) {
            parent->assert_thread();
            rassert(!parent->read_in_progress);
            if (closer->is_pulsed()) {
                if (parent->is_read_open()) {
                    parent->shutdown_read();
                }
            } else {
                this->reset(closer);
            }
            if (!parent->is_read_open()) {
                throw tcp_conn_read_closed_exc_t();
            }
            parent->read_in_progress = true;
        }
        ~read_op_wrapper_t() {
            parent->read_in_progress = false;
        }
    private:
        void run() override {
            if (parent->is_read_open()) {
                parent->shutdown_read();
            }
        }
        buffered_conn_t *parent;
    };

    class write_op_wrapper_t : private signal_t::subscription_t {
    public:
        write_op_wrapper_t(buffered_conn_t *p, signal_t *closer) : parent(p) {
            parent->assert_thread();
            rassert(!parent->write_in_progress);
            if (closer->is_pulsed()) {
                if (parent->is_write_open()) {
                    parent->shutdown_write();
                }
            } else {
                this->reset(closer);
            }
            if (!parent->is_write_open()) {
                throw tcp_conn_write_closed_exc_t();
            }
            parent->write_in_progress = true;
        }
        ~write_op_wrapper_t() {
            parent->write_in_progress = false;
        }
    private:
        void run() override {
            if (parent->is_write_open()) {
                parent->shutdown_write();
            }
        }
        buffered_conn_t *parent;
    };

    scoped_ptr_t<conn_t> base_conn;

    /* True if there is a pending read or write */
    bool read_in_progress, write_in_progress;

    /* Holds data that we read from the socket but hasn't been consumed yet */
    lazy_erase_vector_t<char> read_buffer;

    static const size_t WRITE_QUEUE_MAX_SIZE = 128 * KILOBYTE;
    static const size_t WRITE_CHUNK_SIZE = 8 * KILOBYTE;

    /* Structs to avoid over-using dynamic allocation */
    struct write_buffer_t : public intrusive_list_node_t<write_buffer_t> {
        char buffer[WRITE_CHUNK_SIZE];
        size_t size;
    };

    struct write_queue_op_t : public intrusive_list_node_t<write_queue_op_t> {
        write_buffer_t *dealloc;
        const void *buffer;
        size_t size;
        cond_t *cond;
        auto_drainer_t::lock_t keepalive;
    };

    class write_handler_t : public coro_pool_callback_t<write_queue_op_t*> {
    public:
        explicit write_handler_t(buffered_conn_t *_parent);
    private:
        buffered_conn_t *parent;
        void coro_pool_callback(write_queue_op_t *operation, signal_t *interruptor);
    } write_handler;

    /* Lists of unused buffers, new buffers will be put on this list until needed again, reducing
       the use of dynamic memory.  TODO: decay over time? */
    deleting_intrusive_list_t<write_buffer_t> unused_write_buffers;
    deleting_intrusive_list_t<write_queue_op_t> unused_write_queue_ops;

    write_buffer_t *get_write_buffer();
    write_queue_op_t *get_write_queue_op();
    void release_write_buffer(write_buffer_t *buffer);
    void release_write_queue_op(write_queue_op_t *op);


    /* Schedules old write buffer's contents to be flushed and swaps in a fresh write buffer.
    Blocks until it can acquire the `write_queue_limiter` semaphore, but doesn't wait for
    data to be completely written. */
    void internal_flush_write_buffer();

    /* Used to queue up buffers to write. The functions in `write_queue` will all be
    `std::bind()`s of the `perform_write()` function below. */
    unlimited_fifo_queue_t<write_queue_op_t*, intrusive_list_t<write_queue_op_t> > write_queue;

    /* This semaphore prevents the write queue from getting arbitrarily big. */
    static_semaphore_t write_queue_limiter;

    /* Used to actually perform the writes. Only has one coroutine in it, which will call the
    handle_write_queue callback when operations are ready */
    coro_pool_t<write_queue_op_t *> write_coro_pool;

    /* Buffer we are currently filling up with data that we want to write. When it reaches a
    certain size, we push it onto `write_queue`. */
    scoped_ptr_t<write_buffer_t> current_write_buffer;

    scoped_ptr_t<auto_drainer_t> drainer;
};

#endif // ARCH_IO_NETWORK_BUFFERED_HPP_
