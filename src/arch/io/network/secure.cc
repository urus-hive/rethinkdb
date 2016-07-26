#include "arch/io/network/secure.hpp"

#include "arch/timing.hpp"
#include "concurrency/wait_any.hpp"
#include "logger.hpp"

#ifdef ENABLE_TLS
tls_conn_wrapper_t::tls_conn_wrapper_t(SSL_CTX *tls_ctx)
    THROWS_ONLY(crypto::openssl_error_t) {
    ERR_clear_error();

    conn = SSL_new(tls_ctx);
    if (nullptr == conn) {
        throw crypto::openssl_error_t(ERR_get_error());
    }

    // Add support for partial writes.
    SSL_set_mode(conn, SSL_MODE_ENABLE_PARTIAL_WRITE);
}

tls_conn_wrapper_t::~tls_conn_wrapper_t() {
    SSL_free(conn);
}

// Set the underlying IO.
void tls_conn_wrapper_t::set_fd(fd_t sock)
    THROWS_ONLY(crypto::openssl_error_t) {
    if (0 == SSL_set_fd(conn, sock)) {
        throw crypto::openssl_error_t(ERR_get_error());
    }
}

/* This is the client version of the constructor. The base class constructor
will establish a TCP connection to the peer at the given host:port and then we
wrap the tcp connection in TLS using the configuration in the given tls_ctx. */
secure_tcp_conn_t::secure_tcp_conn_t(
        SSL_CTX *tls_ctx, const ip_address_t &host, int port,
        signal_t *interruptor, int local_port)
        THROWS_ONLY(connect_failed_exc_t, crypto::openssl_error_t, interrupted_exc_t) :
    transport(host, port, interruptor, local_port),
    conn(tls_ctx) {

    conn.set_fd(transport.sock.get());
    SSL_set_connect_state(conn.get());
    perform_handshake(interruptor);
}

/* This is the server version of the constructor */
secure_tcp_conn_t::secure_tcp_conn_t(
        SSL_CTX *tls_ctx, fd_t _sock, signal_t *interruptor)
        THROWS_ONLY(crypto::openssl_error_t, interrupted_exc_t) :
    transport(_sock),
    conn(tls_ctx) {

    conn.set_fd(transport.sock.get());
    SSL_set_accept_state(conn.get());
    perform_handshake(interruptor);
}

secure_tcp_conn_t::~secure_tcp_conn_t() THROWS_NOTHING {
    transport.assert_thread();

    if (is_open()) shutdown();
}

void secure_tcp_conn_t::rethread(threadnum_t thread) {
    closed.rethread(thread);

    transport.rethread(thread);
}

void secure_tcp_conn_t::perform_handshake(signal_t *interruptor)
        THROWS_ONLY(crypto::openssl_error_t, interrupted_exc_t) {
    // Perform TLS handshake.
    while (true) {
        ERR_clear_error();
        int ret = SSL_do_handshake(conn.get());

        if (ret > 0) {
            return; // Successful TLS handshake.
        }

        if (ret == 0) {
            // The handshake failed but the connection shut down cleanly.
            throw crypto::openssl_error_t(ERR_get_error());
        }

        switch (SSL_get_error(conn.get(), ret)) {
        case SSL_ERROR_WANT_READ:
            /* The handshake needs to read data, but the underlying I/O has no data
            ready to read. Wait for it to be ready or for an interrupt signal. */
            {
                linux_event_watcher_t::watch_t watch(transport.get_event_watcher(), poll_event_in);
                wait_interruptible(&watch, interruptor);
            }
            break;
        case SSL_ERROR_WANT_WRITE:
            /* The handshake needs to write data, but the underlying I/O is not ready
            to write. Wait for it to be ready or for an interrupt signal. */
            {
                linux_event_watcher_t::watch_t watch(transport.get_event_watcher(), poll_event_out);
                wait_interruptible(&watch, interruptor);
            }
            break;
        default:
            // Some other error with the underlying I/O.
            throw crypto::openssl_error_t(ERR_get_error());
        }

        if (interruptor->is_pulsed()) {
            // The handshake cannot continue because we need to shutdown now.
            throw interrupted_exc_t();
        }

        /* Go around the loop and try to complete the handshake. */
    }
}

size_t secure_tcp_conn_t::read_internal(void *buffer, size_t size)
    THROWS_ONLY(tcp_conn_read_closed_exc_t) {
    transport.assert_thread();
    rassert(!closed.is_pulsed());

    while(true) {
        ERR_clear_error();

        int ret = SSL_read(conn.get(), buffer, size);

        if (ret > 0) {
            return ret; // Operation successful, returns number of bytes read.
        }

        switch (SSL_get_error(conn.get(), ret)) {
        case SSL_ERROR_ZERO_RETURN:
            // Indicates that the peer has sent the "close notify" alert. The
            // shutdown state is currently SSL_RECEIVED_SHUTDOWN. We must now
            // send our "close notify" alert.
            shutdown();
            throw tcp_conn_read_closed_exc_t();
        case SSL_ERROR_WANT_READ:
            /* The underlying I/O has no data ready to read. Wait for it to be
            ready or for someone to send a close signal. */
            {
                linux_event_watcher_t::watch_t watch(
                    transport.get_event_watcher(), poll_event_in);
                wait_any_t waiter(&watch, &closed);
                waiter.wait_lazily_unordered();
            }
            break;
        case SSL_ERROR_WANT_WRITE:
            /* Though we are reading, a TLS renegotiation may occur at any time
            requiring a write. Wait for the underyling I/O to be ready for a
            write, or for someone to send a close signal. */
            {
                linux_event_watcher_t::watch_t watch(
                    transport.get_event_watcher(), poll_event_out);
                wait_any_t waiter(&watch, &closed);
                waiter.wait_lazily_unordered();
            }
            break;
        default:
            // Some other error. Assume that the connection is unusable.
            shutdown_socket();
            throw tcp_conn_read_closed_exc_t();
        }

        if (closed.is_pulsed()) {
            /* We were closed for whatever reason. Whatever signalled us has
            already called shutdown_socket(). */
            throw tcp_conn_read_closed_exc_t();
        }

        /* Go around the loop and try to read again */
    }
}

void secure_tcp_conn_t::perform_write(const void *buffer, size_t size) {
    transport.assert_thread();

    if (closed.is_pulsed()) {
        /* The connection was closed, but there are still operations in the
        write queue; we are one of those operations. Just don't do anything. */
        return;
    }

    // Loop for retrying if the underlying socket would block and to retry on
    // partial writes.
    while (size > 0) {
        ERR_clear_error();

        int ret = SSL_write(conn.get(), buffer, size);

        if (ret > 0) {
            // Operation successful, returns number of bytes written.
            rassert(static_cast<size_t>(ret) <= size);
            size -= ret;

            // Slide down the buffer.
            buffer = reinterpret_cast<const void *>(
                reinterpret_cast<const char *>(buffer) + ret);

            if (write_perfmon) write_perfmon->record(ret);

            // Go around the loop again if there is more data to write.
            continue;
        }

        switch (SSL_get_error(conn.get(), ret)) {
        case SSL_ERROR_ZERO_RETURN:
            // Indicates that the peer has sent the "close notify" alert. The
            // shutdown state is currently SSL_RECEIVED_SHUTDOWN. We must now
            // send our "close notify" alert.
            shutdown();
            return;
        case SSL_ERROR_WANT_READ:
            /* Though we are writing, a TLS renegotiation may occur at any time
            requiring a read. Wait for the underyling I/O to be ready for a
            read, or for someone to send a close signal. */
            {
                linux_event_watcher_t::watch_t watch(transport.get_event_watcher(), poll_event_in);
                wait_any_t waiter(&watch, &closed);
                waiter.wait_lazily_unordered();
            }
            break;
        case SSL_ERROR_WANT_WRITE:
            /* The underlying I/O is not ready to accept a write. Wait for it
            to be ready or for someone to send a close signal. */
            {
                linux_event_watcher_t::watch_t watch(
                    transport.get_event_watcher(), poll_event_out);
                wait_any_t waiter(&watch, &closed);
                waiter.wait_lazily_unordered();
            }
            break;
        default:
            // Some other error. Assume that the connection is unusable.
            shutdown_socket();
            return;
        }

        if (closed.is_pulsed()) {
            /* We were closed for whatever reason. Whatever signalled
            us has already called shutdown_socket(). */
            return;
        }

        /* Go around the loop and try to read again */
    }
}

/* It is not possible to close only the read or write side of a TLS connection
so we use only a single shutdown method which attempts to shutdown the TLS
before shutting down the underlying tcp connection */
void secure_tcp_conn_t::shutdown() {
    transport.assert_thread();

    // If something else already shut us down, abort immediately.
    if (closed.is_pulsed()) {
        return;
    }

    // Wait at most 5 seconds for the orderly shutdown. If it doesn't complete by then,
    // we simply shutdown the socket.
    signal_timer_t shutdown_timeout(5000);

    bool skip_shutdown = false; // Set to true if TLS shutdown encounters an error.

    // If we have not already received a "close notify" alert from the peer,
    // then we should send one. If we have received one, this loop will respond
    // with our own "close notify" alert.
    while (!(skip_shutdown || shutdown_timeout.is_pulsed())) {
        ERR_clear_error();

        int ret = SSL_shutdown(conn.get());

        if (ret > 0) {
            // "close notify" has been sent and received.
            break;
        }

        if (ret == 0) {
            // "close notify" has been sent but not yet received from peer.
            continue;
        }

        switch(SSL_get_error(conn.get(), ret)) {
        case SSL_ERROR_WANT_READ:
            {
                /* The shutdown needs to read data, but the underlying I/O has no data
                ready to read. Wait for it to be ready or for a timeout. */
                linux_event_watcher_t::watch_t watch(transport.get_event_watcher(), poll_event_in);
                wait_any_t waiter(&watch, &shutdown_timeout);
                waiter.wait_lazily_unordered();
            }
            continue;
        case SSL_ERROR_WANT_WRITE:
            {
                /* The handshake needs to write data, but the underlying I/O is not ready
                to write. Wait for it to be ready or for a timeout. */
                linux_event_watcher_t::watch_t watch(
                    transport.get_event_watcher(), poll_event_out);
                wait_any_t waiter(&watch, &shutdown_timeout);
                waiter.wait_lazily_unordered();
            }
            continue;
        default:
            // Unable to perform clean shutdown. Just skip it.
            skip_shutdown = true;
        }
    }

    shutdown_socket();
}

void secure_tcp_conn_t::shutdown_socket() {
    transport.assert_thread();
    rassert(!closed.is_pulsed());
    rassert(!transport.read_closed.is_pulsed());
    rassert(!transport.write_closed.is_pulsed());

    // Shutdown the underlying TCP connection.
    int res = ::shutdown(transport.sock.get(), SHUT_RDWR);
    if (res != 0 && get_errno() != ENOTCONN) {
        logERR(
            "Could not shutdown socket for reading and writing: %s",
            errno_string(get_errno()).c_str());
    }

    closed.pulse();
    transport.on_shutdown_write();
    transport.on_shutdown_read();
}

bool secure_tcp_conn_t::getpeername(ip_and_port_t *ip_and_port) {
    return transport.getpeername(ip_and_port);
}

void secure_tcp_conn_t::enable_keepalive() {
    transport.enable_keepalive();
}

scoped_signal_t secure_tcp_conn_t::rdhup_watcher() {
    return make_scoped_signal<cond_t>();
}

#endif /* ENABLE_TLS */
