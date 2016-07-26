#ifndef ARCH_IO_NETWORK_SECURE_HPP_
#define ARCH_IO_NETWORK_SECURE_HPP_

#ifdef ENABLE_TLS

#include "arch/io/network/tcp.hpp"
#include "arch/io/openssl.hpp"
#include "crypto/error.hpp"

/* tls_conn_wrapper_t wraps a TLS connection. */
class tls_conn_wrapper_t {
public:
    explicit tls_conn_wrapper_t(SSL_CTX *tls_ctx) THROWS_ONLY(crypto::openssl_error_t);

    ~tls_conn_wrapper_t();

    void set_fd(fd_t sock) THROWS_ONLY(crypto::openssl_error_t);

    SSL *get() { return conn; }

private:
    SSL *conn;

    DISABLE_COPYING(tls_conn_wrapper_t);
};

class secure_tcp_conn_t :
    public bufferable_conn_t {
public:

    // Client connection constructor.
    secure_tcp_conn_t(
        SSL_CTX *tls_ctx, const ip_address_t &host, int port,
        signal_t *interruptor, int local_port = ANY_PORT
    ) THROWS_ONLY(connect_failed_exc_t, crypto::openssl_error_t, interrupted_exc_t);

    ~secure_tcp_conn_t() THROWS_NOTHING;

    /* shutdown_read() and shutdown_write() just close the socket rather than performing
    the full TLS shutdown procedure, because they're assumed not to block.
    Our usual `shutdown` does block and it also assumes that no other operation is
    currently ongoing on the connection, which we can't guarantee here. */
    void shutdown_read() { shutdown_socket(); }
    void shutdown_write() { shutdown_socket(); }

    void rethread(threadnum_t thread);

    // Server connection constructor.
    secure_tcp_conn_t(
        SSL_CTX *tls_ctx, scoped_fd_t &&_sock, signal_t *interruptor
    ) THROWS_ONLY(crypto::openssl_error_t, interrupted_exc_t);

    bool getpeername(ip_and_port_t *ip_and_port);

    bool is_read_open()  const { return is_open(); }
    bool is_write_open() const { return is_open(); }

    void enable_keepalive();

    scoped_signal_t rdhup_watcher();

private:

    void perform_handshake(signal_t *interruptor) THROWS_ONLY(
        crypto::openssl_error_t, interrupted_exc_t);

    /* Reads up to the given number of bytes, but not necessarily that many. Simple
    wrapper around ::read(). Returns the number of bytes read or throws
    tcp_conn_read_closed_exc_t. Bypasses read_buffer. */
    size_t read_internal(void *buffer, size_t size) THROWS_ONLY(
        tcp_conn_read_closed_exc_t);

    /* Used to actually perform a write. If the write end of the connection is open, then
    writes `size` bytes from `buffer` to the socket. */
    void perform_write(const void *buffer, size_t size);

    void shutdown();
    void shutdown_socket();

    bool is_open() const { return !closed.is_pulsed(); }

    tcp_conn_t transport;

    tls_conn_wrapper_t conn;

    cond_t closed;
};

#endif /* ENABLE_TLS */

#endif // ARCH_IO_NETWORK_SECURE_HPP_
