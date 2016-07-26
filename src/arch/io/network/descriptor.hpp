#ifndef ARCH_IO_NETWORK_DESCRIPTOR_HPP_
#define ARCH_IO_NETWORK_DESCRIPTOR_HPP_

#include "arch/io/network/secure.hpp"

// ATN is this class needed?
// ATN rename no linux_
class linux_tcp_conn_descriptor_t {
public:
    ~linux_tcp_conn_descriptor_t();

    void make_server_connection(
        tls_ctx_t *tls_ctx, scoped_ptr_t<buffered_conn_t> *tcp_conn, signal_t *closer)
        THROWS_ONLY(crypto::openssl_error_t, interrupted_exc_t);

    // Must get called exactly once during lifetime of this object.
    // Call it on the thread you'll use the server connection on.
    void make_server_connection(
        tls_ctx_t *tls_ctx, buffered_conn_t **tcp_conn_out, signal_t *closer)
        THROWS_ONLY(crypto::openssl_error_t, interrupted_exc_t);

private:
    friend class linux_nonthrowing_tcp_listener_t;

    explicit linux_tcp_conn_descriptor_t(fd_t fd);

private:
    fd_t fd_;

    DISABLE_COPYING(linux_tcp_conn_descriptor_t);
};

#endif // ARCH_IO_NETWORK_DESCRIPTOR_HPP_
