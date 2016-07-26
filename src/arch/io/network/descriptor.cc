#include "arch/io/network/descriptor.hpp"

#include "arch/io/network/buffered.hpp"

#include "logger.hpp"

conn_descriptor_t::conn_descriptor_t(fd_t fd) : fd_(fd) {
    rassert(fd != INVALID_FD);
}

conn_descriptor_t::~conn_descriptor_t() {
    if (fd_ != INVALID_FD) {
#ifdef _WIN32
        int res = closesocket(fd_to_socket(fd_));
        if (res != 0) {
            logERR("Could not close socket: %s", winerr_string(GetLastError()).c_str());
        }
#else
        int res = ::shutdown(fd_, SHUT_RDWR);
        if (res != 0 && get_errno() != ENOTCONN) {
            logERR(
                "Could not shutdown socket for reading and writing: %s",
                errno_string(get_errno()).c_str());
        }
#endif
    }
}

void conn_descriptor_t::make_server_connection(
    tls_ctx_t *tls_ctx, scoped_ptr_t<buffered_conn_t> *tcp_conn, signal_t *closer
) THROWS_ONLY(crypto::openssl_error_t, interrupted_exc_t) {
    // We pass ownership of `fd_` to the connection.
    fd_t sock = fd_;
    fd_ = INVALID_FD;
#ifdef ENABLE_TLS
    if (tls_ctx != nullptr) {
        tcp_conn->init(new buffered_conn_t(make_scoped<secure_tcp_conn_t>(tls_ctx, sock, closer)));
        return;
    }
#endif
    tcp_conn->init(new buffered_conn_t(make_scoped<tcp_conn_t>(sock)));
}

[[deprecated]] // ATN
void conn_descriptor_t::make_server_connection(
    tls_ctx_t *tls_ctx, buffered_conn_t **tcp_conn_out, signal_t *closer
) THROWS_ONLY(crypto::openssl_error_t, interrupted_exc_t) {
    // We pass ownership of `fd_` to the connection.
    fd_t sock = fd_;
    fd_ = INVALID_FD;
#ifdef ENABLE_TLS
    if (tls_ctx != nullptr) {
        *tcp_conn_out = new buffered_conn_t(make_scoped<secure_tcp_conn_t>(tls_ctx, sock, closer));
        return;
    }
#endif
    *tcp_conn_out = new buffered_conn_t(make_scoped<tcp_conn_t>(sock));
}
