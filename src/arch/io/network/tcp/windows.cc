#include "arch/io/network/tcp.hpp"

#ifdef TCP_IMPLEMENTATION_WINDOWS

#include <fcntl.h>
#include <sys/types.h>
#include "windows.hpp"
#include <ws2tcpip.h> // NOLINT
#include <iphlpapi.h> // NOLINT

#include "concurrency/pmap.hpp"
#include "concurrency/wait_any.hpp"
#include "logger.hpp"

#ifdef TRACE_WINSOCK
#define winsock_debugf(...) debugf("winsock: " __VA_ARGS__)
#else
#define winsock_debugf(...) ((void)0)
#endif

LPFN_CONNECTEX get_ConnectEx(fd_t s) {
    static THREAD_LOCAL LPFN_CONNECTEX ConnectEx = nullptr;
    if (ConnectEx == nullptr) {
        DWORD size = 0;
        GUID id = WSAID_CONNECTEX;
        DWORD res = WSAIoctl(fd_to_socket(s), SIO_GET_EXTENSION_FUNCTION_POINTER,
                             &id, sizeof(id), &ConnectEx, sizeof(ConnectEx),
                             &size, nullptr, nullptr);
        guarantee_winerr(res == 0, "WSAIoctl failed");
    }
    return ConnectEx;
}

LPFN_ACCEPTEX get_AcceptEx(fd_t s) {
    static THREAD_LOCAL LPFN_ACCEPTEX AcceptEx = nullptr;
    if (AcceptEx == nullptr) {
        DWORD size = 0;
        GUID id = WSAID_ACCEPTEX;
        DWORD res = WSAIoctl(fd_to_socket(s), SIO_GET_EXTENSION_FUNCTION_POINTER,
                             &id, sizeof(id), &AcceptEx, sizeof(AcceptEx),
                             &size, nullptr, nullptr);
        guarantee_winerr(res == 0, "WSAIoctl failed");
    }
    return AcceptEx;
}

void async_connect(fd_t socket, sockaddr *sa, size_t sa_len,
                   event_watcher_t *event_watcher, signal_t *interuptor) {
    overlapped_operation_t op(event_watcher);
    winsock_debugf("connecting socket %x\n", socket);
    DWORD bytes_sent;
    BOOL res = get_ConnectEx(socket)(fd_to_socket(socket), sa, sa_len, nullptr, 0, &bytes_sent, &op.overlapped);
    DWORD error = GetLastError();
    if (!res && error != ERROR_IO_PENDING) {
        op.set_cancel();
        logERR("connect failed: %s", winerr_string(error).c_str());
        throw conn_t::connect_failed_exc_t(EIO);
    }
    winsock_debugf("waiting for connection on %x\n", socket);
    op.wait_interruptible(interuptor);
    if (op.error != NO_ERROR) {
        logERR("ConnectEx failed: %s", winerr_string(op.error).c_str());
        throw conn_t::connect_failed_exc_t(EIO);
    }
    winsock_debugf("connected %x\n", socket);
}

void connect_ipv4_internal(fd_t socket, int local_port, const in_addr &addr, int port, event_watcher_t *event_watcher, signal_t *interuptor) {
    struct sockaddr_in sa;
    socklen_t sa_len(sizeof(sa));
    memset(&sa, 0, sa_len);
    sa.sin_family = AF_INET;

    sa.sin_port = htons(local_port);
    sa.sin_addr.s_addr = INADDR_ANY;
    // TODO WINDOWS: can bind block when called like this?
    winsock_debugf("binding socket for connect %x\n", socket);
    if (bind(fd_to_socket(socket), reinterpret_cast<sockaddr *>(&sa), sa_len) != 0) {
        logWRN("Failed to bind to local port %d: %s", local_port, winerr_string(GetLastError()).c_str());
    }

    sa.sin_port = htons(port);
    sa.sin_addr = addr;

    async_connect(socket, reinterpret_cast<sockaddr *>(&sa), sa_len, event_watcher, interuptor);
}

void connect_ipv6_internal(fd_t socket, int local_port, const in6_addr &addr, int port, uint32_t scope_id, event_watcher_t *event_watcher, signal_t *interuptor) {
    struct sockaddr_in6 sa;
    socklen_t sa_len(sizeof(sa));
    memset(&sa, 0, sa_len);
    sa.sin6_family = AF_INET6;

    sa.sin6_port = htons(local_port);
    sa.sin6_addr = in6addr_any;
    winsock_debugf("binding socket for connect %x\n", socket);
    if (bind(fd_to_socket(socket), reinterpret_cast<sockaddr *>(&sa), sa_len) != 0) {
        logWRN("Failed to bind to local port %d: %s", local_port, winerr_string(GetLastError()).c_str());
    }

    sa.sin6_port = htons(port);
    sa.sin6_addr = addr;
    sa.sin6_scope_id = scope_id;

    async_connect(socket, reinterpret_cast<sockaddr *>(&sa), sa_len, event_watcher, interuptor);
}

fd_t create_socket_wrapper(int address_family) {
    fd_t res = socket_to_fd(socket(address_family, SOCK_STREAM, IPPROTO_TCP));
    winsock_debugf("new socket %x\n", res);
    if (res == INVALID_FD) {
        DWORD err = GetLastError();
        logERR("Failed to create socket: %s", winerr_string(err).c_str());
        throw conn_t::connect_failed_exc_t(EIO);
    }
    return res;
}


// Network connection object
tcp_conn_t::tcp_conn_t(const ip_address_t &peer,
                                   int port,
                                   signal_t *interruptor,
                                   int local_port) THROWS_ONLY(connect_failed_exc_t, interrupted_exc_t) :
        sock(create_socket_wrapper(peer.get_address_family())),
        event_watcher(new event_watcher_t(sock.get(), this)) {

#ifndef _WIN32
    guarantee_err(fcntl(sock.get(), F_SETFL, O_NONBLOCK) == 0, "Could not make socket non-blocking");
#endif

    if (local_port != 0) {
        // Set the socket to reusable so we don't block out other sockets from this port
        int reuse = 1;
        if (setsockopt(fd_to_socket(sock.get()), SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char*>(&reuse), sizeof(reuse)) != 0)
            logWRN("Failed to set socket reuse to true: %s", errno_string(get_errno()).c_str());
    }
    {
        // Disable Nagle algorithm just as in the listener case
        int sockoptval = 1;
        int res = setsockopt(fd_to_socket(sock.get()), IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&sockoptval), sizeof(sockoptval));
        guarantee_err(res != -1, "Could not set TCP_NODELAY option");
    }

    if (peer.is_ipv4()) {
        connect_ipv4_internal(sock.get(), local_port, peer.get_ipv4_addr(), port, event_watcher.get(), interruptor);
    } else {
        connect_ipv6_internal(sock.get(), local_port, peer.get_ipv6_addr(), port,
                              peer.get_ipv6_scope_id(), event_watcher.get(), interruptor);
    }
}

tcp_conn_t::tcp_conn_t(fd_t s) :
       sock(s),
       event_watcher(new event_watcher_t(sock.get(), this)) {
    rassert(sock.get() != INVALID_FD);
}

void tcp_conn_t::enable_keepalive() {
    int optval = 1;
    int res = setsockopt(fd_to_socket(sock.get()), SOL_SOCKET, SO_KEEPALIVE, reinterpret_cast<char*>(&optval), sizeof(optval));
    guarantee(res != -1, "Could not set SO_KEEPALIVE option.");
}


size_t tcp_conn_t::read_internal(void *buffer, size_t size) THROWS_ONLY(tcp_conn_read_closed_exc_t) {
    assert_thread();
    rassert(!read_closed.is_pulsed());

    overlapped_operation_t op(event_watcher.get());
    winsock_debugf("request read %d bytes on %x\n", size, sock.get());
    // TODO WINDOWS: WSARecv may be more efficient
    BOOL res = ReadFile(sock.get(), buffer, size, nullptr, &op.overlapped);
    DWORD error = GetLastError();
    if (res || error == ERROR_IO_PENDING) {
        op.wait_abortable(&read_closed);
        error = op.error;
    } else {
        op.set_result(0, error);
    }
    if (read_closed.is_pulsed()) {
        throw tcp_conn_read_closed_exc_t();
    }
    if (op.nb_bytes == 0 || error == ERROR_HANDLE_EOF) {
        on_shutdown_read();
        throw tcp_conn_read_closed_exc_t();
    } else if (error != NO_ERROR) {
        logERR("Could not read from socket: %s", winerr_string(error).c_str());
        on_shutdown_read();
        throw tcp_conn_read_closed_exc_t();
    } else {
        winsock_debugf("read complete, %d/%d on %x\n", op.nb_bytes, size, sock.get());
        return op.nb_bytes;
    }
}

void tcp_conn_t::shutdown_read() {
    assert_thread();
    int res = ::shutdown(fd_to_socket(sock.get()), SD_RECEIVE);
    if (res != 0 && GetLastError() != WSAENOTCONN) {
        logERR("Could not shutdown socket for reading: %s", winerr_string(GetLastError()).c_str());
    }
    on_shutdown_read();
}

void tcp_conn_t::on_shutdown_read() {
    assert_thread();
    rassert(!read_closed.is_pulsed());
    read_closed.pulse();
}

bool tcp_conn_t::is_read_open() const {
    assert_thread();
    return !read_closed.is_pulsed();
}


void tcp_conn_t::perform_write(const void *buf, size_t size) {
    assert_thread();

    if (write_closed.is_pulsed()) {
        /* The write end of the connection was closed, but there are still
           operations in the write queue; we are one of those operations. Just
           don't do anything. */
        return;
    }

    overlapped_operation_t op(event_watcher.get());
    WSABUF wsabuf;
    wsabuf.len = size;
    wsabuf.buf = const_cast<char*>(reinterpret_cast<const char*>(buf));
    DWORD flags = 0;
    winsock_debugf("write on %x\n", sock.get());
    int res = WSASend(fd_to_socket(sock.get()), &wsabuf, 1, nullptr, flags, &op.overlapped, nullptr);
    DWORD error = GetLastError();
    if (res == 0 || error == ERROR_IO_PENDING) {
        op.wait_abortable(&write_closed);
        error = op.error;
    } else {
        op.set_result(0, error);
    }
    if (write_closed.is_pulsed()) {
        return;
    }
    if (error != NO_ERROR) {
        logERR("Could not write to socket %x: %s", sock.get(), winerr_string(error).c_str());
        on_shutdown_write();
    } else if (op.nb_bytes == 0) {
        on_shutdown_write();
    } else {
        if (write_perfmon) write_perfmon->record(op.nb_bytes);
        rassert(op.nb_bytes == size);  // TODO WINDOWS: does windows guarantee this?
    }
}


void tcp_conn_t::shutdown_write() {
    assert_thread();

    int res = ::shutdown(fd_to_socket(sock.get()), SD_SEND);
    if (res != 0 && GetLastError() != WSAENOTCONN) {
        logERR("Could not shutdown socket for writing: %s", winerr_string(GetLastError()).c_str());
    }

    on_shutdown_write();
}

void tcp_conn_t::on_shutdown_write() {
    assert_thread();
    rassert(!write_closed.is_pulsed());
    write_closed.pulse();
}

bool tcp_conn_t::is_write_open() const {
    assert_thread();
    return !write_closed.is_pulsed();
}


void tcp_conn_t::rethread(threadnum_t new_thread) {
    if (home_thread() == get_thread_id() && new_thread == INVALID_THREAD) {
        rassert(event_watcher.has());
        event_watcher->rethread(new_thread);

    } else if (home_thread() == INVALID_THREAD && new_thread == get_thread_id()) {
        event_watcher->rethread(new_thread);

    } else {
        crash("tcp_conn_t can be rethread()ed from no thread to the current thread or "
              "from the current thread to no thread, but no other combination is legal. The "
              "current thread is %" PRIi32 "; the old thread is %" PRIi32 "; the new thread "
              "is %" PRIi32 ".\n",
              get_thread_id().threadnum, home_thread().threadnum, new_thread.threadnum);
    }

    real_home_thread = new_thread;

    read_closed.rethread(new_thread);
    write_closed.rethread(new_thread);
}

bool tcp_conn_t::getpeername(ip_and_port_t *ip_and_port) {
    struct sockaddr_storage addr;
    socklen_t addr_len = sizeof(addr);

    int res = ::getpeername(fd_to_socket(sock.get()), reinterpret_cast<sockaddr *>(&addr), &addr_len);
    if (res == 0) {
        *ip_and_port = ip_and_port_t(reinterpret_cast<sockaddr *>(&addr));
        return true;
    }

    return false;
}

void tcp_conn_t::on_event(int /* events */) {
    assert_thread();

    /* This is called by linux_event_watcher_t when error events occur. Ordinary
       poll_event_in/poll_event_out events are not sent through this function. */

    if (is_write_open()) {
        shutdown_write();
    }

    if (is_read_open()) {
        shutdown_read();
    }

    event_watcher->stop_watching_for_errors();
}

linux_event_watcher_t* tcp_conn_t::get_event_watcher() {
    return &*event_watcher;
}

scoped_signal_t tcp_conn_t::rdhup_watcher() {
    return make_scoped_signal<cond_t>();
}

#endif // TCP_IMPLEMENTATION_WINDOWS
