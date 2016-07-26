#include "arch/io/network/tcp.hpp"

#ifdef TCP_IMPLEMENTATION_NIX

#include <arpa/inet.h>
#include <fcntl.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "concurrency/wait_any.hpp"
#include "logger.hpp"

void async_connect(fd_t socket, sockaddr *sa, size_t sa_len,
                   event_watcher_t *event_watcher, signal_t *interuptor) {
    int res;
    do {
        res = connect(socket, sa, sa_len);
    } while (res == -1 && get_errno() == EINTR);

    if (res != 0) {
        if (get_errno() == EINPROGRESS) {
            linux_event_watcher_t::watch_t watch(event_watcher, poll_event_out);
            wait_interruptible(&watch, interuptor);
            int error;
            socklen_t error_size = sizeof(error);
            int getsockoptres = getsockopt(socket, SOL_SOCKET, SO_ERROR, &error, &error_size);
            if (getsockoptres != 0) {
                throw bufferable_conn_t::connect_failed_exc_t(error);
            }
            if (error != 0) {
                throw bufferable_conn_t::connect_failed_exc_t(error);
            }
        } else {
            throw bufferable_conn_t::connect_failed_exc_t(get_errno());
        }
    }
}

void connect_ipv4_internal(fd_t socket, int local_port, const in_addr &addr, int port, event_watcher_t *event_watcher, signal_t *interuptor) {
    struct sockaddr_in sa;
    socklen_t sa_len(sizeof(sa));
    memset(&sa, 0, sa_len);
    sa.sin_family = AF_INET;

    if (local_port != 0) {
        sa.sin_port = htons(local_port);
        sa.sin_addr.s_addr = INADDR_ANY;
        if (bind(socket, reinterpret_cast<sockaddr *>(&sa), sa_len) != 0) {
            logWRN("Failed to bind to local port %d: %s", local_port, errno_string(get_errno()).c_str());
        }
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

    if (local_port != 0) {
        sa.sin6_port = htons(local_port);
        sa.sin6_addr = in6addr_any;
        if (bind(socket, reinterpret_cast<sockaddr *>(&sa), sa_len) != 0) {
            logWRN("Failed to bind to local port %d: %s", local_port, errno_string(get_errno()).c_str());
        }
    }

    sa.sin6_port = htons(port);
    sa.sin6_addr = addr;
    sa.sin6_scope_id = scope_id;

    async_connect(socket, reinterpret_cast<sockaddr *>(&sa), sa_len, event_watcher, interuptor);
}

fd_t create_socket_wrapper(int address_family) {
    fd_t res = socket(address_family, SOCK_STREAM, 0);
    if (res == INVALID_FD) {
        // Let the user know something is wrong - except in the case where
        // TCP doesn't support AF_INET6, which may be fairly common and spammy
        if (get_errno() != EAFNOSUPPORT || address_family == AF_INET) {
            logERR("Failed to create socket: %s", errno_string(get_errno()).c_str());
        }
        throw bufferable_conn_t::connect_failed_exc_t(get_errno());
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

    guarantee_err(fcntl(sock.get(), F_SETFL, O_NONBLOCK) == 0, "Could not make socket non-blocking");

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

tcp_conn_t::tcp_conn_t(scoped_fd_t &&s) :
       sock(std::move(s)),
       event_watcher(new event_watcher_t(sock.get(), this)) {
    rassert(sock.get() != INVALID_FD);

    int res = fcntl(sock.get(), F_SETFL, O_NONBLOCK);
    guarantee_err(res == 0, "Could not make socket non-blocking");
}

void tcp_conn_t::enable_keepalive() {
    int optval = 1;
    int res = setsockopt(sock.get(), SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval));
    guarantee(res != -1, "Could not set SO_KEEPALIVE option.");
}


size_t tcp_conn_t::read_internal(void *buffer, size_t size) THROWS_ONLY(tcp_conn_read_closed_exc_t) {
    assert_thread();
    rassert(!read_closed.is_pulsed());

    while (true) {
        ssize_t res = ::read(sock.get(), buffer, size);

        if (res == -1 && (get_errno() == EAGAIN || get_errno() == EWOULDBLOCK)) {
            /* There's no data available right now, so we must wait for a notification from the
               epoll queue, or for an order to shut down. */

            linux_event_watcher_t::watch_t watch(event_watcher.get(), poll_event_in);
            wait_any_t waiter(&watch, &read_closed);
            waiter.wait_lazily_unordered();

            if (read_closed.is_pulsed()) {
                /* We were closed for whatever reason. Something else has already called
                   on_shutdown_read(). In fact, we were probably signalled by on_shutdown_read(). */
                throw tcp_conn_read_closed_exc_t();
            }

            /* Go around the loop and try to read again */

        } else if (res == 0 || (res == -1 && (get_errno() == ECONNRESET || get_errno() == ENOTCONN))) {
            /* We were closed. This is the first notification that the kernel has given us, so we
               must call on_shutdown_read(). */
            on_shutdown_read();
            throw tcp_conn_read_closed_exc_t();

        } else if (res == -1) {
            /* Unknown error. This is not expected, but it will probably happen sometime so we
               shouldn't crash. */
            logERR("Could not read from socket: %s", errno_string(get_errno()).c_str());
            on_shutdown_read();
            throw tcp_conn_read_closed_exc_t();

        } else {
            /* We read some data, whooo */
            return res;
        }
    }
}


void tcp_conn_t::shutdown_read() {
    assert_thread();
    int res = ::shutdown(sock.get(), SHUT_RD);
    if (res != 0 && get_errno() != ENOTCONN) {
        logERR("Could not shutdown socket for reading: %s", errno_string(get_errno()).c_str());
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

    while (size > 0) {
        ssize_t res = ::write(sock.get(), buf, size);

        if (res == -1 && (get_errno() == EAGAIN || get_errno() == EWOULDBLOCK)) {
            /* Wait for a notification from the event queue, or for an order to
               shut down */
            linux_event_watcher_t::watch_t watch(event_watcher.get(), poll_event_out);
            wait_any_t waiter(&watch, &write_closed);
            waiter.wait_lazily_unordered();

            if (write_closed.is_pulsed()) {
                /* We were closed for whatever reason. Whatever signalled us has already called
                   on_shutdown_write(). */
                break;
            }

            /* Go around the loop and try to write again */

        } else if (res == -1 && (get_errno() == EPIPE || get_errno() == ENOTCONN || get_errno() == EHOSTUNREACH ||
                                 get_errno() == ENETDOWN || get_errno() == EHOSTDOWN || get_errno() == ECONNRESET)) {
            /* These errors are expected to happen at some point in practice */
            on_shutdown_write();
            break;

        } else if (res == -1) {
            /* In theory this should never happen, but it probably will. So we write a log message
               and then shut down normally. */
            logERR("Could not write to socket: %s", errno_string(get_errno()).c_str());
            on_shutdown_write();
            break;

        } else if (res == 0) {
            /* This should never happen either, but it's better to write an error message than to
               crash completely. */
            logERR("Didn't expect write() to return 0.");
            on_shutdown_write();
            break;

        } else {
            rassert(res <= static_cast<ssize_t>(size));
            buf = reinterpret_cast<const void *>(reinterpret_cast<const char *>(buf) + res);
            size -= res;
            if (write_perfmon) {
                write_perfmon->record(res);
            }
        }
    }
}


void tcp_conn_t::shutdown_write() {
    assert_thread();

    int res = ::shutdown(sock.get(), SHUT_WR);
    if (res != 0 && get_errno() != ENOTCONN) {
        logERR("Could not shutdown socket for writing: %s", errno_string(get_errno()).c_str());
    }

    on_shutdown_write();
}

void tcp_conn_t::on_shutdown_write() {
    assert_thread();
    rassert(!write_closed.is_pulsed());
    write_closed.pulse();

    /* We don't flush out the write queue or stop the write coro pool explicitly.
       But by pulsing `write_closed`, we turn all `perform_write()` operations into
       no-ops, so in practice the write queue empties. */
}

bool tcp_conn_t::is_write_open() const {
    assert_thread();
    return !write_closed.is_pulsed();
}


void tcp_conn_t::rethread(threadnum_t new_thread) {
    if (home_thread() == get_thread_id() && new_thread == INVALID_THREAD) {
        rassert(event_watcher.has());
        event_watcher.reset();

    } else if (home_thread() == INVALID_THREAD && new_thread == get_thread_id()) {
        rassert(!event_watcher.has());
        event_watcher.init(new event_watcher_t(sock.get(), this));

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
    return make_scoped_signal<linux_event_watcher_t::watch_t>(&*event_watcher, poll_event_rdhup);
}

#endif // TCP_IMPLEMENTATION_NIX
