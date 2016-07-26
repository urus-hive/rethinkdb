#include "arch/io/network/listen.hpp"

#include <fcntl.h>
#include <sys/types.h>

#ifdef _WIN32
#include "windows.hpp"
#include <ws2tcpip.h> // NOLINT
#include <iphlpapi.h> // NOLINT
#else
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#endif

#include "concurrency/exponential_backoff.hpp"
#include "concurrency/wait_any.hpp"
#include "logger.hpp"

// ATN dupe from tcp.cc
#ifdef TRACE_WINSOCK
#define winsock_debugf(...) debugf("winsock: " __VA_ARGS__)
#else
#define winsock_debugf(...) ((void)0)
#endif

/* Network listener object */
linux_nonthrowing_tcp_listener_t::linux_nonthrowing_tcp_listener_t(
         const std::set<ip_address_t> &bind_addresses, int _port,
         const std::function<void(scoped_ptr_t<linux_tcp_conn_descriptor_t> &)> &cb) :
    callback(cb),
    local_addresses(bind_addresses),
    port(_port),
    bound(false),
    socks(),
    last_used_socket_index(0),
    event_watchers(),
    log_next_error(true)
{
    // If no addresses were supplied, default to 'any'
    if (local_addresses.empty()) {
        local_addresses.insert(ip_address_t::any(AF_INET6));
#ifdef _WIN32
        local_addresses.insert(ip_address_t::any(AF_INET));
#endif
    }
}

bool linux_nonthrowing_tcp_listener_t::begin_listening() {
    if (!bound) {
        try {
            bind_sockets();
        } catch (const tcp_socket_exc_t &) {
            return false;
        }
    }

    const int RDB_LISTEN_BACKLOG = 256;

    // Start listening to connections
    for (size_t i = 0; i < socks.size(); ++i) {
        winsock_debugf("listening on socket %x\n", socks[i].get());
        int res = listen(fd_to_socket(socks[i].get()), RDB_LISTEN_BACKLOG);
#ifdef _WIN32
        guarantee_winerr(res == 0, "Couldn't listen to the socket");
#else
        guarantee_err(res == 0, "Couldn't listen to the socket");
        res = fcntl(socks[i].get(), F_SETFL, O_NONBLOCK);
        guarantee_err(res == 0, "Could not make socket non-blocking");
#endif
    }

    // Start the accept loop
    accept_loop_drainer.init(new auto_drainer_t);
    coro_t::spawn_sometime(std::bind(
        &linux_nonthrowing_tcp_listener_t::accept_loop, this, auto_drainer_t::lock_t(accept_loop_drainer.get())));

    return true;
}

bool linux_nonthrowing_tcp_listener_t::is_bound() const {
    return bound;
}

int linux_nonthrowing_tcp_listener_t::get_port() const {
    return port;
}

int linux_nonthrowing_tcp_listener_t::init_sockets() {
    event_watchers.reset();
    event_watchers.init(local_addresses.size());
    socks.reset();
    socks.init(local_addresses.size());

    size_t i = 0;
    for (auto addr = local_addresses.begin(); addr != local_addresses.end(); ++addr, ++i) {
        if (event_watchers[i].has()) {
            event_watchers[i].reset();
        }

#ifdef _WIN32
        // TODO WINDOWS: maybe use WSASocket instead
        socks[i].reset(socket_to_fd(socket(addr->get_address_family(), SOCK_STREAM, IPPROTO_TCP)));
        winsock_debugf("new socket for listening: %x\n", socks[i]);
        if (socks[i].get() == INVALID_FD) {
            logERR("socket failed: %s", winerr_string(GetLastError()));
            return EIO;
        }
#else
        socks[i].reset(socket(addr->get_address_family(), SOCK_STREAM, 0));
        if (socks[i].get() == INVALID_FD) {
            return get_errno();
        }
#endif

        event_watchers[i].init(new event_watcher_t(socks[i].get(), this));

        fd_t sock_fd = socks[i].get();
        guarantee_err(sock_fd != INVALID_FD, "Couldn't create socket");

        int sockoptval = 1;
#ifdef _WIN32
        int res = setsockopt(fd_to_socket(sock_fd), SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char*>(&sockoptval), sizeof(sockoptval)); 
        guarantee_winerr(res != -1, "Could not set REUSEADDR option");
#else
        int res = setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &sockoptval, sizeof(sockoptval)); 
        guarantee_err(res != -1, "Could not set REUSEADDR option");
#endif
        /* XXX Making our socket NODELAY prevents the problem where responses to
         * pipelined requests are delayed, since the TCP Nagle algorithm will
         * notice when we send multiple small packets and try to coalesce them. But
         * if we are only sending a few of these small packets quickly, like during
         * pipeline request responses, then Nagle delays for around 40 ms before
         * sending out those coalesced packets if they don't reach the max window
         * size. So for latency's sake we want to disable Nagle.
         *
         * This might decrease our throughput, so perhaps we should add a
         * runtime option for it.
         */
#ifdef _WIN32
        res = setsockopt(fd_to_socket(sock_fd), IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&sockoptval), sizeof(sockoptval));
        guarantee_winerr(res != -1, "Could not set TCP_NODELAY option");
#else
        res = setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &sockoptval, sizeof(sockoptval));
        guarantee_err(res != -1, "Could not set TCP_NODELAY option");
#endif
    }
    return 0;
}

int bind_ipv4_interface(fd_t sock, int *port_out, const struct in_addr &addr) {
    sockaddr_in serv_addr;
    socklen_t sa_len(sizeof(serv_addr));
    memset(&serv_addr, 0, sa_len);
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(*port_out);
    serv_addr.sin_addr = addr;

    int res = bind(fd_to_socket(sock), reinterpret_cast<sockaddr *>(&serv_addr), sa_len);
    winsock_debugf("bound socket %x\n", sock);
    if (res != 0) {
#ifdef _WIN32
        logWRN("bind failed: %s", winerr_string(GetLastError()).c_str());
        res = EIO;
#else
        res = get_errno();
#endif
    } else if (*port_out == ANY_PORT) {
        res = ::getsockname(fd_to_socket(sock), reinterpret_cast<sockaddr *>(&serv_addr), &sa_len);
        guarantee_err(res != -1, "Could not determine socket local port number");
        *port_out = ntohs(serv_addr.sin_port);
    }

    return res;
}

int bind_ipv6_interface(fd_t sock, int *port_out, const ip_address_t &addr) {
    sockaddr_in6 serv_addr;
    socklen_t sa_len(sizeof(serv_addr));
    memset(&serv_addr, 0, sa_len);
    serv_addr.sin6_family = AF_INET6;
    serv_addr.sin6_port = htons(*port_out);
    serv_addr.sin6_addr = addr.get_ipv6_addr();
    serv_addr.sin6_scope_id = addr.get_ipv6_scope_id();

    int res = bind(fd_to_socket(sock), reinterpret_cast<sockaddr *>(&serv_addr), sa_len);
    if (res != 0) {
#ifdef _WIN32
        logWRN("bind failed: %s", winerr_string(GetLastError()).c_str());
        res = EIO;
#else
        res = get_errno();
#endif
    } else if (*port_out == ANY_PORT) {
        res = ::getsockname(fd_to_socket(sock), reinterpret_cast<sockaddr *>(&serv_addr), &sa_len);
        guarantee_err(res != -1, "Could not determine socket local port number");
        *port_out = ntohs(serv_addr.sin6_port);
    }

    return res;
}

void fallback_to_ipv4(std::set<ip_address_t> *addrs, int err, int port) {
    bool ipv6_address_found = false;

    // Fallback to IPv4 only - remove any IPv6 addresses and resize dependant arrays
    for (auto it = addrs->begin(); it != addrs->end();) {
        if (it->get_address_family() == AF_INET6) {
            if (it->is_any()) {
                addrs->insert(ip_address_t::any(AF_INET));
            }
            addrs->erase(*(it++));
            ipv6_address_found = true;
        } else {
            ++it;
        }
    }

    if (!ipv6_address_found) {
        throw tcp_socket_exc_t(err, port);
    }
    logERR("Failed to create sockets for listener on port %d, "
           "falling back to IPv4 only", port);
}

void linux_nonthrowing_tcp_listener_t::bind_sockets() {
    // It may take multiple attempts to get all the sockets onto the same port
    int local_port = port;

    for (uint32_t attempts = (port == ANY_PORT) ? MAX_BIND_ATTEMPTS : 1;
         attempts > 0; --attempts) {
        local_port = port;
        int res = init_sockets();
        if (res == EAFNOSUPPORT) {
            ++attempts; // This attempt doesn't count
            fallback_to_ipv4(&local_addresses, res, port);
            continue;
        } else if (res != 0) {
            throw tcp_socket_exc_t(res, port);
        }

        size_t i = 0;
        for (std::set<ip_address_t>::iterator addr = local_addresses.begin();
             addr != local_addresses.end(); ++i, ++addr) {
            winsock_debugf("binding to %s\n", addr->to_string().c_str());
            switch (addr->get_address_family()) {
            case AF_INET:
                res = bind_ipv4_interface(socks[i].get(), &local_port, addr->get_ipv4_addr());
                break;
            case AF_INET6:
                res = bind_ipv6_interface(socks[i].get(), &local_port, *addr);
                break;
            default:
                unreachable();
            }

            if (res == EADDRNOTAVAIL) {
                ++attempts; // This attempt doesn't count
                fallback_to_ipv4(&local_addresses, res, port);
                break;
            } else if (res == EADDRINUSE || res == EACCES) {
                break;
            } else if (res != 0) {
                throw tcp_socket_exc_t(res, port);
            }
        }

        if (i == local_addresses.size()) {
            bound = true;
            port = local_port;
            return;
        }
    }
    throw tcp_socket_exc_t(EADDRINUSE, port);
}

#ifdef _WIN32
void linux_nonthrowing_tcp_listener_t::accept_loop_single(
       const auto_drainer_t::lock_t &lock,
       exponential_backoff_t backoff,
       windows_event_watcher_t *event_watcher) {

    static const int ADDRESS_SIZE = INET6_ADDRSTRLEN + 16;

    fd_t listening_sock = event_watcher->handle;

    WSAPROTOCOL_INFO pinfo;
    int pinfo_size = sizeof(pinfo);
    DWORD res = getsockopt(fd_to_socket(listening_sock), SOL_SOCKET, SO_PROTOCOL_INFO, reinterpret_cast<char*>(&pinfo), &pinfo_size);
    guarantee_winerr(res == 0, "getsockopt failed");
    int address_family = pinfo.iAddressFamily;

    while (!lock.get_drain_signal()->is_pulsed()) {
        overlapped_operation_t op(event_watcher);
        fd_t new_sock = socket_to_fd(WSASocket(address_family, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED));
        winsock_debugf("new socket for accepting: %x\n", new_sock);
        guarantee_winerr(new_sock != INVALID_FD, "WSASocket failed");
        winsock_debugf("accepting on socket %x\n", listening_sock);
        DWORD bytes_received;
        char addresses[ADDRESS_SIZE][2];
        BOOL res = get_AcceptEx(listening_sock)(fd_to_socket(listening_sock), fd_to_socket(new_sock), addresses, 0, ADDRESS_SIZE, ADDRESS_SIZE, &bytes_received, &op.overlapped);

        if (res) {
            op.set_result(0, NO_ERROR);
        } else {
            DWORD error = GetLastError();
            if (error != ERROR_IO_PENDING) {
                op.set_result(0, error);
            } else {
                op.wait_abortable(lock.get_drain_signal());
                if (lock.get_drain_signal()->is_pulsed()) {
                    return;
                }
            }
        }
        if (op.error != NO_ERROR) {
            logERR("AcceptEx failed: %s", winerr_string(op.error).c_str());
            try {
                backoff.failure(lock.get_drain_signal());
            } catch (const interrupted_exc_t &) {
                return;
            }
        } else {
            winsock_debugf("accepted %x from %x\n", new_sock, listening_sock);
            coro_t::spawn_now_dangerously(std::bind(&linux_nonthrowing_tcp_listener_t::handle, this, new_sock));
            backoff.success();
        }
    }
}
#else
fd_t linux_nonthrowing_tcp_listener_t::wait_for_any_socket(const auto_drainer_t::lock_t &lock) {
    scoped_array_t<scoped_ptr_t<linux_event_watcher_t::watch_t> > watches(event_watchers.size());
    wait_any_t waiter(lock.get_drain_signal());

    for (size_t i = 0; i < event_watchers.size(); ++i) {
        watches[i].init(new linux_event_watcher_t::watch_t(event_watchers[i].get(), poll_event_in));
        waiter.add(watches[i].get());
    }

    waiter.wait_lazily_unordered();

    if (lock.get_drain_signal()->is_pulsed()) {
        return -1;
    }

    for (size_t i = 0; i < watches.size(); ++i) {
        // This rather convoluted expression is to make sure we don't starve out higher-indexed interfaces
        //  because connections are coming in too fast on the lower interfaces, unlikely but valid
        size_t index = (last_used_socket_index + i + 1) % watches.size();
        if (watches[index]->is_pulsed()) {
            last_used_socket_index = index;
            return socks[index].get();
        }
    }

    // This should never happen, but it shouldn't be much of a problem
    return -1;
}
#endif

void linux_nonthrowing_tcp_listener_t::accept_loop(auto_drainer_t::lock_t lock) {
    exponential_backoff_t backoff(10, 160, 2.0, 0.5);

#ifdef _WIN32

    pmap(event_watchers.size(), [this, &lock, &backoff](int i){
        accept_loop_single(lock, backoff, event_watchers[i].get());
    });

#else
    fd_t active_fd = socks[0].get();
    while(!lock.get_drain_signal()->is_pulsed()) {
        fd_t new_sock = accept(active_fd, nullptr, nullptr);

        if (new_sock != INVALID_FD) {
            coro_t::spawn_now_dangerously(std::bind(&linux_nonthrowing_tcp_listener_t::handle, this, new_sock));
            backoff.success();

            /* Assume that if there was a problem before, it's gone now because accept()
               is working. */
            log_next_error = true;

        } else if (get_errno() == EAGAIN || get_errno() == EWOULDBLOCK) {
            active_fd = wait_for_any_socket(lock);

        } else if (get_errno() == EINTR) {
            /* Harmless error; just try again. */

        } else {
            /* Unexpected error. Log it unless it's a repeat error. */
            if (log_next_error) {
                logERR("accept() failed: %s.",
                       errno_string(get_errno()).c_str());
                log_next_error = false;
            }

            /* Delay before retrying. */
            try {
                backoff.failure(lock.get_drain_signal());
            } catch (const interrupted_exc_t &) {
                return;
            }
        }
    }
#endif
}

void linux_nonthrowing_tcp_listener_t::handle(fd_t socket) {
    scoped_ptr_t<linux_tcp_conn_descriptor_t> nconn(new linux_tcp_conn_descriptor_t(socket));
    callback(nconn);
}

linux_nonthrowing_tcp_listener_t::~linux_nonthrowing_tcp_listener_t() {
    /* Interrupt the accept loop */
    accept_loop_drainer.reset();

    // scoped_fd_t destructor will close() the socket
}

void linux_nonthrowing_tcp_listener_t::on_event(int) {
    /* This is only called in cases of error; normal input events are received
       via event_listener.watch(). */
}

void noop_fun(UNUSED const scoped_ptr_t<linux_tcp_conn_descriptor_t> &arg) { }

linux_tcp_bound_socket_t::linux_tcp_bound_socket_t(const std::set<ip_address_t> &bind_addresses, int port) :
    listener(new linux_nonthrowing_tcp_listener_t(bind_addresses, port, noop_fun))
{
    listener->bind_sockets();
}

int linux_tcp_bound_socket_t::get_port() const {
    return listener->get_port();
}

linux_tcp_listener_t::linux_tcp_listener_t(const std::set<ip_address_t> &bind_addresses, int port,
    const std::function<void(scoped_ptr_t<linux_tcp_conn_descriptor_t> &)> &callback) :
        listener(new linux_nonthrowing_tcp_listener_t(bind_addresses, port, callback))
{
    if (!listener->begin_listening()) {
        throw address_in_use_exc_t("localhost", listener->get_port());
    }
}

linux_tcp_listener_t::linux_tcp_listener_t(
    linux_tcp_bound_socket_t *bound_socket,
    const std::function<void(scoped_ptr_t<linux_tcp_conn_descriptor_t> &)> &callback) :
        listener(bound_socket->listener.release())
{
    listener->callback = callback;
    if (!listener->begin_listening()) {
        throw address_in_use_exc_t("localhost", listener->get_port());
    }
}

int linux_tcp_listener_t::get_port() const {
    return listener->get_port();
}

linux_repeated_nonthrowing_tcp_listener_t::linux_repeated_nonthrowing_tcp_listener_t(
    const std::set<ip_address_t> &bind_addresses,
    int port,
    const std::function<void(scoped_ptr_t<linux_tcp_conn_descriptor_t> &)> &callback) :
        listener(bind_addresses, port, callback)
{ }

int linux_repeated_nonthrowing_tcp_listener_t::get_port() const {
    return listener.get_port();
}

void linux_repeated_nonthrowing_tcp_listener_t::begin_repeated_listening_attempts() {
    auto_drainer_t::lock_t lock(&drainer);
    coro_t::spawn_sometime(
                           std::bind(&linux_repeated_nonthrowing_tcp_listener_t::retry_loop, this, lock));
}

void linux_repeated_nonthrowing_tcp_listener_t::retry_loop(auto_drainer_t::lock_t lock) {
    try {
        bool bound = listener.begin_listening();

        for (int retry_interval = 1;
             !bound;
             retry_interval = std::min(10, retry_interval + 2)) {
            logNTC("Will retry binding to port %d in %d seconds.\n",
                   listener.get_port(),
                   retry_interval);
            nap(retry_interval * 1000, lock.get_drain_signal());
            bound = listener.begin_listening();
        }

        bound_cond.pulse();
    } catch (const interrupted_exc_t &e) {
        // ignore
    }
}

signal_t *linux_repeated_nonthrowing_tcp_listener_t::get_bound_signal() {
    return &bound_cond;
}

std::vector<std::string> get_ips() {
    std::vector<std::string> ret;

#ifdef _WIN32
    const ULONG SIZE_INCREMENT = 15000;
    scoped_malloc_t<IP_ADAPTER_ADDRESSES> addresses_buffer;
    UINT res;
    for (int i = 1; i < 10; i++) {
        ULONG buf_size = i * SIZE_INCREMENT;
        addresses_buffer = scoped_malloc_t<IP_ADAPTER_ADDRESSES>(buf_size);
        res = GetAdaptersAddresses(AF_UNSPEC, GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST | GAA_FLAG_SKIP_DNS_SERVER, nullptr, addresses_buffer.get(), &buf_size);
        if (res != ERROR_BUFFER_OVERFLOW) {
            break;
        }
    }
    guarantee_winerr(res == NO_ERROR, "GetAdaptersAddresses failed");
    for (IP_ADAPTER_ADDRESSES *addresses = addresses_buffer.get(); addresses != nullptr; addresses = addresses->Next) {
        for (IP_ADAPTER_UNICAST_ADDRESS *unicast = addresses->FirstUnicastAddress; unicast != nullptr; unicast = unicast->Next) {
            SOCKADDR *addr = unicast->Address.lpSockaddr;
            char buf[INET6_ADDRSTRLEN];
            const char *str;
            switch (addr->sa_family) {
            case AF_INET: {
                auto sa = reinterpret_cast<sockaddr_in*>(addr);
                str = InetNtop(AF_INET, &sa->sin_addr, buf, sizeof(buf));
                break;
            }
            case AF_INET6: {
                auto sa6 = reinterpret_cast<sockaddr_in6*>(addr);
                str = InetNtop(AF_INET6, &sa6->sin6_addr, buf, sizeof(buf));
                break;
            }
            default:
                continue;
            }
            guarantee_winerr(str != nullptr, "InetNtop failed");
            ret.emplace_back(str);
        }
    }
#else
    struct ifaddrs *if_addrs = NULL;
    int addr_res = getifaddrs(&if_addrs);
    guarantee_err(addr_res == 0, "getifaddrs failed, could not determine local ip addresses");

    for (ifaddrs *p = if_addrs; p != nullptr; p = p->ifa_next) {
        if (p->ifa_addr == nullptr) {
            continue;
        } else if (p->ifa_addr->sa_family == AF_INET) {
            if (!(p->ifa_flags & IFF_LOOPBACK)) {
                struct sockaddr_in *in_addr = reinterpret_cast<sockaddr_in *>(p->ifa_addr);
                // I don't think the "+ 1" is necessary, we're being
                // paranoid about weak documentation.
                const int buflength = INET_ADDRSTRLEN;
                char buf[buflength + 1] = { 0 };
                const char *res = inet_ntop(AF_INET, &in_addr->sin_addr, buf, buflength);

                guarantee_err(res != nullptr, "inet_ntop failed");

                ret.push_back(std::string(buf));
            }
        } else if (p->ifa_addr->sa_family == AF_INET6) {
            if (!(p->ifa_flags & IFF_LOOPBACK)) {
                struct sockaddr_in6 *in6_addr = reinterpret_cast<sockaddr_in6 *>(p->ifa_addr);

                const int buflength = INET6_ADDRSTRLEN;
                scoped_array_t<char> buf(buflength + 1);
                memset(buf.data(), 0, buf.size());
                const char *res = inet_ntop(AF_INET6, &in6_addr->sin6_addr, buf.data(), buflength);

                guarantee_err(res != nullptr, "inet_ntop failed on an ipv6 address");

                ret.push_back(std::string(buf.data()));
            }
        }
    }

    freeifaddrs(if_addrs);
#endif

    return ret;
}
