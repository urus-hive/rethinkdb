#ifndef ARCH_IO_NETWORK_LISTEN_HPP_
#define ARCH_IO_NETWORK_LISTEN_HPP_

#include <set>

#include "arch/address.hpp"
#include "arch/io/event_watcher.hpp"
#include "arch/io/io_utils.hpp"
#include "arch/io/network/buffered.hpp"
#include "arch/runtime/event_queue_types.hpp"
#include "concurrency/auto_drainer.hpp"

/* The nonthrowing_tcp_listener_t is used to listen on a network port for incoming
connections. Create a nonthrowing_tcp_listener_t with some port and then call set_callback();
the provided callback will be called in a new coroutine every time something connects. */

class nonthrowing_tcp_listener_t : private linux_event_callback_t {
public:
    nonthrowing_tcp_listener_t(const std::set<ip_address_t> &bind_addresses, int _port,
        const std::function<void(scoped_fd_t &&)> &callback);

    ~nonthrowing_tcp_listener_t();

    MUST_USE bool begin_listening();
    bool is_bound() const;
    int get_port() const;

protected:
    friend class tcp_listener_t;
    friend class tcp_bound_socket_t;

    void bind_sockets();

    // The callback to call when we get a connection
    std::function<void(scoped_fd_t &&)> callback;

private:
    static const uint32_t MAX_BIND_ATTEMPTS = 20;
    int init_sockets();

    /* accept_loop() runs in a separate coroutine. It repeatedly tries to accept
    new connections; when accept() blocks, then it waits for events from the
    event loop. */
    void accept_loop(auto_drainer_t::lock_t lock);

#ifdef _WIN32
    void accept_loop_single(const auto_drainer_t::lock_t &lock,
                            exponential_backoff_t backoff,
                            windows_event_watcher_t *event_watcher);
#else
    fd_t wait_for_any_socket(const auto_drainer_t::lock_t &lock);
#endif
    scoped_ptr_t<auto_drainer_t> accept_loop_drainer;

    void handle(fd_t sock);

    /* event_watcher sends any error conditions to here */
    void on_event(int events);

    // The selected local addresses to listen on, 'any' if empty
    std::set<ip_address_t> local_addresses;

    // The port we're asked to bind to
    int port;

    // Inidicates successful binding to a port
    bool bound;

    // The sockets to listen for connections on
    scoped_array_t<scoped_fd_t> socks;

    // The last socket to get a connection, used for round-robining
    size_t last_used_socket_index;

    // Sentries representing our registrations with the event loop, one per socket
    scoped_array_t<scoped_ptr_t<event_watcher_t> > event_watchers;

    bool log_next_error;
};

/* Used by the old style tcp listener */
class tcp_bound_socket_t {
public:
    tcp_bound_socket_t(const std::set<ip_address_t> &bind_addresses, int _port);
    int get_port() const;
private:
    friend class tcp_listener_t;

    scoped_ptr_t<nonthrowing_tcp_listener_t> listener;
};

/* Replicates old constructor-exception-throwing style for backwards compaitbility */
class tcp_listener_t {
public:
    tcp_listener_t(tcp_bound_socket_t *bound_socket,
        const std::function<void(scoped_fd_t &&)> &callback);
    tcp_listener_t(const std::set<ip_address_t> &bind_addresses, int port,
        const std::function<void(scoped_fd_t &&)> &callback);

    int get_port() const;

private:
    scoped_ptr_t<nonthrowing_tcp_listener_t> listener;
};

/* Like a linux tcp listener but repeatedly tries to bind to its port until successful */
class repeated_nonthrowing_tcp_listener_t {
public:
    repeated_nonthrowing_tcp_listener_t(const std::set<ip_address_t> &bind_addresses, int port,
        const std::function<void(scoped_fd_t &&)> &callback);
    void begin_repeated_listening_attempts();

    signal_t *get_bound_signal();
    int get_port() const;

private:
    void retry_loop(auto_drainer_t::lock_t lock);

    nonthrowing_tcp_listener_t listener;
    cond_t bound_cond;
    auto_drainer_t drainer;
};

std::vector<std::string> get_ips();

#endif // ARCH_IO_NETWORK_LISTEN_HPP_
