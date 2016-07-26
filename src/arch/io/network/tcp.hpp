#ifndef ARCH_IO_NETWORK_TCP_HPP_
#define ARCH_IO_NETWORK_TCP_HPP_

#include "arch/io/event_watcher.hpp"
#include "arch/io/io_utils.hpp"
#include "arch/io/network/bufferable.hpp"
#include "arch/runtime/event_queue_types.hpp"
#include "arch/types.hpp"
#include "concurrency/cond_var.hpp"
#include "concurrency/interruptor.hpp"

#ifdef _WIN32
#define TCP_IMPLEMENTATION_WINDOWS
#else
#define TCP_IMPLEMENTATION_NIX
#endif

class tcp_conn_t :
    public bufferable_conn_t,
    public home_thread_mixin_t,
    private event_callback_t {
public:

    scoped_signal_t rdhup_watcher();

    void enable_keepalive();

    // NB. interruptor cannot be nullptr.
    tcp_conn_t(
        const ip_address_t &host,
        int port,
        signal_t *interruptor,
        int local_port = ANY_PORT)
        THROWS_ONLY(connect_failed_exc_t, interrupted_exc_t);

    /* Call shutdown_write() to close the half of the pipe that goes from us to the peer */
    void shutdown_write();

    /* Returns false if the half of the pipe that goes from us to the peer has been closed. */
    bool is_write_open() const;

    void shutdown_read();

    /* Returns false if the half of the pipe that goes from the peer to us has been closed. */
    bool is_read_open() const;

    void rethread(threadnum_t thread);

    explicit tcp_conn_t(scoped_fd_t &&sock);

private:

    friend class secure_tcp_conn_t;

    // The underlying TCP socket file descriptor.
    scoped_fd_t sock;

    /* These are pulsed if and only if the read/write end of the connection has been closed. */
    cond_t read_closed, write_closed;

    void on_shutdown_read();
    void on_shutdown_write();

    /* Note that this only gets called to handle error-events. Read and write
    events are handled through the event_watcher_t. */
    void on_event(int events);

    /* Object that we use to watch for events. It's NULL when we are not registered on any
    thread, and otherwise is an object that's valid for the current thread. */
    scoped_ptr_t<event_watcher_t> event_watcher;

    friend class buffered_conn_t;

    /* Reads up to the given number of bytes, but not necessarily that many. Simple
    wrapper around ::read(). Returns the number of bytes read or throws
    tcp_conn_read_closed_exc_t. Bypasses read_buffer. */
    size_t read_internal(
        void *buffer, size_t size
    ) THROWS_ONLY(tcp_conn_read_closed_exc_t);

    /* Used to actually perform a write. If the write end of the connection is open, then
    writes `size` bytes from `buffer` to the socket. */
    void perform_write(const void *buffer, size_t size);

    bool getpeername(ip_and_port_t *ip_and_port);

    event_watcher_t *get_event_watcher();
};

#endif // ARCH_IO_NETWORK_TCP_HPP_
