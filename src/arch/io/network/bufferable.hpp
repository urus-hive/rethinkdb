#ifndef ARCH_IO_NETWORK_BUFFERABLE_HPP_
#define ARCH_IO_NETWORK_BUFFERABLE_HPP_

#include <cstddef>
#include <exception>
#include <string>

#include "arch/address.hpp"
#include "concurrency/signal.hpp"
#include "arch/io/network/exceptions.hpp"
#include "perfmon/perfmon.hpp"
#include "threading.hpp"

class bufferable_conn_t {
public:
    bufferable_conn_t() : write_perfmon(nullptr) { }

    class connect_failed_exc_t : public std::exception {
    public:
        explicit connect_failed_exc_t(int en) :
            error(en),
            info("Could not make connection: " + errno_string(error)) { }

        const char *what() const throw () {
            return info.c_str();
        }

        ~connect_failed_exc_t() throw () { }

        const int error;
        const std::string info;
    };

    virtual size_t read_internal(void *buffer, size_t size) = 0;

    virtual void perform_write(const void *buffer, size_t size) = 0;

    virtual ~bufferable_conn_t() { }

    virtual void rethread(threadnum_t new_thread) = 0;
    virtual bool getpeername(ip_and_port_t *ip_and_port) = 0;

    virtual bool is_read_open() const = 0;
    virtual bool is_write_open() const = 0;
    virtual void shutdown_read() = 0;
    virtual void shutdown_write() = 0;
    virtual void enable_keepalive() = 0;
    virtual scoped_signal_t rdhup_watcher() = 0;

protected:
    /* Put a `perfmon_rate_monitor_t` here if you want to record stats on how fast data is being
    transmitted over the network. */
    perfmon_rate_monitor_t *write_perfmon;
};

#endif // ARCH_IO_NETWORK_BUFFERABLE_HPP_
