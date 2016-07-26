#include "arch/types.hpp"

#include "utils.hpp"

file_account_t::file_account_t(file_t *par, int pri, int outstanding_requests_limit) :
    parent(par),
    account(parent->create_account(pri, outstanding_requests_limit)) { }

file_account_t::~file_account_t() {
    parent->destroy_account(account);
}

void linux_iocallback_t::on_io_failure(int errsv, int64_t offset, int64_t count) {
    if (errsv == ENOSPC) {
        // fail_due_to_user_error rather than crash because we don't want to
        // print a backtrace in this case.
        fail_due_to_user_error("Ran out of disk space. (offset = %" PRIi64
                               ", count = %" PRIi64 ")", offset, count);
    } else {
        crash("I/O operation failed. (%s) (offset = %" PRIi64 ", count = %" PRIi64 ")",
              errno_string(errsv).c_str(), offset, count);
    }
}
