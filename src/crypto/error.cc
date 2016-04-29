// Copyright 2010-2016 RethinkDB, all rights reserved.
#include "crypto/error.hpp"

#include "utils.hpp"

namespace crypto {

openssl_error_category_t::openssl_error_category_t() {

}

/* virtual */ char const *openssl_error_category_t::name() const noexcept {
    return "OpenSSL";
}

/* virtual */ std::string openssl_error_category_t::message(int condition) const {
    switch (condition) {
        case 336027804:
            return "Received a plain HTTP request on the HTTPS port.";
        case 336027900:
            return "Unknown protocol. By default, RethinkDB requires at least TLSv1.2. "
                   "You can enable older TLS versions on the server through the "
                   "`--tls-min-protocol TLSv1` option. (OpenSSL error 336027900)";
        case 336109761:
            return "No shared cipher. You can change the list of enabled ciphers on the "
                   "server through the `--tls-ciphers` option. Older OpenSSL clients "
                   "(e.g. on OS X) might require `--tls-ciphers "
                   "EECDH+AESGCM:EDH+AESGCM:AES256+EECDH:AES256+EDH:AES256-SHA`. "
                   "(OpenSSL error 336109761)";
        default:
            char const *message = ERR_reason_error_string(condition);
            return strprintf("%s (OpenSSL error %d)",
                             message == nullptr ? "unknown error" : message,
                             condition);
    }
}

}  // namespace crypto
