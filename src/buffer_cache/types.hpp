// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef BUFFER_CACHE_TYPES_HPP_
#define BUFFER_CACHE_TYPES_HPP_

#include <limits.h>
#include <stdint.h>

#include "containers/archive/archive.hpp"
#include "serializer/types.hpp"

// write_durability_t::INVALID is an invalid value, notably it can't be serialized.
enum class write_durability_t { INVALID, SOFT, HARD };
ARCHIVE_PRIM_MAKE_RANGED_SERIALIZABLE(write_durability_t, int8_t,
                                      write_durability_t::SOFT,
                                      write_durability_t::HARD);

#define DEFAULT_FLUSH_INTERVAL 1000
// Converting this value from millis to nanos is less than half of 2^63.
#define NEVER_FLUSH_INTERVAL (0x100000000ll * 1000ll)

struct flush_interval_t {
    int64_t millis;
};

typedef uint32_t block_magic_comparison_t;

struct block_magic_t {
    char bytes[sizeof(block_magic_comparison_t)];

    bool operator==(const block_magic_t &other) const {
        union {
            block_magic_t x;
            block_magic_comparison_t n;
        } u, v;

        u.x = *this;
        v.x = other;

        return u.n == v.n;
    }

    bool operator!=(const block_magic_t &other) const {
        return !(operator==(other));
    }
};

void debug_print(printf_buffer_t *buf, block_magic_t magic);

struct which_cpu_shard_t {
    int which_shard;
    int num_shards;
};

#endif /* BUFFER_CACHE_TYPES_HPP_ */
