// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/generic/raft_core.hpp"

#include "concurrency/new_semaphore.hpp"

RDB_IMPL_SERIALIZABLE_1_SINCE_v2_1(raft_member_id_t, uuid);
RDB_IMPL_SERIALIZABLE_2_SINCE_v2_1(raft_config_t, voting_members, non_voting_members);
RDB_IMPL_SERIALIZABLE_2_SINCE_v2_1(raft_complex_config_t, config, new_config);

void debug_print(printf_buffer_t *buf, const raft_member_id_t &member_id) {
    debug_print(buf, member_id.uuid);
}

new_semaphore_t *get_raft_election_semaphore() {
    const int64_t max_concurrent_elections = 10;
    static new_semaphore_t raft_election_semaphore(max_concurrent_elections);
    return &raft_election_semaphore;
}
