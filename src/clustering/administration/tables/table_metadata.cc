// Copyright 2010-2014 RethinkDB, all rights reserved.
#include "clustering/administration/tables/table_metadata.hpp"

#include "clustering/administration/tables/database_metadata.hpp"
#include "containers/archive/archive.hpp"
#include "containers/archive/boost_types.hpp"
#include "containers/archive/stl_types.hpp"
#include "containers/archive/versioned.hpp"
#include "rdb_protocol/protocol.hpp"

RDB_IMPL_SERIALIZABLE_3_SINCE_v2_1(table_basic_config_t,
    name, database, primary_key);
RDB_IMPL_EQUALITY_COMPARABLE_3(table_basic_config_t,
    name, database, primary_key);

RDB_IMPL_SERIALIZABLE_3_SINCE_v2_1(table_config_t::shard_t,
    all_replicas, nonvoting_replicas, primary_replica);
RDB_IMPL_EQUALITY_COMPARABLE_3(table_config_t::shard_t,
    all_replicas, nonvoting_replicas, primary_replica);

template <cluster_version_t W>
void serialize(write_message_t *wm, const table_config_t &tc) {
    table_basic_config_t basic = tc.basic;
    serialize<W>(wm, basic);

    std::vector<table_config_t::shard_t> shards = tc.shards;
    serialize<W>(wm, shards);

    std::map<std::string, sindex_config_t> sindexes = tc.sindexes;
    serialize<W>(wm, sindexes);

    boost::optional<modifier_config_t> modifier = tc.modifier;
    serialize<W>(wm, modifier);

    write_ack_config_t write_ack_config = tc.write_ack_config;
    serialize<W>(wm, write_ack_config);

    write_durability_t durability = tc.durability;
    serialize<W>(wm, durability);
}

INSTANTIATE_SERIALIZE_FOR_CLUSTER_AND_DISK(table_config_t);

template <cluster_version_t  W>
archive_result_t deserialize(
    read_stream_t *s, table_config_t *tc) {
    archive_result_t res;

    table_basic_config_t basic;
    res = deserialize<W>(s, &basic);
    if (bad(res)) { return res; }

    std::vector<table_config_t::shard_t> shards;
    res = deserialize<W>(s, &shards);
    if (bad(res)) { return res; }

    std::map<std::string, sindex_config_t> sindexes;
    res = deserialize<W>(s, &sindexes);
    if (bad(res)) { return res; }

    write_ack_config_t write_ack_config;
    res = deserialize<W>(s, &write_ack_config);
    if (bad(res)) { return res; }

    write_durability_t durability;
    res = deserialize<W>(s, &durability);
    if (bad(res)) { return res; }

    tc->basic = basic;
    tc->shards = shards;
    tc->sindexes = sindexes;
    tc->write_ack_config = write_ack_config;
    tc->durability = durability;

    return res;
}

template archive_result_t deserialize<cluster_version_t::v2_1>(
        read_stream_t *, table_config_t *);
template archive_result_t deserialize<cluster_version_t::v2_2>(
        read_stream_t *, table_config_t *);
template archive_result_t deserialize<cluster_version_t::v2_3>(
        read_stream_t *, table_config_t *);

template <cluster_version_t  W>
archive_result_t deserialize_table_config(
    read_stream_t *s, table_config_t *tc) {
    archive_result_t res;

    table_basic_config_t basic;
    res = deserialize<W>(s, &basic);
    if (bad(res)) { return res; }

    std::vector<table_config_t::shard_t> shards;
    res = deserialize<W>(s, &shards);
    if (bad(res)) { return res; }

    std::map<std::string, sindex_config_t> sindexes;
    res = deserialize<W>(s, &sindexes);
    if (bad(res)) { return res; }

    boost::optional<modifier_config_t> modifier;
    res = deserialize<W>(s, &modifier);
    if (bad(res)) { return res; }

    write_ack_config_t write_ack_config;
    res = deserialize<W>(s, &write_ack_config);
    if (bad(res)) { return res; }

    write_durability_t durability;
    res = deserialize<W>(s, &durability);
    if (bad(res)) { return res; }

    *tc = table_config_t{basic,
                         shards,
                         sindexes,
                         modifier,
                         write_ack_config,
                         durability};

    return res;
}

template <>
archive_result_t deserialize<cluster_version_t::v2_4_is_latest>(
    read_stream_t *s, table_config_t *tc) {
    return deserialize_table_config<cluster_version_t::v2_4_is_latest>(s, tc);
}


RDB_IMPL_EQUALITY_COMPARABLE_6(table_config_t,
    basic, shards, modifier, sindexes, write_ack_config, durability);

RDB_IMPL_SERIALIZABLE_1_SINCE_v1_16(table_shard_scheme_t, split_points);
RDB_IMPL_EQUALITY_COMPARABLE_1(table_shard_scheme_t, split_points);

RDB_IMPL_SERIALIZABLE_3_SINCE_v2_1(table_config_and_shards_t,
                                    config, shard_scheme, server_names);
RDB_IMPL_EQUALITY_COMPARABLE_3(table_config_and_shards_t,
                               config, shard_scheme, server_names);

RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(
    table_config_and_shards_change_t::set_table_config_and_shards_t,
    new_config_and_shards);
RDB_IMPL_SERIALIZABLE_2_FOR_CLUSTER(table_config_and_shards_change_t::sindex_create_t,
    name, config);
RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(table_config_and_shards_change_t::sindex_drop_t,
    name);
RDB_IMPL_SERIALIZABLE_3_FOR_CLUSTER(table_config_and_shards_change_t::sindex_rename_t,
    name, new_name, overwrite);

RDB_IMPL_SERIALIZABLE_1_FOR_CLUSTER(table_config_and_shards_change_t::modifier_create_t,
    config);
RDB_IMPL_SERIALIZABLE_0_FOR_CLUSTER(table_config_and_shards_change_t::modifier_drop_t);

RDB_IMPL_SERIALIZABLE_1_SINCE_v1_13(database_semilattice_metadata_t, name);
RDB_IMPL_SEMILATTICE_JOINABLE_1(database_semilattice_metadata_t, name);
RDB_IMPL_EQUALITY_COMPARABLE_1(database_semilattice_metadata_t, name);

RDB_IMPL_SERIALIZABLE_1_SINCE_v1_13(databases_semilattice_metadata_t, databases);
RDB_IMPL_SEMILATTICE_JOINABLE_1(databases_semilattice_metadata_t, databases);
RDB_IMPL_EQUALITY_COMPARABLE_1(databases_semilattice_metadata_t, databases);

