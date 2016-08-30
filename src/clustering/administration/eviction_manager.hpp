// Copyright 2010-2016 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_EVICTION_MANAGER_HPP_
#define CLUSTERING_ADMINISTRATION_EVICTION_MANAGER_HPP_

#include "clustering/administration/auth/user_context.hpp"
#include "clustering/administration/namespace_interface_repository.hpp"
#include "clustering/query_routing/metadata.hpp"
#include "concurrency/watchable_map.hpp"
#include "rdb_protocol/changefeed.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/error.hpp"
#include "rpc/connectivity/peer_id.hpp"

class table_eviction_manager_t {
public:
    table_eviction_manager_t(namespace_id_t _table_id,
                             ql::changefeed::client_t *_changefeed_client,
                             table_meta_client_t *_table_meta_client)
        : timer([&](){ on_timer(); }),
          changefeed_client(_changefeed_client),
          table_meta_client(_table_meta_client),
          table_id(_table_id)
      {
        // TODO construct changefeed to watch things we're the primary for
        fprintf(stderr, "table_eviction_manager_t\n");
    }

    ~table_eviction_manager_t() {
        fprintf(stderr, "~table_eviction_manager_t\n");
        interruptor.pulse();
    }

    void create_changefeed_coro(region_t region) {
        ql::env_t fake_env(&interruptor,
                           ql::return_empty_normal_batches_t::NO,
                           reql_version_t::LATEST,
                           auth::user_context_t(auth::permissions_t(true, true, true)));
        ql::changefeed::keyspec_t::limit_t limit;
        ql::changefeed::keyspec_t::range_t range;

        range.sorting = sorting_t::ASCENDING;
        range.datumspec = ql::datumspec_t(ql::datum_range_t::universe())
            .trim_secondary(region.inner, reql_version_t::LATEST);
        range.sindex = "num";

        limit.range = range;
        limit.limit = 1;
        ql::changefeed::streamspec_t ss(
            counted_t<ql::datum_stream_t>(),
            convert_uuid_to_datum(table_id).as_str().to_std(),
            false,
            false,
            false,
            ql::configured_limits_t::unlimited,
            ql::datum_t::boolean(false),
            limit);
        bool success = false;
        counted_t<ql::datum_stream_t> stream;
        while (!success) {
            fprintf(stderr, "trying...\n");
            try {
                stream  = changefeed_client->new_stream(
                    &fake_env,
                    ss,
                    table_id,
                    ql::backtrace_id_t(),
                    table_meta_client.get());
                success = true;
            } catch(ql::base_exc_t &ex) {
                fprintf(stderr, "failed :(\n");
            }
        }

        // Try to get all changes as a test
        const ql::batchspec_t test_batchspec =
            ql::batchspec_t::default_for(
                ql::batch_type_t::NORMAL_FIRST);
        while (!stream->is_exhausted() && !interruptor.is_pulsed()) {
            std::vector<ql::datum_t> test_datum =
                stream->next_batch(&fake_env, test_batchspec);
            for (auto dat : test_datum) {
                fprintf(stderr, "Datum: %s", debug_str(dat).c_str());
            }
            coro_t::yield();
        }
    }
    void handle_directory_change(const table_query_bcard_t *value) {
        fprintf(stderr, "table_eviction_manager for %s, handling %s\n",
                debug_str(table_id).c_str(),
                debug_str(value).c_str());

        // Get changefeed
        region_t region = value->region;
            //changespec_t()

        coro_t::spawn_sometime([&](){create_changefeed_coro(region);});
    }

    void set_expiration(int64_t _ms) {
        timer.cancel();
        timer.start(_ms);
    }

    void on_timer() {
        fprintf(stderr, "table_eviction_manager_t on_timer \n");
        int64_t new_sleep = -1;
        if (new_sleep != -1) {
            set_expiration(new_sleep);
        }

        // Try sending a test delete
        auth::user_context_t(auth::permissions_t(true, true, true));
        //write_t write = batched_replace_t();
        //namespace_repo->get_namespace_interface()->get()->write();
    }

private:
    cond_t interruptor;
    single_callback_timer_t timer;

    scoped_ptr_t<ql::changefeed::client_t> changefeed_client;
    scoped_ptr_t<table_meta_client_t> table_meta_client;

    namespace_id_t table_id;

};

class eviction_manager_t {
public:
    eviction_manager_t(
        peer_id_t _server_id,
        ql::changefeed::client_t *_changefeed_client,
        table_meta_client_t *_table_meta_client,
        namespace_repo_t *_namespace_repo,
        watchable_map_t<std::pair<peer_id_t, std::pair<namespace_id_t, branch_id_t> >,
        table_query_bcard_t> *d) :
        server_id(_server_id),
        changefeed_client(_changefeed_client),
        table_meta_client(_table_meta_client),
        namespace_repo(_namespace_repo),
        directory(d),
        directory_subs(
            d,
            [&](const std::pair<peer_id_t, std::pair<namespace_id_t, branch_id_t> > &k,
                const table_query_bcard_t *v) {
                on_directory_change(k, v); }){
        ;;
    }

    ~eviction_manager_t() {
        fprintf(stderr, "eviction_manager_t\n");
    }

    void on_directory_change(
        std::pair<peer_id_t, std::pair<namespace_id_t, branch_id_t> > key,
        const table_query_bcard_t *value) {

        namespace_id_t table_id = key.second.first;
        fprintf(stderr, "BRANCH ID ============== %s\n", debug_str(key.second.second).c_str());

        if (value != nullptr &&
            value->primary &&
            key.first == server_id) {
            // Keep our local directory updated
            fprintf(stderr, "This is OURS! %s --- %s\n",
                    debug_str(key.first).c_str(),
                    debug_str(key.second).c_str());
            region_t region = value->primary->region;

            if (table_managers.find(table_id) == table_managers.end()) {
                // Create new table_eviction_manager for table
                table_managers[table_id] =
                    make_scoped<table_eviction_manager_t>(
                        table_id,
                        changefeed_client.get(),
                        table_meta_client.get());
                fprintf(stderr,
                        "Created TABLE_EVICTION_MANAGER_T for %s\n",
                        debug_str(table_id).c_str());

                table_managers[table_id]->handle_directory_change(value);
            }
        }
    }
private:
    std::map<namespace_id_t, scoped_ptr_t<table_eviction_manager_t> > table_managers;

    peer_id_t server_id;

    scoped_ptr_t<ql::changefeed::client_t> changefeed_client;
    scoped_ptr_t<table_meta_client_t> table_meta_client;
    scoped_ptr_t<namespace_repo_t> namespace_repo;

    UNUSED watchable_map_t<std::pair<peer_id_t, std::pair<namespace_id_t, branch_id_t> >,
                    table_query_bcard_t> *directory;
    watchable_map_t<std::pair<peer_id_t, std::pair<namespace_id_t, branch_id_t> >,
                    table_query_bcard_t>::all_subs_t directory_subs;

};

#endif // #ifndef CLUSTERING_ADMINISTRATION_EVICTION_MANAGER_HPP_
