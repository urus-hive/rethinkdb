// Copyright 2010-2016 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_EVICTION_MANAGER_HPP_
#define CLUSTERING_ADMINISTRATION_EVICTION_MANAGER_HPP_

#include "clustering/administration/auth/user_context.hpp"
#include "clustering/administration/namespace_interface_repository.hpp"
#include "clustering/query_routing/metadata.hpp"
#include "concurrency/cross_thread_signal.hpp"
#include "concurrency/watchable_map.hpp"
#include "rdb_protocol/changefeed.hpp"
#include "rdb_protocol/env.hpp"
#include "rdb_protocol/error.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/pseudo_time.hpp"
#include "rdb_protocol/shards.hpp"
#include "rpc/connectivity/peer_id.hpp"


class table_eviction_manager_t;
class eviction_internal_t : home_thread_mixin_t {
public:
    friend class table_eviction_manager_t;

    eviction_internal_t(eviction_config_t _eviction_config,
                      namespace_id_t _table_id,
                      region_t _region, //TODO check type
                      namespace_repo_t *_namespace_repo,
                      ql::changefeed::client_t *_changefeed_client,
                      table_meta_client_t *_table_meta_client)
        : eviction_config(_eviction_config),
          table_id(_table_id),
          region(_region),
          timer([&]() {
                  coro_t::spawn_now_dangerously([&]() {
                          on_timer();
                      });
              }),
          namespace_repo(_namespace_repo),
          changefeed_client(_changefeed_client),
          table_meta_client(_table_meta_client) {
        fprintf(stderr, "eviction_internal_t\n");
        // create a changefeed on this table/range to let us know when
        // the timer is supposed to change.
    }

    ~eviction_internal_t() {
        interrupt();
        debugf("EVICTION_INTERNAL DESTRUCTED!!!!!!!!!!!!!!!!!!!!!!!!!\n");
    }

    void changefeed_coro(auto_drainer_t *changefeed_drainer) {
        auto_drainer_t::lock_t lock(changefeed_drainer);
        auto_drainer_t::lock_t self_lock(&drainer);
        cross_thread_signal_t interruptor_on_home(&interruptor, home_thread());
        wait_any_t waiter(lock.get_drain_signal(), &interruptor_on_home);
        ql::env_t fake_env(&waiter,
                           ql::return_empty_normal_batches_t::NO,
                           reql_version_t::LATEST,
                           auth::user_context_t(auth::permissions_t(true, true, true)));
        ql::changefeed::keyspec_t::limit_t limit;
        ql::changefeed::keyspec_t::range_t range;

        range.sorting = sorting_t::ASCENDING;

        range.datumspec = ql::datumspec_t(ql::datum_range_t::universe())
            .trim_secondary(region.inner, reql_version_t::LATEST);
        debugf("Trimmed datumspec is %s\n", debug_str(range.datumspec.covering_range()).c_str());
        range.sindex = eviction_config.index_name;

        limit.range = range;
        limit.limit = 1;
        counted_t<ql::datum_stream_t> empty_stream =
            make_counted<ql::vector_datum_stream_t>(
                ql::backtrace_id_t(),
                std::vector<ql::datum_t>(),
                boost::none);
        ql::changefeed::streamspec_t ss(
            empty_stream,
            convert_uuid_to_datum(table_id).as_str().to_std(),
            false,
            false,
            false,
            ql::configured_limits_t::unlimited,
            ql::datum_t::boolean(false),
            limit);

        bool success = false;
        counted_t<ql::datum_stream_t> stream;

        bool done = false;
        while (!done) {
            while (!success) {
                fprintf(stderr, "trying...\n");
                try {
                    stream  = changefeed_client->new_stream(
                        &fake_env,
                        ss,
                        table_id,
                        ql::backtrace_id_t(),
                        table_meta_client);
                    success = true;
                } catch(ql::base_exc_t &ex) {
                    fprintf(stderr, "failed :( %s\n", ex.what());
                } catch(interrupted_exc_t &ex) {
                    break;
                }
                coro_t::yield();
            }

            // Try to get all changes as a test
            const ql::batchspec_t test_batchspec =
                ql::batchspec_t::default_for(
                    ql::batch_type_t::NORMAL_FIRST);
            while (success &&
                   !stream->is_exhausted() &&
                   !waiter.is_pulsed()) {
                coro_t::yield();
                try {
                    std::vector<ql::datum_t> test_datum =
                        stream->next_batch(&fake_env, test_batchspec);

                    // There should only be one element, which is the min
                    if (test_datum.size() > 0) {
                        ql::datum_t change = test_datum[0];
                        debugf("ELEMENT: %s\n", change.print().c_str());
                        guarantee(change.has());
                        ql::datum_t new_min = change.get_field("new_val",
                                                               ql::throw_bool_t::NOTHROW);
                        if (new_min.has() &&
                            new_min.get_type() != ql::datum_t::type_t::R_NULL) {
                            fprintf(stderr, "Datum: %s", debug_str(new_min).c_str());
                            // Set timer based on the value of the new lowest element
                            //TODO: get value of secondary index if it's not a field
                            wakeup_time = new_min.get_field(
                                datum_string_t(eviction_config.index_name),
                                ql::throw_bool_t::NOTHROW);
                            // TODO: don't assume type of index value is valid
                            double new_delay =
                                ql::pseudo::time_to_epoch_time(wakeup_time) -
                                ql::pseudo::time_to_epoch_time(ql::pseudo::time_now());
                            set_expiration(new_delay*1000);
                        }
                    }
                } catch (ql::base_exc_t &e) {
                    // TODO: deal with this
                    debugf("Exception in eviction changefeed: %s", e.what());
                    break;
                } catch (interrupted_exc_t) {
                    done = 1;
                    break;
                }
            }
        }
        debugf("THIS IS BAD, WE'RE NOT SUPPOSED TO BE HERE\n");
    }

    void interrupt() {
        debugf("Interrupting eviction_internal\n");
        interruptor.pulse();
    }

    void on_timer() {
        cross_thread_signal_t interruptor_on_home(&interruptor, home_thread());
        ql::env_t fake_env(&interruptor_on_home,
                           ql::return_empty_normal_batches_t::NO,
                           reql_version_t::LATEST,
                           auth::user_context_t(auth::permissions_t(true, true, true)));
        fprintf(stderr, "table_eviction_manager_t on_timer \n");
        int64_t new_sleep = -1;
        if (new_sleep != -1) {
            set_expiration(new_sleep);
        }

        namespace_interface_access_t interface_access =
            namespace_repo->get_namespace_interface(table_id, &interruptor);
        // Do a between to get all the keys on the index that need to be deleted

        ql::datum_range_t datum_range =
            ql::datum_range_t(ql::datum_t::minval(), key_range_t::open,
                              wakeup_time, key_range_t::closed);

        // Delete all the things on the index until now
        auth::user_context_t(auth::permissions_t(true, true, true));
        // TODO Need to get keys that actually exist
        std::vector<store_key_t> keys;

        // TODO get actual pkey
        std::string pkey = "id";

        // TODO make sure admin exists
        auth::user_context_t sudo_context(auth::username_t("admin"), false);

        // TODO make sure this is not bogus
        order_source_t order_source;

        // Do read to get keys

        fprintf(stderr, "===== Region: %s", debug_str(region).c_str());
        debugf("Datum Range is minval to %s\n", wakeup_time.print().c_str());
        sindex_rangespec_t sindex_rangespec(
            eviction_config.index_name,
            region,
            ql::datumspec_t(datum_range),
            require_sindexes_t::YES);

        rget_read_t read(
            boost::none,
            region_t::universe(), //TODO: smaller?
            boost::none,
            boost::none,
            ql::global_optargs_t(),
            sudo_context,
            convert_uuid_to_datum(table_id).as_str().to_std(),
            ql::batchspec_t::default_for(ql::batch_type_t::TERMINAL),
            std::vector<ql::transform_variant_t>(),
            boost::none,
            sindex_rangespec,
            sorting_t::ASCENDING);

        read_t between_read(
            read,
            profile_bool_t::DONT_PROFILE,
            read_mode_t::MAJORITY);

        bool trying_read = true;
        read_response_t read_response;
        while (trying_read) {
            try {
                interface_access.get()->read(
                    sudo_context,
                    between_read,
                    &read_response,
                    order_token_t::ignore,//read_token.with_read_mode(),
                    &interruptor);
                trying_read = false;
            } catch (cannot_perform_query_exc_t &ex) {
                debugf("%s\n", ex.what());
                // Do nothing
            }
            coro_t::yield();
        }

        auto rget_res = boost::get<rget_read_response_t>(&read_response.response);
        r_sanity_check(rget_res != nullptr);
        if (auto e = boost::get<ql::exc_t>(&rget_res->result)) {
            throw *e;
        }

        // TODO: is this the best way, or should I use a different acc
        scoped_ptr_t<ql::eager_acc_t> to_array = ql::make_to_array();
        to_array->add_res(&fake_env,
                          &rget_res->result,
                          sorting_t::ASCENDING);
        scoped_ptr_t<ql::val_t> value =
            to_array->finish_eager(
                ql::backtrace_id_t(),
                false,
                ql::configured_limits_t::unlimited);

        ql::datum_t elements_array = value->as_datum();
        guarantee(elements_array.get_type() == ql::datum_t::R_ARRAY);

        fprintf(stderr, "There are %lu elements.\n", elements_array.arr_size());
        // TODO batch these deletes
        if (elements_array.arr_size() > 0) {
            for (size_t i = 0; i < elements_array.arr_size(); ++i) {
                keys.push_back(
                    store_key_t(
                        elements_array
                        .get(i)
                        .get_field(datum_string_t(pkey))
                        .print_primary()
                    ));
            }
            // Do write to delete elements
            batched_replace_t replace(
                std::move(keys),
                pkey,
                eviction_config.func.compile_wire_func(),
                ql::global_optargs_t(),
                sudo_context,
                return_changes_t::NO);
            write_t replace_write(replace,
                                  profile_bool_t::DONT_PROFILE,
                                  ql::configured_limits_t::unlimited);


            write_response_t write_response;

            bool trying_write = true;
            while (trying_write) {
                try {
                    interface_access.get()->write(
                        sudo_context,
                        replace_write,
                        &write_response,
                        order_token_t::ignore,
                        &interruptor);
                    trying_write = false;
                } catch(cannot_perform_query_exc_t &ex) {
                    // Do nothing
                }
                coro_t::yield();
            }
        }

        // Not sure we actually care about the response, because failed deletes will
        // TODO be handled later, but look into it.
        // DONE!
    }

    void set_expiration(int64_t _ms) {
        debugf("Setting expiration for region %s\n", debug_str(region).c_str());
        // TODO: think about negative time/ monotonic time
        timer.cancel();
        if (_ms <= 0) {
            on_timer();
        } else {
            timer.start(_ms);
        }
    }

private:
    cond_t interruptor;
    eviction_config_t eviction_config;

    namespace_id_t table_id;
    region_t region;

    single_callback_timer_t timer;

    ql::datum_t wakeup_time;

    namespace_repo_t *namespace_repo;
    ql::changefeed::client_t *changefeed_client;
    table_meta_client_t *table_meta_client;

    auto_drainer_t drainer;
};

enum class eviction_change_t { DROP = 0, ADD = 1 };
class table_eviction_manager_t {
public:
    table_eviction_manager_t(namespace_id_t _table_id,
                             region_t _region,
                             ql::changefeed::client_t *_changefeed_client,
                             table_meta_client_t *_table_meta_client,
                             namespace_repo_t *_namespace_repo)
        : region(_region),
          changefeed_client(_changefeed_client),
          table_meta_client(_table_meta_client),
          namespace_repo(_namespace_repo),
          table_id(_table_id) {
        // TODO construct changefeed to watch things we're the primary for
        fprintf(stderr, "table_eviction_manager_t\n");
    }

    ~table_eviction_manager_t() {
        fprintf(stderr, "~table_eviction_manager_t\n");
        interruptor.pulse();
    }

    void handle_eviction_change(std::string name, eviction_change_t change) {
        fprintf(stderr, "table_eviction_manager for %s, handling eviction change\n",
                debug_str(table_id).c_str());
        table_config_and_shards_t config;
        table_meta_client->get_config(table_id,
                                      &interruptor,
                                      &config);
        std::map<std::string, sindex_config_t> sindexes = config.config.sindexes;

        if (change == eviction_change_t::ADD) {
            for (auto &pair : sindexes) {
                auto eviction = pair.second.eviction_list.find(name);
                if (eviction != pair.second.eviction_list.end()) {
                    evictions[eviction->first] =
                        make_scoped<eviction_internal_t>(
                            eviction->second,
                            table_id,
                            region,
                            namespace_repo,
                            changefeed_client,
                            table_meta_client);
                    coro_t::spawn_now_dangerously([&]() {
                            evictions[eviction->first]->changefeed_coro(
                                &(changefeed_client->drainer));
                        });
                }
            }
        } else {
            auto eviction = evictions.find(name);
            if (eviction != evictions.end()) {
                evictions.erase(name);
            }
        }
    }

    void handle_directory_change(table_query_bcard_t value) {
        fprintf(stderr, "table_eviction_manager for %s, handling %s\n",
                debug_str(table_id).c_str(),
                debug_str(&value).c_str());

        table_config_and_shards_t config;
        table_meta_client->get_config(table_id,
                                      &interruptor,
                                      &config);

        std::map<std::string, sindex_config_t> sindexes = config.config.sindexes;

        // For each eviction/sindex, create an eviction_internal_t

        // Update region if it's changed
        region = value.region;
        // TODO: is there a better way than just destroying and recreating the
        // eviction_internal_t things
        for (auto pair : sindexes) {
            for (auto eviction : pair.second.eviction_list) {
                debugf("*************************\n");

                debugf("Creating eviction_internal for %s in region %s\n",
                       eviction.first.c_str(),
                       debug_str(region).c_str());
                if (evictions.find(eviction.first) != evictions.end()) {
                    debugf("Deleting old eviction_internal\n");
                    evictions[eviction.first].reset();
                }

                evictions[eviction.first] =
                    make_scoped<eviction_internal_t>(
                        eviction.second,
                        table_id,
                        region,
                        namespace_repo,
                        changefeed_client,
                        table_meta_client);
                coro_t::spawn_now_dangerously([&]() {
                        evictions[eviction.first]->changefeed_coro(
                            &(changefeed_client->drainer));
                    });
            }
        }
    }

private:
    cond_t interruptor;

    region_t region;
    ql::changefeed::client_t *changefeed_client;
    table_meta_client_t *table_meta_client;
    namespace_repo_t *namespace_repo;

    std::map<std::string, scoped_ptr_t<eviction_internal_t> > evictions;
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

    ~eviction_manager_t() { }

    void on_directory_change_coro(
        std::pair<peer_id_t, std::pair<namespace_id_t, branch_id_t> > key,
        const table_query_bcard_t *value) {
        namespace_id_t table_id = key.second.first;

        bool added = false;
        bool dropped = false;
        if (key.first == server_id) {
            if (value != nullptr) {
                if (value->primary) {
                    added = true;
                } else {
                    // We became a secondary, also drop it
                    dropped = true;
                }
            } else {
                // Deleted from the map, so delete it
                dropped = true;
            }
        }
        if (added) {
            // Keep our local directory updated
            region_t region = value->primary->region;
            debugf("Adding eviction for region %s\n", debug_str(region).c_str());
            if (table_managers.find(key.second) == table_managers.end()) {
                // Create new table_eviction_manager for table
                table_managers[key.second] =
                    make_scoped<table_eviction_manager_t>(
                        table_id,
                        region,
                        changefeed_client,
                        table_meta_client,
                        namespace_repo);
            }
            coro_t::spawn_now_dangerously([&](){
                    table_managers[key.second]->handle_directory_change(*value);
                });
        } else if (dropped) {
            debugf("Removing eviction\n");
            // We may not have anything but the key
            if (table_managers.find(key.second) != table_managers.end()) {
                table_managers.erase(key.second);
            }
        }
    }

    void on_directory_change(
        std::pair<peer_id_t, std::pair<namespace_id_t, branch_id_t> > key,
        const table_query_bcard_t *value) {
        coro_t::spawn_now_dangerously([=]() {
                on_directory_change_coro(key, value);
            });
    }

    void on_eviction_change(std::string eviction_name, eviction_change_t change) {
        for (auto pair = table_managers.begin(); pair != table_managers.end(); ++pair) {
            debugf("Updating evictions!\n");
            coro_t::spawn_now_dangerously([=]() {
                    pair->second->handle_eviction_change(eviction_name, change);
                });
        }
    }
private:
    std::map<std::pair<namespace_id_t, branch_id_t>,
             scoped_ptr_t<table_eviction_manager_t> > table_managers;

    peer_id_t server_id;

    ql::changefeed::client_t *changefeed_client;
    table_meta_client_t *table_meta_client;
    namespace_repo_t *namespace_repo;

    UNUSED watchable_map_t<std::pair<peer_id_t, std::pair<namespace_id_t, branch_id_t> >,
                           table_query_bcard_t> *directory;
    watchable_map_t<std::pair<peer_id_t, std::pair<namespace_id_t, branch_id_t> >,
                    table_query_bcard_t>::all_subs_t directory_subs;

};

#endif // #ifndef CLUSTERING_ADMINISTRATION_EVICTION_MANAGER_HPP_
