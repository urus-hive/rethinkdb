// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/terms/terms.hpp"

#include <string>

#include "clustering/administration/admin_op_exc.hpp"
#include "containers/archive/string_stream.hpp"
#include "rdb_protocol/real_table.hpp"
#include "rdb_protocol/btree.hpp"
#include "rdb_protocol/error.hpp"
#include "rdb_protocol/func.hpp"
#include "rdb_protocol/op.hpp"
#include "rdb_protocol/minidriver.hpp"
#include "rdb_protocol/term_walker.hpp"

namespace ql {

class modifier_create_term_t : public op_term_t {
public:
    modifier_create_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(2)) { }

    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        counted_t<table_t> table = args->arg(env, 0)->as_table();

        /* Parse the modifier configuration */
        modifier_config_t config;

        scoped_ptr_t<val_t> v = args->arg(env, 1);
        // We ignore the modifier's old `reql_version` and make the new version
        // just be `reql_version_t::LATEST`; but in the future we may have
        // to do some conversions for compatibility.
        config.func = ql::map_wire_func_t(v->as_func());
        config.func_version = reql_version_t::LATEST;

        config.func.compile_wire_func()->assert_deterministic(
            "Modifier functions must be deterministic.");

        boost::optional<size_t> arity = config.func.compile_wire_func()->arity();

        rcheck(static_cast<bool>(arity) && arity.get() == 2,
               base_exc_t::LOGIC,
               strprintf("Modifier functions must expect 2 arguments."));

        try {
            admin_err_t error;
            if (!env->env->reql_cluster_interface()->modifier_create(
                    env->env->get_user_context(),
                    table->db,
                    name_string_t::guarantee_valid(table->name.c_str()),
                    config,
                    env->env->interruptor,
                    &error)) {
                REQL_RETHROW(error);
            }
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        ql::datum_object_builder_t res;
        res.overwrite("created", datum_t(1.0));
        return new_val(std::move(res).to_datum());
    }

    virtual const char *name() const { return "modifier_create"; }
};

class modifier_drop_term_t : public op_term_t {
public:
    modifier_drop_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1)) { }

    virtual scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        counted_t<table_t> table = args->arg(env, 0)->as_table();

        try {
            admin_err_t error;
            if (!env->env->reql_cluster_interface()->modifier_drop(
                    env->env->get_user_context(),
                    table->db,
                    name_string_t::guarantee_valid(table->name.c_str()),
                    env->env->interruptor,
                    &error)) {
                REQL_RETHROW(error);
            }
        } catch (auth::permission_error_t const &permission_error) {
            rfail(ql::base_exc_t::PERMISSION_ERROR, "%s", permission_error.what());
        }

        ql::datum_object_builder_t res;
        res.overwrite("dropped", datum_t(1.0));
        return new_val(std::move(res).to_datum());
    }

    virtual const char *name() const { return "modifier_drop"; }
};

counted_t<term_t> make_modifier_create_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<modifier_create_term_t>(env, term);
}
counted_t<term_t> make_modifier_drop_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<modifier_drop_term_t>(env, term);
}

} // namespace ql
