// Copyright 2010-2016 RethinkDB, all rights reserved.
#include "rdb_protocol/terms/terms.hpp"

#include <string>

// TODO: Check to see if we need all these.
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

class eviction_create_term_t : public op_term_t {
public:
    eviction_create_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1),
                        optargspec_t({"trigger", "delay", "index", "replace"})) { }

    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        counted_t<table_t> table = args->arg(env, 0)->as_table();
        datum_t name_datum = args->arg(env, 1)->as_datum();

        // TODO: check index name is a real index
        eviction_config_t config;

        scoped_ptr_t<val_t> index_name = args->optarg(env, "index");
        rcheck(index_name.has(),
               base_exc_t::LOGIC,
               "Index name is required.");
        config.index_name = index_name->as_str().to_std();

        bool got_func = false;
        scoped_ptr_t<val_t> replace_function = args->optarg(env, "replace");
        if (replace_function) {
            config.func = ql::map_wire_func_t(replace_function->as_func());
            config.func_version = reql_version_t::LATEST;
        } else {
            minidriver_t r(backtrace());
            auto x = minidriver_t::dummy_var_t::EVICTIONCREATE_X;
            compile_env_t empty_compile_env((var_visibility_t()));
            counted_t<func_term_t> func_term =
                make_counted<func_term_t>(&empty_compile_env,
                                          r.fun(x, r.null()).root_term());
            config.func = ql::map_wire_func_t(func_term->eval_to_func(env->scope));
            config.func_version = reql_version_t::LATEST;
        }

        try {
            admin_err_t error;
            if (!env->env->reql_cluster_interface()->eviction_create(
                env->env->get_user_context(),
                table->db,
                name_string_t::guarantee_valid(table->name.c_str()),
                    index_name->as_str().to_std(),
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
    virtual const char *name() const { return "eviction_create"; }
};

class eviction_drop_term_t : public op_term_t {
public:
    eviction_drop_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env,
                    term,
                    argspec_t(1)) { }

    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        counted_t<table_t> table = args->arg(env, 0)->as_table();
    }
    virtual const char *name() const { return "eviction_drop"; }
};

class eviction_list_term_t : public op_term_t {
public:
    eviction_list_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env,
                    term,
                    argspec_t(1)) { }

    virtual scoped_ptr_t<val_t> eval_impl(
        scope_env_t *env, args_t *args, eval_flags_t) const {
        counted_t<table_t> table = args->arg(env, 0)->as_table();
    }
    virtual const char *name() const { return "eviction_list"; }
};

counted_t<term_t> make_eviction_create_term(
    compile_env_t *env, const raw_term_t &term) {
    return make_counted<eviction_create_term_t>(env, term);
}
counted_t<term_t> make_eviction_drop_term(
    compile_env_t *env, const raw_term_t &term) {
    return make_counted<eviction_drop_term_t>(env, term);
}
counted_t<term_t> make_eviction_list_term(
    compile_env_t *env, const raw_term_t &term) {
    return make_counted<eviction_list_term_t>(env, term);
}

} // namespace ql
