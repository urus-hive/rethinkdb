// Copyright 2010-2016 RethinkDB, all rights reserved.
#ifndef CLUSTERING_ADMINISTRATION_TABLES_NAME_RESOLVER_HPP_
#define CLUSTERING_ADMINISTRATION_TABLES_NAME_RESOLVER_HPP_

#include "errors.hpp"
#include <boost/optional.hpp>

#include "clustering/administration/artificial_reql_cluster_interface.hpp"
#include "clustering/administration/metadata.hpp"
#include "clustering/table_manager/table_meta_client.hpp"
#include "containers/name_string.hpp"

template <typename T>
class name_resolver_optional_t {
public:
    struct no_such_name_t { };
    struct ambiguous_name_t { };

    explicit name_resolver_optional_t(T const &value)
        : m_optional(value),
          m_error(error::NO_ERROR) { }
    explicit name_resolver_optional_t(T &&value)
        : m_optional(std::forward<T>(value)),
          m_error(error::NO_ERROR) { }
    explicit name_resolver_optional_t(no_such_name_t)
        : m_optional(boost::none),
          m_error(error::NO_SUCH_NAME) { }
    explicit name_resolver_optional_t(ambiguous_name_t)
        : m_optional(boost::none),
          m_error(error::AMBIGUOUS_NAME) { }

    explicit operator bool() const noexcept {
        return m_error == error::NO_ERROR;
    }

    T const &get() const {
        return m_optional.get();
    }

    bool is_no_such() const noexcept {
        return m_error == error::NO_SUCH_NAME;
    }

    bool is_ambiguous() const noexcept {
        return m_error == error::AMBIGUOUS_NAME;
    }

private:
    boost::optional<T> m_optional;
    enum class error { NO_ERROR = 0, NO_SUCH_NAME, AMBIGUOUS_NAME } m_error;
};

class name_resolver_t
{
public:
    name_resolver_t(
            boost::shared_ptr<semilattice_read_view_t<cluster_semilattice_metadata_t>>,
            table_meta_client_t *,
            artificial_reql_cluster_interface_t const &);

    cluster_semilattice_metadata_t get_cluster_metadata() const noexcept;

    boost::optional<name_string_t> database_id_to_name(
            database_id_t const &) const noexcept;
    boost::optional<name_string_t> database_id_to_name(
            database_id_t const &,
            cluster_semilattice_metadata_t const &) const noexcept;

    boost::optional<table_basic_config_t> table_id_to_basic_config(
            namespace_id_t const &,
            boost::optional<database_id_t> const & = boost::none) const noexcept;

    name_resolver_optional_t<database_id_t> database_name_to_id(
            name_string_t const &) const noexcept;
    name_resolver_optional_t<database_id_t> database_name_to_id(
            name_string_t const &,
            cluster_semilattice_metadata_t const &) const noexcept;

    name_resolver_optional_t<namespace_id_t> table_name_to_id(
            database_id_t const &,
            name_string_t const &) const noexcept;

private:
    boost::shared_ptr<semilattice_read_view_t<cluster_semilattice_metadata_t>>
        m_cluster_semilattice_view;
    table_meta_client_t *m_table_meta_client;
    artificial_reql_cluster_interface_t const &m_artificial_reql_cluster_interface;
};

#endif  // CLUSTERING_ADMINISTRATION_TABLES_NAME_RESOLVER_HPP_
