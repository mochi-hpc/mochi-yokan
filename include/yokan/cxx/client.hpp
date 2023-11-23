/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_CLIENT_HPP
#define __YOKAN_CLIENT_HPP

#include <yokan/client.h>
#include <yokan/database.h>
#include <yokan/cxx/exception.hpp>
#include <yokan/cxx/database.hpp>

namespace yokan {

class Client {

    public:

    Client() = default;

    Client(margo_instance_id mid) {
        auto err = yk_client_init(mid, &m_client);
        YOKAN_CONVERT_AND_THROW(err);
    }

    ~Client() {
        if(m_client != YOKAN_CLIENT_NULL) {
            yk_client_finalize(m_client);
        }
    }

    Client(Client&& other)
    : m_client(other.m_client) {
        other.m_client = YOKAN_CLIENT_NULL;
    }

    Client(const Client&) = delete;

    Client& operator=(const Client&) = delete;

    Client& operator=(Client&& other) {
        if(this == &other) return *this;
        if(m_client != YOKAN_CLIENT_NULL) {
            yk_client_finalize(m_client);
        }
        m_client = other.m_client;
        other.m_client = YOKAN_CLIENT_NULL;
        return *this;
    }

    Database makeDatabaseHandle(
        hg_addr_t addr,
        uint16_t provider_id) const {
        yk_database_handle_t db;
        auto err = yk_database_handle_create(
            m_client, addr, provider_id, &db);
        YOKAN_CONVERT_AND_THROW(err);
        return Database(db, false);
    }

    auto handle() const {
        return m_client;
    }

    private:

    yk_client_t m_client = YOKAN_CLIENT_NULL;
};

}

#endif
