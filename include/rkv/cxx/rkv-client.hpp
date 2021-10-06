/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_CLIENT_HPP
#define __RKV_CLIENT_HPP

#include <rkv/rkv-client.h>
#include <rkv/cxx/rkv-exception.hpp>
#include <rkv/cxx/rkv-database.hpp>

namespace rkv {

class Client {

    public:

    Client(margo_instance_id mid) {
        auto err = rkv_client_init(mid, &m_client);
        RKV_CONVERT_AND_THROW(err);
    }

    ~Client() {
        if(m_client != RKV_CLIENT_NULL) {
            rkv_client_finalize(m_client);
        }
    }

    Client(Client&& other)
    : m_client(other.m_client) {
        other.m_client = RKV_CLIENT_NULL;
    }

    Client(const Client&) = delete;

    Client& operator=(const Client&) = delete;

    Client& operator=(Client&&) = delete;

    Database makeDatabaseHandle(
        hg_addr_t addr,
        uint16_t provider_id,
        rkv_database_id_t database_id) const {
        rkv_database_handle_t db;
        auto err = rkv_database_handle_create(
            m_client, addr, provider_id, database_id, &db);
        RKV_CONVERT_AND_THROW(err);
        return Database(db, false);
    }

    auto handle() const {
        return m_client;
    }

    private:

    rkv_client_t m_client = RKV_CLIENT_NULL;
};

}

#endif
