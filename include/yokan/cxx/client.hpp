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
#include <memory>

namespace yokan {

class Client {

    public:

    Client() = default;

    Client(margo_instance_id mid) {
        yk_client_t client;
        auto err = yk_client_init(mid, &client);
        YOKAN_CONVERT_AND_THROW(err);
        m_client = std::shared_ptr<yk_client>{
            client, yk_client_finalize};
    }

    Client(Client&& other) = default;

    Client(const Client&) = default;

    Client& operator=(const Client&) = default;

    Client& operator=(Client&& other) = default;

    Database makeDatabaseHandle(
        hg_addr_t addr,
        uint16_t provider_id,
        bool check = true) const {
        yk_database_handle_t db;
        auto err = yk_database_handle_create(
            handle(), addr, provider_id, check, &db);
        YOKAN_CONVERT_AND_THROW(err);
        return Database(db, false, m_client);
    }

    yk_client_t handle() const {
        return m_client.get();
    }

    private:

    std::shared_ptr<yk_client> m_client;
};

}

#endif
