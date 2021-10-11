/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_ADMIN_HPP
#define __YOKAN_ADMIN_HPP

#include <yokan/admin.h>
#include <yokan/cxx/exception.hpp>
#include <vector>

namespace yokan {

class Admin {

    public:

    Admin(margo_instance_id mid) {
        auto ret = yk_admin_init(mid, &m_admin);
        YOKAN_CONVERT_AND_THROW(ret);
    }

    ~Admin() {
        if(m_admin != YOKAN_ADMIN_NULL) {
            yk_admin_finalize(m_admin);
        }
    }

    Admin(Admin&& other)
    : m_admin(other.m_admin) {
        other.m_admin = YOKAN_ADMIN_NULL;
    }

    Admin(const Admin&) = delete;

    Admin& operator=(const Admin&) = delete;

    Admin& operator=(Admin&&) = delete;

    yk_database_id_t openDatabase(
            hg_addr_t address,
            uint16_t provider_id,
            const char* token,
            const char* type,
            const char* config) const {
        yk_database_id_t id;
        auto ret = yk_open_database(
            m_admin, address, provider_id,
            token, type, config, &id);
        YOKAN_CONVERT_AND_THROW(ret);
        return id;
    }

    void closeDatabase(
            hg_addr_t address,
            uint16_t provider_id,
            const char* token,
            yk_database_id_t id) const {
        auto ret = yk_close_database(
            m_admin, address, provider_id,
            token, id);
        YOKAN_CONVERT_AND_THROW(ret);
    }

    void destroyDatabase(
            hg_addr_t address,
            uint16_t provider_id,
            const char* token,
            yk_database_id_t id) const {
        auto ret = yk_destroy_database(
            m_admin, address, provider_id,
            token, id);
        YOKAN_CONVERT_AND_THROW(ret);
    }

    std::vector<yk_database_id_t> listDatabases(
            hg_addr_t address,
            uint16_t provider_id,
            const char* token) const {
        std::vector<yk_database_id_t> ids;
        size_t count;
        size_t max = 8;
        do {
            max *= 2;
            count = max;
            ids.resize(max);
            auto ret = yk_list_databases(
                m_admin, address, provider_id,
                token, ids.data(), &count);
            YOKAN_CONVERT_AND_THROW(ret);
            ids.resize(count);
        } while(count == max);
        return ids;
    }

    auto handle() const {
        return m_admin;
    }

    private:

    yk_admin_t m_admin = YOKAN_ADMIN_NULL;
};

}

#endif
