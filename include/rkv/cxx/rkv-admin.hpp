/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_ADMIN_HPP
#define __RKV_ADMIN_HPP

#include <rkv/rkv-admin.h>
#include <rkv/cxx/rkv-exception.hpp>
#include <vector>

namespace rkv {

class Admin {

    public:

    Admin(margo_instance_id mid) {
        auto ret = rkv_admin_init(mid, &m_admin);
        RKV_CONVERT_AND_THROW(ret);
    }

    ~Admin() {
        if(m_admin != RKV_ADMIN_NULL) {
            rkv_admin_finalize(m_admin);
        }
    }

    Admin(Admin&& other)
    : m_admin(other.m_admin) {
        other.m_admin = RKV_ADMIN_NULL;
    }

    Admin(const Admin&) = delete;

    Admin& operator=(const Admin&) = delete;

    Admin& operator=(Admin&&) = delete;

    rkv_database_id_t openDatabase(
            hg_addr_t address,
            uint16_t provider_id,
            const char* token,
            const char* type,
            const char* config) const {
        rkv_database_id_t id;
        auto ret = rkv_open_database(
            m_admin, address, provider_id,
            token, type, config, &id);
        RKV_CONVERT_AND_THROW(ret);
        return id;
    }

    void closeDatabase(
            hg_addr_t address,
            uint16_t provider_id,
            const char* token,
            rkv_database_id_t id) const {
        auto ret = rkv_close_database(
            m_admin, address, provider_id,
            token, id);
        RKV_CONVERT_AND_THROW(ret);
    }

    void destroyDatabase(
            hg_addr_t address,
            uint16_t provider_id,
            const char* token,
            rkv_database_id_t id) const {
        auto ret = rkv_destroy_database(
            m_admin, address, provider_id,
            token, id);
        RKV_CONVERT_AND_THROW(ret);
    }

    std::vector<rkv_database_id_t> listDatabases(
            hg_addr_t address,
            uint16_t provider_id,
            const char* token) const {
        std::vector<rkv_database_id_t> ids;
        size_t count;
        size_t max = 8;
        do {
            max *= 2;
            count = max;
            ids.resize(max);
            auto ret = rkv_list_databases(
                m_admin, address, provider_id,
                token, ids.data(), &count);
            RKV_CONVERT_AND_THROW(ret);
            ids.resize(count);
        } while(count == max);
        return ids;
    }

    auto handle() const {
        return m_admin;
    }

    private:

    rkv_admin_t m_admin = RKV_ADMIN_NULL;
};

}

#endif
