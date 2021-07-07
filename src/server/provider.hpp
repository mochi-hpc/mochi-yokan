/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PROVIDER_H
#define __PROVIDER_H

#include "rkv/rkv-backend.hpp"
#include <margo.h>
#include <uuid.h>
#include <unordered_map>
#include <string>
#include <cstring>

namespace std {
    template <> struct hash<rkv_database_id_t> {
        size_t operator()(const rkv_database_id_t& id) const {
            // since a UUID is already pretty random,
            // this hash just takes the first 8 bytes
            size_t h;
            std::memcpy(&h, &id.uuid, sizeof(h));
            return h;
        }
    };
}

inline bool operator==(const rkv_database_id_t& lhs, const rkv_database_id_t& rhs) {
    return std::memcmp(&lhs.uuid, &rhs.uuid, sizeof(lhs.uuid)) == 0;
}

inline bool operator!=(const rkv_database_id_t& lhs, const rkv_database_id_t& rhs) {
    return std::memcmp(&lhs.uuid, &rhs.uuid, sizeof(lhs.uuid)) != 0;
}

typedef struct rkv_provider {
    /* Margo/Argobots/Mercury environment */
    margo_instance_id  mid;                 // Margo instance
    uint16_t           provider_id;         // Provider id
    ABT_pool           pool;                // Pool on which to post RPC requests
    std::string        token;               // Security token
    rkv_bulk_cache     bulk_cache;          // Bulk cache functions
    void*              bulk_cache_data;     // Bulk cache data

    /* Databases */
    std::unordered_map<rkv_database_id_t, rkv_database_t> dbs;  // Databases
    // Note: in the above map, the std::string keys are actually uuids (32 bytes)

    /* RPC identifiers for admins */
    hg_id_t open_database_id;
    hg_id_t close_database_id;
    hg_id_t destroy_database_id;
    hg_id_t list_databases_id;
    /* RPC identifiers for clients */
    hg_id_t exists_id;
    hg_id_t length_id;
    hg_id_t put_id;
    hg_id_t get_id;
    hg_id_t erase_id;
    hg_id_t list_keys_id;
    hg_id_t list_keyvals_id;
} rkv_provider;

/* Admin RPCs */
DECLARE_MARGO_RPC_HANDLER(rkv_create_database_ult)
void rkv_create_database_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(rkv_open_database_ult)
void rkv_open_database_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(rkv_close_database_ult)
void rkv_close_database_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(rkv_destroy_database_ult)
void rkv_destroy_database_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(rkv_list_databases_ult)
void rkv_list_databases_ult(hg_handle_t h);

/* Client RPCs */
DECLARE_MARGO_RPC_HANDLER(rkv_put_ult)
void rkv_put_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(rkv_erase_ult)
void rkv_erase_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(rkv_get_ult)
void rkv_get_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(rkv_length_ult)
void rkv_length_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(rkv_exists_ult)
void rkv_exists_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(rkv_list_keys_ult)
void rkv_list_keys_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(rkv_list_keyvals_ult)
void rkv_list_keyvals_ult(hg_handle_t h);

static inline rkv_database_t find_database(rkv_provider_t provider,
                                           rkv_database_id_t* db_id)
{
    auto it = provider->dbs.find(*db_id);
    if(it == provider->dbs.end()) return nullptr;
    else return it->second;
}

#endif
