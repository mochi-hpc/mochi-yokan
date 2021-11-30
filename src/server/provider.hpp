/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PROVIDER_H
#define __PROVIDER_H

#include "yokan/backend.hpp"
#include <nlohmann/json.hpp>
#include <margo.h>
#include <uuid.h>
#include <unordered_map>
#include <string>
#include <cstring>

namespace std {
    template <> struct hash<yk_database_id_t> {
        size_t operator()(const yk_database_id_t& id) const {
            // since a UUID is already pretty random,
            // this hash just takes the first 8 bytes
            size_t h;
            std::memcpy(&h, &id.uuid, sizeof(h));
            return h;
        }
    };
}

inline bool operator==(const yk_database_id_t& lhs, const yk_database_id_t& rhs) {
    return std::memcmp(&lhs.uuid, &rhs.uuid, sizeof(lhs.uuid)) == 0;
}

inline bool operator!=(const yk_database_id_t& lhs, const yk_database_id_t& rhs) {
    return std::memcmp(&lhs.uuid, &rhs.uuid, sizeof(lhs.uuid)) != 0;
}

using json = nlohmann::json;

typedef struct yk_provider {
    /* Margo/Argobots/Mercury environment */
    margo_instance_id  mid;                 // Margo instance
    uint16_t           provider_id;         // Provider id
    ABT_pool           pool;                // Pool on which to post RPC requests
    json               config;              // JSON configuration
    std::string        token;               // Security token
    yk_bulk_cache      bulk_cache;          // Bulk cache functions
    void*              bulk_cache_data;     // Bulk cache data

    /* Databases */
    std::unordered_map<yk_database_id_t, yk_database_t> dbs;  // Databases
    // Note: in the above map, the std::string keys are actually uuids (32 bytes)

    /* RPC identifiers for admins */
    hg_id_t open_database_id;
    hg_id_t close_database_id;
    hg_id_t destroy_database_id;
    hg_id_t list_databases_id;
    /* RPC identifiers for clients */
    hg_id_t count_id;
    hg_id_t exists_id;
    hg_id_t exists_direct_id;
    hg_id_t length_id;
    hg_id_t length_direct_id;
    hg_id_t put_id;
    hg_id_t get_id;
    hg_id_t erase_id;
    hg_id_t list_keys_id;
    hg_id_t list_keyvals_id;
    hg_id_t coll_create_id;
    hg_id_t coll_drop_id;
    hg_id_t coll_exists_id;
    hg_id_t coll_last_id_id;
    hg_id_t coll_size_id;
    hg_id_t doc_erase_id;
    hg_id_t doc_load_id;
    hg_id_t doc_store_id;
    hg_id_t doc_store_direct_id;
    hg_id_t doc_update_id;
    hg_id_t doc_length_id;
    hg_id_t doc_list_id;
} yk_provider;

/* Admin RPCs */
DECLARE_MARGO_RPC_HANDLER(yk_create_database_ult)
void yk_create_database_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_open_database_ult)
void yk_open_database_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_close_database_ult)
void yk_close_database_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_destroy_database_ult)
void yk_destroy_database_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_list_databases_ult)
void yk_list_databases_ult(hg_handle_t h);

/* Client RPCs */
DECLARE_MARGO_RPC_HANDLER(yk_count_ult)
void yk_count_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_put_ult)
void yk_put_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_erase_ult)
void yk_erase_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_get_ult)
void yk_get_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_length_ult)
void yk_length_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_length_direct_ult)
void yk_length_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_exists_ult)
void yk_exists_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_exists_direct_ult)
void yk_exists_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_list_keys_ult)
void yk_list_keys_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_list_keyvals_ult)
void yk_list_keyvals_ult(hg_handle_t h);

DECLARE_MARGO_RPC_HANDLER(yk_coll_create_ult)
void yk_coll_create_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_coll_drop_ult)
void yk_coll_drop_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_coll_exists_ult)
void yk_coll_exists_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_coll_last_id_ult)
void yk_coll_last_id_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_coll_size_ult)
void yk_coll_size_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_erase_ult)
void yk_doc_erase_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_load_ult)
void yk_doc_load_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_store_ult)
void yk_doc_store_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_store_direct_ult)
void yk_doc_store_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_update_ult)
void yk_doc_update_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_length_ult)
void yk_doc_length_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_list_ult)
void yk_doc_list_ult(hg_handle_t h);

static inline yk_database_t find_database(yk_provider_t provider,
                                           yk_database_id_t* db_id)
{
    auto it = provider->dbs.find(*db_id);
    if(it == provider->dbs.end()) return nullptr;
    else return it->second;
}

#endif
