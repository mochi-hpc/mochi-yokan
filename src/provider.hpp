/*
 * (C) 2020 The University of Chicago
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
    char*              token;               // Security token
    /* Databases */
    std::unordered_map<rkv_database_id_t, rkv_database_t> dbs;  // Databases
    // Note: in the above map, the std::string keys are actually uuids (32 bytes)

    /* RPC identifiers for admins */
    hg_id_t open_database_id;
    hg_id_t close_database_id;
    hg_id_t destroy_database_id;
    hg_id_t list_databases_id;
    /* RPC identifiers for clients */
    /* exists */
    hg_id_t exists_id;
    hg_id_t exists_multi_id;
    hg_id_t exists_packed_id;
    /* length */
    hg_id_t length_id;
    hg_id_t length_multi_id;
    hg_id_t length_packed_id;
    /* put */
    hg_id_t put_id;
    hg_id_t put_multi_id;
    hg_id_t put_packed_id;
    /* get */
    hg_id_t get_id;
    hg_id_t get_multi_id;
    hg_id_t get_packed_id;
    /* erase */
    hg_id_t erase_id;
    hg_id_t erase_multi_id;
    hg_id_t erase_packed_id;
    /* list keys */
    hg_id_t list_keys_id;
    hg_id_t list_keys_packed_id;
    /* list key/vals */
    hg_id_t list_keyvals_id;
    hg_id_t list_keyvals_packed_id;
} rkv_provider;

#endif
