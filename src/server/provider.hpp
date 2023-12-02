/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PROVIDER_H
#define __PROVIDER_H

#include "config.h"
#include "yokan/server.h"
#include "yokan/backend.hpp"
#include "yokan/bulk-cache.h"
#include <nlohmann/json.hpp>
#include <margo.h>
#include <unordered_map>
#include <string>
#include <cstring>

using json = nlohmann::json;

typedef struct yk_provider {
    /* Margo/Argobots/Mercury environment */
    margo_instance_id  mid;                 // Margo instance
    uint16_t           provider_id;         // Provider id
    ABT_pool           pool;                // Pool on which to post RPC requests
    json               config;              // JSON configuration
    yk_bulk_cache      bulk_cache;          // Bulk cache functions
    void*              bulk_cache_data;     // Bulk cache data

    /* Database */
    yk_database_t db;

    /* RPC identifiers for clients */
    hg_id_t count_id;
    hg_id_t exists_id;
    hg_id_t exists_direct_id;
    hg_id_t length_id;
    hg_id_t length_direct_id;
    hg_id_t put_id;
    hg_id_t put_direct_id;
    hg_id_t get_id;
    hg_id_t get_direct_id;
    hg_id_t fetch_id;
    hg_id_t fetch_direct_id;
    hg_id_t fetch_back_id;
    hg_id_t fetch_direct_back_id;
    hg_id_t erase_id;
    hg_id_t erase_direct_id;
    hg_id_t list_keys_id;
    hg_id_t list_keys_direct_id;
    hg_id_t list_keyvals_id;
    hg_id_t list_keyvals_direct_id;
    hg_id_t iter_id;
    hg_id_t iter_direct_id;
    hg_id_t iter_back_id;
    hg_id_t iter_direct_back_id;
    hg_id_t coll_create_id;
    hg_id_t coll_drop_id;
    hg_id_t coll_exists_id;
    hg_id_t coll_last_id_id;
    hg_id_t coll_size_id;
    hg_id_t doc_erase_id;
    hg_id_t doc_load_id;
    hg_id_t doc_load_direct_id;
    hg_id_t doc_fetch_id;
    hg_id_t doc_fetch_back_id;
    hg_id_t doc_fetch_direct_back_id;
    hg_id_t doc_store_id;
    hg_id_t doc_store_direct_id;
    hg_id_t doc_update_id;
    hg_id_t doc_update_direct_id;
    hg_id_t doc_length_id;
    hg_id_t doc_list_id;
    hg_id_t doc_list_direct_id;
    hg_id_t doc_iter_id;
    hg_id_t doc_iter_direct_id;
    hg_id_t doc_iter_back_id;
    hg_id_t doc_iter_direct_back_id;
    hg_id_t get_remi_provider_id;

    // REMI information
    struct {
        remi_provider_t provider;
        remi_client_t   client;
    } remi;

} yk_provider;

/* Client RPCs */
DECLARE_MARGO_RPC_HANDLER(yk_count_ult)
void yk_count_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_put_ult)
void yk_put_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_put_direct_ult)
void yk_put_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_erase_ult)
void yk_erase_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_erase_direct_ult)
void yk_erase_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_get_ult)
void yk_get_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_get_direct_ult)
void yk_get_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_fetch_ult)
void yk_fetch_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_fetch_direct_ult)
void yk_fetch_direct_ult(hg_handle_t h);
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
DECLARE_MARGO_RPC_HANDLER(yk_list_keys_direct_ult)
void yk_list_keys_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_list_keyvals_ult)
void yk_list_keyvals_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_list_keyvals_direct_ult)
void yk_list_keyvals_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_iter_ult)
void yk_iter_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_iter_direct_ult)
void yk_iter_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_iter_ult)

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
DECLARE_MARGO_RPC_HANDLER(yk_doc_load_direct_ult)
void yk_doc_load_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_fetch_ult)
void yk_doc_fetch_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_store_ult)
void yk_doc_store_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_store_direct_ult)
void yk_doc_store_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_update_ult)
void yk_doc_update_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_update_direct_ult)
void yk_doc_update_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_length_ult)
void yk_doc_length_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_list_ult)
void yk_doc_list_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_list_direct_ult)
void yk_doc_list_direct_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_iter_ult)
void yk_doc_iter_ult(hg_handle_t h);
DECLARE_MARGO_RPC_HANDLER(yk_doc_iter_direct_ult)
void yk_doc_iter_direct_ult(hg_handle_t h);

DECLARE_MARGO_RPC_HANDLER(yk_get_remi_provider_id_ult)
void yk_get_remi_provider_id_ult(hg_handle_t h);
#endif
