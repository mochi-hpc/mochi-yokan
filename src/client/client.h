/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef _CLIENT_H
#define _CLIENT_H

#include "yokan/client.h"
#include "yokan/database.h"

typedef struct yk_client {
   margo_instance_id mid;

   hg_id_t           count_id;
   hg_id_t           exists_id;
   hg_id_t           length_id;
   hg_id_t           put_id;
   hg_id_t           get_id;
   hg_id_t           erase_id;
   hg_id_t           list_keys_id;
   hg_id_t           list_keyvals_id;

   uint64_t          num_database_handles;
} yk_client;

typedef struct yk_database_handle {
    yk_client_t      client;
    hg_addr_t         addr;
    uint16_t          provider_id;
    uint64_t          refcount;
    yk_database_id_t database_id;
} yk_database_handle;

#endif
