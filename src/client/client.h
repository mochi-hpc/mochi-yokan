/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef _CLIENT_H
#define _CLIENT_H

#include "rkv/rkv-client.h"
#include "rkv/rkv-database.h"

typedef struct rkv_client {
   margo_instance_id mid;
   /* exists */
   hg_id_t           exists_id;
   hg_id_t           exists_multi_id;
   hg_id_t           exists_packed_id;
   /* length */
   hg_id_t           length_id;
   hg_id_t           length_multi_id;
   hg_id_t           length_packed_id;
   /* put */
   hg_id_t           put_id;
   hg_id_t           put_multi_id;
   hg_id_t           put_packed_id;
   /* get */
   hg_id_t           get_id;
   hg_id_t           get_multi_id;
   hg_id_t           get_packed_id;
   /* erase */
   hg_id_t           erase_id;
   hg_id_t           erase_multi_id;
   hg_id_t           erase_packed_id;
   /* list keys */
   hg_id_t           list_keys_id;
   hg_id_t           list_keys_packed_id;
   /* list key/vals */
   hg_id_t           list_keyvals_id;
   hg_id_t           list_keyvals_packed_id;

   uint64_t          num_database_handles;
} rkv_client;

typedef struct rkv_database_handle {
    rkv_client_t      client;
    hg_addr_t         addr;
    uint16_t          provider_id;
    uint64_t          refcount;
    rkv_database_id_t database_id;
} rkv_database_handle;

#endif
