/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef _CLIENT_H
#define _CLIENT_H

#include "types.h"
#include "rkv/rkv-client.h"
#include "rkv/rkv-database.h"

typedef struct rkv_client {
   margo_instance_id mid;
   hg_id_t           hello_id;
   hg_id_t           sum_id;
   uint64_t          num_database_handles;
} rkv_client;

typedef struct rkv_database_handle {
    rkv_client_t      client;
    hg_addr_t           addr;
    uint16_t            provider_id;
    uint64_t            refcount;
    rkv_database_id_t database_id;
} rkv_database_handle;

#endif
