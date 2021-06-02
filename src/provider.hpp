/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __PROVIDER_H
#define __PROVIDER_H

#include <margo.h>
#include <abt-io.h>
#include <uuid.h>
#include "rkv/rkv-backend.hpp"

typedef struct rkv_provider {
    /* Margo/Argobots/Mercury environment */
    margo_instance_id  mid;                 // Margo instance
    uint16_t           provider_id;         // Provider id
    ABT_pool           pool;                // Pool on which to post RPC requests
    abt_io_instance_id abtio;               // ABT-IO instance
    char*              token;               // Security token
    /* Resources and backend types */
    // TODO
    /* RPC identifiers for admins */
    hg_id_t create_database_id;
    hg_id_t open_database_id;
    hg_id_t close_database_id;
    hg_id_t destroy_database_id;
    hg_id_t list_databases_id;
    /* RPC identifiers for clients */
    hg_id_t hello_id;
    hg_id_t sum_id;
    /* ... add other RPC identifiers here ... */
} rkv_provider;

#endif
