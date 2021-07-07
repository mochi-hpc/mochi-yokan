/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_SERVER_H
#define __RKV_SERVER_H

#include <rkv/rkv-common.h>
#include <rkv/rkv-bulk-cache.h>
#include <margo.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RKV_ABT_POOL_DEFAULT ABT_POOL_NULL

typedef struct rkv_provider* rkv_provider_t;
#define RKV_PROVIDER_NULL ((rkv_provider_t)NULL)
#define RKV_PROVIDER_IGNORE ((rkv_provider_t*)NULL)

struct rkv_provider_args {
    const char*        token;  // Security token
    const char*        config; // JSON configuration
    ABT_pool           pool;   // Pool used to run RPCs
    rkv_bulk_cache_t   cache;  // cache implementation for bulk handles
};

#define RKV_PROVIDER_ARGS_INIT { NULL, NULL, ABT_POOL_NULL, NULL }

/**
 * @brief Creates a new RKV provider. If RKV_PROVIDER_IGNORE
 * is passed as last argument, the provider will be automatically
 * destroyed when calling margo_finalize.
 *
 * @param[in] mid Margo instance
 * @param[in] provider_id provider id
 * @param[in] args argument structure
 * @param[out] provider provider
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        const struct rkv_provider_args* args,
        rkv_provider_t* provider);

/**
 * @brief Destroys the Alpha provider and deregisters its RPC.
 *
 * @param[in] provider Alpha provider
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_provider_destroy(
        rkv_provider_t provider);

#ifdef __cplusplus
}
#endif

#endif
