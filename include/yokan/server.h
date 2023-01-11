/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_SERVER_H
#define __YOKAN_SERVER_H

#include <yokan/common.h>
#include <yokan/bulk-cache.h>
#include <margo.h>

#ifdef __cplusplus
extern "C" {
#endif

#define YOKAN_ABT_POOL_DEFAULT ABT_POOL_NULL

typedef struct yk_provider* yk_provider_t;
#define YOKAN_PROVIDER_NULL ((yk_provider_t)NULL)
#define YOKAN_PROVIDER_IGNORE ((yk_provider_t*)NULL)

typedef struct remi_client* remi_client_t; // define without including <remi-client.h>
typedef struct remi_provider* remi_provider_t; // define without including <remi-server.h>

struct yk_provider_args {
    const char*     token;  // Security token
    const char*     config; // JSON configuration
    ABT_pool        pool;   // Pool used to run RPCs
    yk_bulk_cache_t cache;  // cache implementation for bulk handles
    struct {
        remi_client_t   client;
        remi_provider_t provider;
    } remi; // REMI information (yokan needs to be built with ENABLE_REMI)
};

#define YOKAN_PROVIDER_ARGS_INIT { NULL, NULL, ABT_POOL_NULL, NULL, {NULL, NULL} }

/**
 * @brief Creates a new YOKAN provider. If YOKAN_PROVIDER_IGNORE
 * is passed as last argument, the provider will be automatically
 * destroyed when calling margo_finalize.
 *
 * @param[in] mid Margo instance
 * @param[in] provider_id provider id
 * @param[in] args argument structure
 * @param[out] provider provider
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        const struct yk_provider_args* args,
        yk_provider_t* provider);

/**
 * @brief Destroys the YOKAN provider and deregisters its RPC.
 *
 * @param[in] provider YOKAN provider
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_provider_destroy(
        yk_provider_t provider);

/**
 * @brief Returns the internal configuration of the YOKAN
 * provider. The returned string must be free-ed by the caller.
 */
char* yk_provider_get_config(yk_provider_t provider);

#ifdef __cplusplus
}
#endif

#endif
