/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_PROVIDER_HANDLE_H
#define __YOKAN_PROVIDER_HANDLE_H

#include <margo.h>
#include <yokan/common.h>
#include <yokan/client.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Yokan Provider handle.
 * These are not used directly in Yokan's client or admin API
 * but can be supplied by Bedrock if another Mochi microservice
 * needs one.
 */
struct yk_provider_handle {
    margo_instance_id mid;
    yk_client_t       client;
    hg_addr_t         addr;
    uint16_t          provider_id;
};

typedef struct yk_provider_handle* yk_provider_handle_t;
#define YOKAN_PROVIDER_HANDLE_NULL ((yk_provider_handle_t)NULL)

#ifdef __cplusplus
}
#endif

#endif
