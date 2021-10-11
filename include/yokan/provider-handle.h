/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_PROVIDER_HANDLE_H
#define __YOKAN_PROVIDER_HANDLE_H

#include <margo.h>
#include <yokan/common.h>

#ifdef __cplusplus
extern "C" {
#endif

struct yk_provider_handle {
    margo_instance_id mid;
    hg_addr_t         addr;
    uint16_t          provider_id;
};

typedef struct yk_provider_handle* yk_provider_handle_t;
#define YOKAN_PROVIDER_HANDLE_NULL ((yk_provider_handle_t)NULL)

#ifdef __cplusplus
}
#endif

#endif
