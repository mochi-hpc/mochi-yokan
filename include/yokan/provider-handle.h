/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_PROVIDER_HANDLE_H
#define __RKV_PROVIDER_HANDLE_H

#include <margo.h>
#include <yokan/common.h>

#ifdef __cplusplus
extern "C" {
#endif

struct rkv_provider_handle {
    margo_instance_id mid;
    hg_addr_t         addr;
    uint16_t          provider_id;
};

typedef struct rkv_provider_handle* rkv_provider_handle_t;
#define RKV_PROVIDER_HANDLE_NULL ((rkv_provider_handle_t)NULL)

#ifdef __cplusplus
}
#endif

#endif
