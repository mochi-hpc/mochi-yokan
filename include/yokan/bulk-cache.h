/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_BULK_CACHE_H
#define __RKV_BULK_CACHE_H

#include <margo.h>
#include <yokan/common.h>

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct rkv_buffer {
    size_t     size; /* size in bytes */
    hg_uint8_t mode; /* HG_BULK_READWRITE, HG_BULK_READ_ONLY, or HG_BULK_WRITE_ONLY */
    char*      data; /* local data */
    hg_bulk_t  bulk; /* local bulk handle for the data */
} *rkv_buffer_t;

typedef struct rkv_bulk_cache {
    /* initialize a bulk cache and return it as a void* pointer */
    void* (*init)(margo_instance_id mid, const char* config);

    /* finalize and destroy the bulk cache */
    void (*finalize)(void* cache);

    /* get or allocate a buffer. May return a buffer
     * with a larger size than requested. */
    rkv_buffer_t (*get)(void* cache, size_t size, hg_uint8_t mode);

    /* release a bulk entry when no longer needed. */
    void (*release)(void* cache, rkv_buffer_t entry);
} *rkv_bulk_cache_t;

#if defined(__cplusplus)
}
#endif

#endif
