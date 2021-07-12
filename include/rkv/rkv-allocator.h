/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_ALLOCATOR_H
#define __RKV_ALLOCATOR_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Memory allocator definition.
 * Custom allocators may be used for certain backends such as map.
 */
typedef struct rkv_allocator {
    void* context;
    void* (*allocate)(void* context, size_t item_size, size_t count);
    void (*deallocate)(void* context, void* address, size_t item_size, size_t count);
    void (*finalize)(void* context);
} rkv_allocator_t;

/**
 * @brief Type of functions used to initialize an allocator object.
 */
typedef void (*rkv_allocator_init_fn)(rkv_allocator_t*);

#ifdef __cplusplus
}
#endif

#endif
