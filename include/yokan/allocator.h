/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_ALLOCATOR_H
#define __YOKAN_ALLOCATOR_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Memory allocator definition.
 * Custom allocators may be used for certain backends such as map.
 */
typedef struct yk_allocator {
    void* context;
    void* (*allocate)(void* context, size_t item_size, size_t count);
    void (*deallocate)(void* context, void* address, size_t item_size, size_t count);
    void (*finalize)(void* context);
} yk_allocator_t;

/**
 * @brief Type of functions used to initialize an allocator object.
 */
typedef void (*yk_allocator_init_fn)(yk_allocator_t*, const char*);

#ifdef __cplusplus
}
#endif

#endif
