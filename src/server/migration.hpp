/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MIGRATION_H
#define __MIGRATION_H
#ifdef YOKAN_HAS_REMI
#include "provider.hpp"
#include <remi/remi-common.h>
#include <remi/remi-client.h>
#include <remi/remi-server.h>

static inline int32_t before_migration_cb(remi_fileset_t fileset, void* uargs)
{
    (void)fileset;
    yk_provider_t provider = (yk_provider_t)uargs;
    // TODO
    std::cout << "Before Migration callback called with fileset:\n";
    char root[1024];
    size_t root_size = 1024;
    remi_fileset_get_root(fileset, root, &root_size);
    std::cout << "Root is " << root << std::endl;
    remi_fileset_walkthrough(fileset, [](const char* name, void*) {
        std::cout << "- " << name << std::endl;
    }, nullptr);
    return 0;
}

static inline int32_t after_migration_cb(remi_fileset_t fileset, void* uargs)
{
    (void)fileset;
    yk_provider_t provider = (yk_provider_t)uargs;
    std::cout << "After migration callback" << std::endl;
    return 0;
}

#endif
#endif
