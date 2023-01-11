/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __MIGRATION_H
#define __MIGRATION_H
#ifdef YOKAN_HAS_REMI
#include "provider.hpp"
#include <remi/remi-client.h>
#include <remi/remi-server.h>

static inline int32_t before_migration_cb(remi_fileset_t fileset, void* uargs)
{
    (void)fileset;
    yk_provider_t provider = (yk_provider_t)uargs;
    // TODO
    return 0;
}

static inline int32_t after_migration_cb(remi_fileset_t fileset, void* uargs)
{
    (void)fileset;
    yk_provider_t provider = (yk_provider_t)uargs;
    // TODO
    return 0;
}

#endif
#endif
