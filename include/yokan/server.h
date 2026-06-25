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
#include <stdbool.h>

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
    ABT_pool        pool;   // Pool used to run RPCs
    yk_bulk_cache_t cache;  // cache implementation for bulk handles
    struct {
        remi_client_t   client;
        remi_provider_t provider;
    } remi; // REMI information (yokan needs to be built with ENABLE_REMI)
};

#define YOKAN_PROVIDER_ARGS_INIT { ABT_POOL_NULL, NULL, {NULL, NULL} }

/**
 * @brief Creates a new YOKAN provider. If YOKAN_PROVIDER_IGNORE
 * is passed as last argument, the provider will be automatically
 * destroyed when calling margo_finalize.
 *
 * @param[in] mid Margo instance
 * @param[in] provider_id provider id
 * @param[in] config Configuration
 * @param[in] args argument structure
 * @param[out] provider provider
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_provider_register(
        margo_instance_id mid,
        uint16_t provider_id,
        const char* config,
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
 * @brief Migrates the database from the given provider to
 * the target provider.
 *
 * @param provider
 * @param dest_addr
 * @param dest_provider_id
 * @param options
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_provider_migrate_database(
        yk_provider_t provider,
        const char* dest_addr,
        uint16_t dest_provider_id,
        const struct yk_migration_options* options);

/**
 * @brief Snapshots the database currently attached to the provider into a
 * directory accessible via the local filesystem (typically a parallel
 * filesystem mount point). The directory is created if it does not exist.
 *
 * The snapshot contains the database's backing files plus a small manifest
 * (yokan-snapshot.json) that records the backend type, original db_config,
 * and file list. The manifest is what yk_provider_restore_database reads
 * back.
 *
 * Unlike yk_provider_migrate_database, this does not require REMI and works
 * regardless of how Yokan was built.
 *
 * @param provider       YOKAN provider whose database to snapshot.
 * @param dest_path      Local directory path where the snapshot will be
 *                       written. Created if missing.
 * @param remove_source  If true, the local database is destroyed after a
 *                       successful snapshot (analogous to migrate). If
 *                       false, the database remains live and serving.
 * @param options        Optional snapshot options (may be NULL).
 *
 * @return YOKAN_SUCCESS or an error code defined in common.h.
 */
yk_return_t yk_provider_snapshot_database(
        yk_provider_t provider,
        const char* dest_path,
        bool remove_source,
        const struct yk_snapshot_options* options);

/**
 * @brief Restores a database from a snapshot directory produced by
 * yk_provider_snapshot_database, attaching it to the given provider.
 *
 * If the provider already has a database attached, it is destroyed first.
 * If options->new_root is non-NULL, the snapshot's backing files are first
 * copied from src_path to that new root (typical HPC pattern: snapshot lives
 * on PFS, restore back to local SSD). Otherwise the database is opened
 * in-place against src_path.
 *
 * @param provider  YOKAN provider to attach the restored database to.
 * @param src_path  Local directory path containing a Yokan snapshot.
 * @param options   Optional restore options (may be NULL).
 *
 * @return YOKAN_SUCCESS or an error code defined in common.h.
 */
yk_return_t yk_provider_restore_database(
        yk_provider_t provider,
        const char* src_path,
        const struct yk_restore_options* options);

/**
 * @brief Returns the internal configuration of the YOKAN
 * provider. The returned string must be free-ed by the caller.
 */
char* yk_provider_get_config(yk_provider_t provider);

#ifdef __cplusplus
}
#endif

#endif
