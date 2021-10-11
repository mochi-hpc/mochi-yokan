/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_ADMIN_H
#define __YOKAN_ADMIN_H

#include <margo.h>
#include <yokan/common.h>

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct yk_admin* yk_admin_t;
#define YOKAN_ADMIN_NULL ((yk_admin_t)NULL)

#define YOKAN_DATABASE_ID_IGNORE ((yk_database_id_t*)NULL)

/**
 * @brief Creates a YOKAN admin.
 *
 * @param[in] mid Margo instance
 * @param[out] admin YOKAN admin
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_admin_init(margo_instance_id mid, yk_admin_t* admin);

/**
 * @brief Finalizes a YOKAN admin.
 *
 * @param[in] admin YOKAN admin to finalize
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_admin_finalize(yk_admin_t admin);

/**
 * @brief Requests the provider to open a database of the
 * specified type and configuration and return a database id.
 *
 * @param[in] admin YOKAN admin object.
 * @param[in] address address of the provider.
 * @param[in] provider_id provider id.
 * @param[in] token security token.
 * @param[in] type type of database to open.
 * @param[in] config Configuration.
 * @param[out] id resulting database id.
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_open_database(
        yk_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        const char* type,
        const char* config,
        yk_database_id_t* id);

/**
 * @brief Requests the provider to close a database it is managing.
 *
 * @param[in] admin YOKAN admin object.
 * @param[in] address address of the provider.
 * @param[in] provider_id provider id.
 * @param[in] token security token.
 * @param[in] id resulting database id.
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_close_database(
        yk_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        yk_database_id_t id);

/**
 * @brief Requests the provider to destroy a database it is managing.
 *
 * @param[in] admin YOKAN admin object.
 * @param[in] address address of the provider.
 * @param[in] provider_id provider id.
 * @param[in] token security token.
 * @param[in] id resulting database id.
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_destroy_database(
        yk_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        yk_database_id_t id);

/**
 * @brief Lists the ids of databases available on the provider.
 *
 * @param[in] admin YOKAN admin object.
 * @param[in] address address of the provider.
 * @param[in] provider_id provider id.
 * @param[in] token security token.
 * @param[out] ids array of database ids.
 * @param[inout] count size of the array (in), number of ids returned (out).
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_list_databases(
        yk_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        yk_database_id_t* ids,
        size_t* count);

#if defined(__cplusplus)
}
#endif

#endif
