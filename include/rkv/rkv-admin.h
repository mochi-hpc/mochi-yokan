/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_ADMIN_H
#define __RKV_ADMIN_H

#include <margo.h>
#include <rkv/rkv-common.h>

#if defined(__cplusplus)
extern "C" {
#endif

typedef struct rkv_admin* rkv_admin_t;
#define RKV_ADMIN_NULL ((rkv_admin_t)NULL)

#define RKV_DATABASE_ID_IGNORE ((rkv_database_id_t*)NULL)

/**
 * @brief Creates a RKV admin.
 *
 * @param[in] mid Margo instance
 * @param[out] admin RKV admin
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_admin_init(margo_instance_id mid, rkv_admin_t* admin);

/**
 * @brief Finalizes a RKV admin.
 *
 * @param[in] admin RKV admin to finalize
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_admin_finalize(rkv_admin_t admin);

/**
 * @brief Requests the provider to open a database of the
 * specified type and configuration and return a database id.
 *
 * @param[in] admin RKV admin object.
 * @param[in] address address of the provider.
 * @param[in] provider_id provider id.
 * @param[in] token security token.
 * @param[in] type type of database to open.
 * @param[in] config Configuration.
 * @param[out] id resulting database id.
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_open_database(
        rkv_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        const char* type,
        const char* config,
        rkv_database_id_t* id);

/**
 * @brief Requests the provider to close a database it is managing.
 *
 * @param[in] admin RKV admin object.
 * @param[in] address address of the provider.
 * @param[in] provider_id provider id.
 * @param[in] token security token.
 * @param[in] id resulting database id.
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_close_database(
        rkv_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        rkv_database_id_t id);

/**
 * @brief Requests the provider to destroy a database it is managing.
 *
 * @param[in] admin RKV admin object.
 * @param[in] address address of the provider.
 * @param[in] provider_id provider id.
 * @param[in] token security token.
 * @param[in] id resulting database id.
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_destroy_database(
        rkv_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        rkv_database_id_t id);

/**
 * @brief Lists the ids of databases available on the provider.
 *
 * @param[in] admin RKV admin object.
 * @param[in] address address of the provider.
 * @param[in] provider_id provider id.
 * @param[in] token security token.
 * @param[out] ids array of database ids.
 * @param[inout] count size of the array (in), number of ids returned (out).
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_list_databases(
        rkv_admin_t admin,
        hg_addr_t address,
        uint16_t provider_id,
        const char* token,
        rkv_database_id_t* ids,
        size_t* count);

#endif
