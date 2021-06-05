/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_DATABASE_H
#define __RKV_DATABASE_H

#include <margo.h>
#include <rkv/rkv-common.h>
#include <rkv/rkv-client.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct rkv_database_handle *rkv_database_handle_t;
#define RKV_DATABASE_HANDLE_NULL ((rkv_database_handle_t)NULL)

/**
 * @brief Creates a RKV database handle.
 *
 * @param[in] client RKV client responsible for the database handle
 * @param[in] addr Mercury address of the provider
 * @param[in] provider_id id of the provider
 * @param[in] handle database handle
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_database_handle_create(
        rkv_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        rkv_database_id_t database_id,
        rkv_database_handle_t* handle);

/**
 * @brief Increments the reference counter of a database handle.
 *
 * @param handle database handle
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_database_handle_ref_incr(
        rkv_database_handle_t handle);

/**
 * @brief Releases the database handle. This will decrement the
 * reference counter, and free the database handle if the reference
 * counter reaches 0.
 *
 * @param[in] handle database handle to release.
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_database_handle_release(rkv_database_handle_t handle);

#if 0
/**
 * @brief Makes the target RKV database print Hello World.
 *
 * @param[in] handle database handle.
 * @param[in] x first number.
 * @param[in] y second number.
 * @param[out] result resulting value.
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_say_hello(rkv_database_handle_t handle);

/**
 * @brief Makes the target RKV database compute the sum of the
 * two numbers and return the result.
 *
 * @param[in] handle database handle.
 * @param[in] x first number.
 * @param[in] y second number.
 * @param[out] result resulting value.
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_compute_sum(
        rkv_database_handle_t handle,
        int32_t x,
        int32_t y,
        int32_t* result);
#endif

#ifdef __cplusplus
}
#endif

#endif
