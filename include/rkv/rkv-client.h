/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_CLIENT_H
#define __RKV_CLIENT_H

#include <margo.h>
#include <rkv/rkv-common.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct rkv_client* rkv_client_t;
#define RKV_CLIENT_NULL ((rkv_client_t)NULL)

/**
 * @brief Creates a RKV client.
 *
 * @param[in] mid Margo instance
 * @param[out] client RKV client
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_client_init(margo_instance_id mid, rkv_client_t* client);

/**
 * @brief Finalizes a RKV client.
 *
 * @param[in] client RKV client to finalize
 *
 * @return RKV_SUCCESS or error code defined in rkv-common.h
 */
rkv_return_t rkv_client_finalize(rkv_client_t client);

#ifdef __cplusplus
}
#endif

#endif
