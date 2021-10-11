/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_CLIENT_H
#define __YOKAN_CLIENT_H

#include <margo.h>
#include <yokan/common.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct yk_client* yk_client_t;
#define YOKAN_CLIENT_NULL ((yk_client_t)NULL)

/**
 * @brief Creates a YOKAN client.
 *
 * @param[in] mid Margo instance
 * @param[out] client YOKAN client
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_client_init(margo_instance_id mid, yk_client_t* client);

/**
 * @brief Finalizes a YOKAN client.
 *
 * @param[in] client YOKAN client to finalize
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_client_finalize(yk_client_t client);

#ifdef __cplusplus
}
#endif

#endif
