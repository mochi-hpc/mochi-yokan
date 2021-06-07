/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_COMMON_H
#define __RKV_COMMON_H

#include <uuid.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Error codes that can be returned by RKV functions.
 */
typedef enum rkv_return_t {
    RKV_SUCCESS,
    RKV_ERR_ALLOCATION,        /* Allocation error */
    RKV_ERR_INVALID_MID,       /* Invalid margo instance */
    RKV_ERR_INVALID_ARGS,      /* Invalid argument */
    RKV_ERR_INVALID_PROVIDER,  /* Invalid provider id */
    RKV_ERR_INVALID_DATABASE,  /* Invalid database id */
    RKV_ERR_INVALID_BACKEND,   /* Invalid backend type */
    RKV_ERR_INVALID_CONFIG,    /* Invalid configuration */
    RKV_ERR_INVALID_TOKEN,     /* Invalid token */
    RKV_ERR_FROM_MERCURY,      /* Mercurt error */
    RKV_ERR_FROM_ARGOBOTS,     /* Argobots error */
    RKV_ERR_OP_UNSUPPORTED,    /* Unsupported operation */
    RKV_ERR_OP_FORBIDDEN,      /* Forbidden operation */
    /* ... TODO add more error codes here if needed */
    RKV_ERR_OTHER              /* Other error */
} rkv_return_t;

/**
 * @brief Identifier for a database.
 */
typedef struct rkv_database_id_t {
    uuid_t uuid;
} rkv_database_id_t;

/**
 * @brief Converts a rkv_database_id_t into a string.
 *
 * @param id Id to convert
 * @param out[37] Resulting null-terminated string
 */
static inline void rkv_database_id_to_string(
        rkv_database_id_t id,
        char out[37]) {
    uuid_unparse(id.uuid, out);
}

/**
 * @brief Converts a string into a rkv_database_id_t. The string
 * should be a 36-characters string + null terminator.
 *
 * @param in input string
 * @param id resulting id
 */
static inline void rkv_database_id_from_string(
        const char* in,
        rkv_database_id_t* id) {
    uuid_parse(in, id->uuid);
}

#ifdef __cplusplus
}
#endif

#endif
