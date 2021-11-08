/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_COMMON_H
#define __YOKAN_COMMON_H

#include <uuid.h>
#include <stdint.h>
#include <limits.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Error codes that can be returned by YOKAN functions.
 */
#define YOKAN_RETURN_VALUES                              \
    X(YOKAN_SUCCESS, "Success")                          \
    X(YOKAN_ERR_ALLOCATION, "Allocation error")          \
    X(YOKAN_ERR_INVALID_MID, "Invalid margo instance")   \
    X(YOKAN_ERR_INVALID_ARGS, "Invalid argument")        \
    X(YOKAN_ERR_INVALID_PROVIDER, "Invalid provider id") \
    X(YOKAN_ERR_INVALID_DATABASE, "Invalid database id") \
    X(YOKAN_ERR_INVALID_BACKEND, "Invalid backend type") \
    X(YOKAN_ERR_INVALID_CONFIG, "Invalid configuration") \
    X(YOKAN_ERR_INVALID_TOKEN, "Invalid token")          \
    X(YOKAN_ERR_INVALID_ID, "Invalid document id")       \
    X(YOKAN_ERR_FROM_MERCURY, "Mercury error")           \
    X(YOKAN_ERR_FROM_ARGOBOTS, "Argobots error")         \
    X(YOKAN_ERR_OP_UNSUPPORTED, "Unsupported operation") \
    X(YOKAN_ERR_OP_FORBIDDEN, "Forbidden operation")     \
    X(YOKAN_ERR_KEY_NOT_FOUND, "Key not found")          \
    X(YOKAN_ERR_BUFFER_SIZE, "Buffer too small")         \
    X(YOKAN_ERR_KEY_EXISTS, "Key exists")                \
    X(YOKAN_ERR_CORRUPTION, "Data corruption")           \
    X(YOKAN_ERR_IO, "IO error")                          \
    X(YOKAN_ERR_INCOMPLETE, "Imcomplete operation")      \
    X(YOKAN_ERR_TIMEOUT, "Timeout")                      \
    X(YOKAN_ERR_ABORTED, "Operation aborted")            \
    X(YOKAN_ERR_BUSY, "Busy")                            \
    X(YOKAN_ERR_EXPIRED, "Operation expired")            \
    X(YOKAN_ERR_TRY_AGAIN, "Try again")                  \
    X(YOKAN_ERR_SYSTEM, "System error")                  \
    X(YOKAN_ERR_CANCELED, "Canceled")                    \
    X(YOKAN_ERR_PERMISSION, "Permission error")          \
    X(YOKAN_ERR_MODE, "Invalid mode")                    \
    X(YOKAN_ERR_NONCONTIG, "Non-contiguous buffer")      \
    X(YOKAN_ERR_READONLY, "Read-only buffer")            \
    X(YOKAN_ERR_OTHER, "Other error")

#define X(__err__, __msg__) __err__,
typedef enum yk_return_t {
    YOKAN_RETURN_VALUES
} yk_return_t;
#undef X

/**
 * @brief These two constants are used in returned value sizes
 * to indicate respectively that the buffer was too small to hold
 * the value, and that the key was not found.
 */
#define YOKAN_KEY_NOT_FOUND  (ULLONG_MAX)
#define YOKAN_SIZE_TOO_SMALL (ULLONG_MAX-1)
#define YOKAN_NO_MORE_KEYS   (ULLONG_MAX-2)
#define YOKAN_NO_MORE_DOCS   (ULLONG_MAX-2) /* same as YOKAN_NO_MORE_KEYS */

/**
 * @brief Modes can be passed to many functions to alter the
 * semantics of the function.
 * - YOKAN_MODE_PACKED: indicate that the data is packed in memory.
 * - YOKAN_MODE_INCLUSIVE: "start" key in "list_keys"/"list_keyvals"
 *   is included in results if it is found.
 * - YOKAN_MODE_APPEND: "put" functions will append the provided
 *   data to any existing value instead of replacing it.
 * - YOKAN_MODE_CONSUME: "get" and "list" functions will also remove
 *   the returned key/value pairs from the database.
 * - YOKAN_MODE_WAIT: "get" will wait for any non-present key to
 *   appear in the database instead of returning YOKAN_KEY_NODE_FOUND.
 *   Writers need to put their key with YOKAN_MODE_NOTIFY in order
 *   to wake up waiters.
 * - YOKAN_MODE_NEW_ONLY: "put" will only add key/value pairs if the
 *   key was not already present in the database.
 * - YOKAN_MODE_NO_PREFIX: "list_keys" and "list_keyvals" will remove
 *   the prefix from results before sending the keys back.
 * - YOKAN_MODE_IGNORE_KEY: "list_keyvals" will only return values.
 * - YOKAN_MODE_KEEP_LAST: implies YOKAN_MODE_IGNORE_KEYS but "list_keyvals"
 *   will still return the last key found. The rest of the keys will
 *   be set as empty.
 * - YOKAN_MODE_SUFFIX: consider the "filter" argument of "list_keys"
 *   and "list_keyvals" as a suffix instead of a prefix.
 *   YOKAN_MODE_NO_PREFIX, if provided, will be re-interpreted
 *   accordingly, removing the suffix from the resulting keys.
 * - YOKAN_MODE_LUA_FILTER: interpret the filter as Lua code.
 * - YOKAN_MODE_IGNORE_DOCS: only return IDs of documents matching
 *   a filter.
 *
 * Important: not all backends support all modes.
 */
#define YOKAN_MODE_DEFAULT      0b000000000000
#define YOKAN_MODE_INCLUSIVE    0b000000000001
#define YOKAN_MODE_APPEND       0b000000000010
#define YOKAN_MODE_CONSUME      0b000000000100
#define YOKAN_MODE_WAIT         0b000000001000
#define YOKAN_MODE_NOTIFY       0b000000001000
#define YOKAN_MODE_NEW_ONLY     0b000000010000
#define YOKAN_MODE_EXIST_ONLY   0b000000100000
#define YOKAN_MODE_NO_PREFIX    0b000001000000
#define YOKAN_MODE_IGNORE_KEYS  0b000010000000
#define YOKAN_MODE_KEEP_LAST    0b000110000000
#define YOKAN_MODE_SUFFIX       0b001000000000
#define YOKAN_MODE_LUA_FILTER   0b010000000000
#define YOKAN_MORE_IGNORE_DOCS  0b100000000000

/**
 * @brief Identifier for a database.
 */
typedef struct yk_database_id_t {
    uuid_t uuid;
} yk_database_id_t;

/**
 * @brief Converts a yk_database_id_t into a string.
 *
 * @param id Id to convert
 * @param out[37] Resulting null-terminated string
 */
static inline void yk_database_id_to_string(
        yk_database_id_t id,
        char out[37]) {
    uuid_unparse(id.uuid, out);
}

/**
 * @brief Converts a string into a yk_database_id_t. The string
 * should be a 36-characters string + null terminator.
 *
 * @param in input string
 * @param id resulting id
 */
static inline void yk_database_id_from_string(
        const char* in,
        yk_database_id_t* id) {
    uuid_parse(in, id->uuid);
}

/**
 * @brief Record when working with collections.
 */
typedef uint64_t yk_id_t;

#ifdef __cplusplus
}
#endif

#endif
