/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_DATABASE_H
#define __YOKAN_DATABASE_H

#include <stdbool.h>
#include <margo.h>
#include <yokan/common.h>
#include <yokan/client.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct yk_database_handle *yk_database_handle_t;
#define YOKAN_DATABASE_HANDLE_NULL ((yk_database_handle_t)NULL)

/**
 * @brief Type of callback used by the fetch and iter functions.
 *
 * @param void* User-provided arguments.
 * @param size_t Index of the key/value pair (if fetching multiple).
 * @param const void* Key data.
 * @param size_t Size of the key.
 * @param const void* Value data.
 * @param size_t Size of the data.
 *
 * @return YK_SUCCESS or other error code.
 */
typedef yk_return_t (*yk_keyvalue_callback_t)(void*, size_t, const void*, size_t, const void*, size_t);

/**
 * @brief If a database has a "name":"something" entry in its
 * configuration, this function will find the corresponding
 * database id.
 *
 * Note that if multiple databases have the same name, this
 * function will return the first one.
 *
 * @param[in] client Client
 * @param[in] addr Server address
 * @param[in] provider_id Provider id
 * @param[in] db_name Database name
 * @param[out] database_id Database id
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_database_find_by_name(
        yk_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        const char* db_name,
        yk_database_id_t* database_id);

/**
 * @brief Creates a YOKAN database handle.
 *
 * @param[in] client YOKAN client responsible for the database handle
 * @param[in] addr Mercury address of the provider
 * @param[in] provider_id id of the provider
 * @param[in] handle database handle
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_database_handle_create(
        yk_client_t client,
        hg_addr_t addr,
        uint16_t provider_id,
        yk_database_id_t database_id,
        yk_database_handle_t* handle);

/**
 * @brief Retrieve the internal information from a database handle.
 * Any NULL pointer will be ignored. The hg_addr_t addr field, if set,
 * needs to be free by the caller using margo_addr_free.
 *
 * @param[in] handle YOKAN database handle
 * @param[out] client Client used to create the handle
 * @param[out] addr Address of the handle
 * @param[out] provider_id Provider id of the handle
 * @param[out] database_id Database id of the handle
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_database_handle_get_info(
        yk_database_handle_t handle,
        yk_client_t* client,
        hg_addr_t* addr,
        uint16_t* provider_id,
        yk_database_id_t* database_id);

/**
 * @brief Increments the reference counter of a database handle.
 *
 * @param handle database handle
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_database_handle_ref_incr(
        yk_database_handle_t handle);

/**
 * @brief Releases the database handle. This will decrement the
 * reference counter, and free the database handle if the reference
 * counter reaches 0.
 *
 * @param[in] handle database handle to release.
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_database_handle_release(yk_database_handle_t handle);

/**
 * @brief Get the number of key/val pairs stored in the database.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[out] count Number of pairs stored.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_count(yk_database_handle_t dbh,
                     int32_t mode,
                     size_t* count);

/**
 * @brief Put a single key/value pair into the database.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] key Key.
 * @param[in] ksize Size of the key.
 * @param[in] value Value.
 * @param[in] vsize Size of the value.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_put(yk_database_handle_t dbh,
                   int32_t mode,
                   const void* key,
                   size_t ksize,
                   const void* value,
                   size_t vsize);

/**
 * @brief Put multiple key/value pairs into the database.
 * The keys and values are provided by arrays of points,
 * and may not be contiguous in memory.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of key/value pairs.
 * @param[in] keys Array of pointers to keys.
 * @param[in] ksizes Array of key sizes.
 * @param[in] values Array of pointers to values.
 * @param[in] vsizes Array of value sizes.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_put_multi(yk_database_handle_t dbh,
                         int32_t mode,
                         size_t count,
                         const void* const* keys,
                         const size_t* ksizes,
                         const void* const* values,
                         const size_t* vsizes);

/**
 * @brief Put multiple key/value pairs into the database.
 * The keys and values are provided via contiguous memory
 * segments.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of key/value pairs.
 * @param[in] keys Buffer containing keys.
 * @param[in] ksizes Array of key sizes.
 * @param[in] values Buffer containing values.
 * @param[in] vsizes Array of value sizes.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_put_packed(yk_database_handle_t dbh,
                          int32_t mode,
                          size_t count,
                          const void* keys,
                          const size_t* ksizes,
                          const void* values,
                          const size_t* vsizes);

/**
 * @brief Low-level put operation based on a bulk handle.
 * This function will take the data in [offset, offset+size[
 * from the bulk handle and interpret it as follows:
 * - The first count * sizeof(size_t) bytes store the key sizes.
 * - The next count * sizeof(size_t) bytes store the value sizes.
 * - The next N bytes store keys back to back, where
 *   N = sum of key sizes
 * - The last M bytes store values back to bake, where
 *   M = sum of value sizes
 * Origin represents the address of the process that created
 * the bulk handle. If NULL, the bulk handle is considered to
 * have been created by the calling process.
 *
 * This function is useful in situation where a process received
 * a bulk handle from another process and wants to forward it to
 * an YOKAN provider.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of key/values in the bulk data.
 * @param[in] origin Address of the process that created the bulk handle.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] size Size of the payload in the bulk handle.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_put_bulk(yk_database_handle_t dbh,
                        int32_t mode,
                        size_t count,
                        const char* origin,
                        hg_bulk_t data,
                        size_t offset,
                        size_t size);

/**
 * @brief Check if the key exists in the database.
 * exists is set to 1 if the key exists, 0 otherwise.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] key Key.
 * @param[in] ksize Size of the key.
 * @param[out] exists Whether the key exists.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_exists(yk_database_handle_t dbh,
                      int32_t mode,
                      const void* key,
                      size_t ksize,
                      uint8_t* exists);

/**
 * @brief Check if the list of keys exist in the database.
 * The flags argument is a pointer to an arrat of size
 * ceil(count/8). Each bit (not byte!) in this array indicates
 * the presence (1) or absence (0) of a corresponding key.
 * The yk_unpack_exists_flag function can be used to return
 * proper booleans from this array.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of keys to check.
 * @param[in] keys Array of pointers to keys.
 * @param[in] ksizes Array of key sizes.
 * @param[out] flags Bitfield indicating whether each key exists.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_exists_multi(yk_database_handle_t dbh,
                            int32_t mode,
                            size_t count,
                            const void* const* keys,
                            const size_t* ksizes,
                            uint8_t* flags);

/**
 * @brief Same as yk_exists_multi but keys are packed
 * contiguously in memory.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of keys to check.
 * @param[in] keys Packed keys.
 * @param[in] ksizes Array of key sizes.
 * @param[out] flags Bitfield indicating whether each key exists.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_exists_packed(yk_database_handle_t dbh,
                             int32_t mode,
                             size_t count,
                             const void* keys,
                             const size_t* ksizes,
                             uint8_t* flags);

/**
 * @brief Low-level exists operation based on a bulk handle.
 * This function will take the data in [offset, offset+size[
 * from the bulk handle and interpret it as follows:
 * - The first count * sizeof(size_t) bytes store the key sizes.
 * - The next N bytes store keys back to back, where
 *   N = sum of key sizes
 * - The last M bytes will be used to store the resulting bitfield
 *   where M = ceil(count/8)
 * Origin represents the address of the process that created
 * the bulk handle. If NULL, the bulk handle is considered to
 * have been created by the calling process.
 *
 * This function is useful in situation where a process received
 * a bulk handle from another process and wants to forward it to
 * an YOKAN provider.
 *
 * Note: the bulk handle must have been created with HG_BULK_READWRITE.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of key/values in the bulk data.
 * @param[in] origin Address of the process that created the bulk handle.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] size Size of the payload in the bulk handle.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_exists_bulk(yk_database_handle_t dbh,
                           int32_t mode,
                           size_t count,
                           const char* origin,
                           hg_bulk_t data,
                           size_t offset,
                           size_t size);

/**
 * @brief Interpret the bitfield returned by the yk_exists_multi,
 * yk_exists_packed, and yk_exists_bulk operation, taking the flags
 * array (as passed to these operations) and an index i, and returning
 * whether key i exists.
 *
 * @param flags bitfield.
 * @param i index of the key.
 *
 * @return whether key at index i exists.
 */
static inline bool yk_unpack_exists_flag(const uint8_t* flags, size_t i)
{
    uint8_t mask = 1 << (i%8);
    return flags[i/8] & mask;
}

/**
 * @brief Get the length of the value associated with a key.
 *
 * @param[in] dbh Database Handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] key Key.
 * @param[in] ksize Size of the key.
 * @param[out] vsize Size of the value.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_length(yk_database_handle_t dbh,
                      int32_t mode,
                      const void* key,
                      size_t ksize,
                      size_t* vsize);

/**
 * @brief Get the size of the values associates with a list of keys.
 * The vsizes pointer should point to a preallocated array of count
 * elements.
 *
 * Note that contrary to yk_length, which will return YOKAN_ERR_KEY_NOT_FOUND
 * if the key is not found, the yk_length_multi function will return
 * YOKAN_SUCCESS (even when none of the keys are found) and size of keys
 * not found will be set to YOKAN_KEY_NOT_FOUND.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of keys.
 * @param[in] keys Array of pointers to keys.
 * @param[in] ksizes Array of key sizes.
 * @param[out] vsizes Resulting array of value sizes.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_length_multi(yk_database_handle_t dbh,
                            int32_t mode,
                            size_t count,
                            const void* const* keys,
                            const size_t* ksizes,
                            size_t* vsizes);

/**
 * @brief Same as yk_length_multi but the keys are packed
 * contiguously in memory.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of keys.
 * @param[in] keys Packed keys.
 * @param[in] ksizes Array of key sizes.
 * @param[out] vsizes Resulting array of value sizes.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_length_packed(yk_database_handle_t dbh,
                             int32_t mode,
                             size_t count,
                             const void* keys,
                             const size_t* ksizes,
                             size_t* vsizes);

/**
 * @brief Low-level length operation based on a bulk handle.
 * This function will take the data in [offset, offset+size[
 * from the bulk handle and interpret it as follows:
 * - The first count * sizeof(size_t) bytes store the key sizes.
 * - The next N bytes store keys back to back, where
 *   N = sum of key sizes
 * - The last count * sizeof(size_t)  bytes will be used to
 *   store the resulting value sizes.
 * Origin represents the address of the process that created
 * the bulk handle. If NULL, the bulk handle is considered to
 * have been created by the calling process.
 *
 * This function is useful in situation where a process received
 * a bulk handle from another process and wants to forward it to
 * an YOKAN provider.
 *
 * Note: the bulk handle must have been created with HG_BULK_READWRITE.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of key/values in the bulk data.
 * @param[in] origin Address of the process that created the bulk handle.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] size Size of the payload in the bulk handle.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_length_bulk(yk_database_handle_t dbh,
                           int32_t mode,
                           size_t count,
                           const char* origin,
                           hg_bulk_t data,
                           size_t offset,
                           size_t size);

/**
 * @brief Get the value associated with a key. The vsize argument
 * provides the initial length of the value buffer, and is set to the
 * actual value size upon success of this function. If the key
 * if not found, the function will return YOKAN_ERR_KEY_NOT_FOUND.
 * If the key is found but the value buffer is too small to hold
 * the value, the function will return YOKAN_ERR_BUFFER_SIZE. In these
 * cases the vsize parameter will be set to YOKAN_KEY_NOT_FOUND and
 * YOKAN_SIZE_TOO_SMALL respectively.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] key Key.
 * @param[in] ksize Size of the key.
 * @param[out] value Value buffer.
 * @param[inout] vsize Size of the buffer, then set to size of the value.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_get(yk_database_handle_t dbh,
                   int32_t mode,
                   const void* key,
                   size_t ksize,
                   void* value,
                   size_t* vsize);

/**
 * @brief Get the values associated with a set of keys.
 * For any key that is not found, the corresponding value size
 * will be set to YOKAN_KEY_NOT_FOUND.
 * For any key that is found but for which the provided
 * value buffer is too small for the value, the corresponding
 * value size will be set to YOKAN_SIZE_TOO_SMALL.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of key/value pairs.
 * @param[in] keys Array of keys.
 * @param[in] ksizes Arrat of key sizes.
 * @param[out] values Array of value buffers.
 * @param[inout] vsizes Array of buffer sizes, set to value sizes.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_get_multi(yk_database_handle_t dbh,
                         int32_t mode,
                         size_t count,
                         const void* const* keys,
                         const size_t* ksizes,
                         void* const* values,
                         size_t* vsizes);

/**
 * @brief Get the values associated with a set of keys.
 * The keys are packed contiguously in memory. This function
 * will also pack values contiguously in the provided memory.
 *
 * For any key that is not found, the corresponding value size
 * will be set to YOKAN_KEY_NOT_FOUND.
 * Once the function reaches a key whose associated value is
 * too big to fit in the buffer, the rest of the value sizes
 * will be set to YOKAN_SIZE_TOO_SMALL.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of key/value pairs.
 * @param[in] keys Packed keys.
 * @param[in] ksizes Arrat of key sizes.
 * @param[in] vbufsize Total memory available in values buffer.
 * @param[out] values Buffer to hold packed values.
 * @param[out] vsizes Value sizes.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_get_packed(yk_database_handle_t dbh,
                          int32_t mode,
                          size_t count,
                          const void* keys,
                          const size_t* ksizes,
                          size_t vbufsize,
                          void* values,
                          size_t* vsizes);

/**
 * @brief Low-level get operation based on a bulk handle.
 * This function will take the data in [offset, offset+size[
 * from the bulk handle and interpret it as follows:
 * - The first count * sizeof(size_t) bytes store the key sizes.
 * - The next count * sizeof(size_t) bytes store the value sizes.
 * - The next N bytes store keys back to back, where
 *   N = sum of key sizes
 * - The last M bytes store values back to bake, where
 *   M = sum of value sizes
 * Origin represents the address of the process that created
 * the bulk handle. If NULL, the bulk handle is considered to
 * have been created by the calling process.
 *
 * This function is useful in situation where a process received
 * a bulk handle from another process and wants to forward it to
 * an YOKAN provider.
 *
 * Note: the "packed" argument is important. It specifies whether
 * the process that created the bulk handle did so by exposing a single
 * contiguous buffer in which packed values are meant to be stored
 * (in which case the value sizes do not matter as an input), or
 * if individual buffers were exposed to hold each value (in which case
 * the value sizes do matter as an input).
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of key/values in the bulk data.
 * @param[in] origin Address of the process that created the bulk handle.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] size Size of the payload in the bulk handle.
 * @param[in] packed Whether values are packed on the client end.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_get_bulk(yk_database_handle_t dbh,
                        int32_t mode,
                        size_t count,
                        const char* origin,
                        hg_bulk_t data,
                        size_t offset,
                        size_t size,
                        bool packed);

/**
 * @brief This function performs a GET but instead of providing a buffer
 * in which to receive the value, the caller provides a function to call
 * on the fetched key/value pair. This function is useful when the size
 * of the value is unknown, but yk_get should be preferred if the size is
 * known.
 *
 * Important: the key/value data passed to the callback are guaranteed to
 * be valid only until the callback returns. Hence if the callback
 *
 * Important: for this function to work, the calling process must have
 * a margo instance running in server mode.
 *
 * @param dbh Database handle.
 * @param mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param key Key data.
 * @param ksize Size of the key.
 * @param cb Callback to invoke on the key/value pair.
 * @param uargs Argument for the callback.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_fetch(yk_database_handle_t dbh,
                     int32_t mode,
                     const void* key,
                     size_t ksize,
                     yk_keyvalue_callback_t cb,
                     void* uargs);


/**
 * @brief Options for the yk_fetch_* functions.
 */
typedef struct yk_fetch_options {
    ABT_pool pool;       /* pool in which to run the callback */
    unsigned batch_size; /* value are sent back in batches of this size */
} yk_fetch_options_t;

/**
 * @brief Packed version of yk_fetch meant to fetch multiple values at once
 * from a pack of keys.
 *
 * The options argument, if provided, will indicate how the processing is done.
 *
 * @param dbh Database handle.
 * @param mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param count
 * @param keys Packed keys data.
 * @param ksizes Size of the keys.
 * @param cb Callback to invoke on the key/value pair.
 * @param uargs Argument for the callback.
 * @param options Batching and parallelism options.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_fetch_packed(yk_database_handle_t dbh,
                            int32_t mode,
                            size_t count,
                            const void* keys,
                            const size_t* ksizes,
                            yk_keyvalue_callback_t cb,
                            void* uargs,
                            const yk_fetch_options_t* options);

/**
 * @brief Multi version of yk_fetch meant to fetch multiple values at once
 * from an array of keys.
 *
 * The options argument, if provided, will indicate how the processing is done.
 *
 * @param dbh Database handle.
 * @param mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param count
 * @param keys keys data.
 * @param ksizes Size of the keys.
 * @param cb Callback to invoke on the key/value pair.
 * @param uargs Argument for the callback.
 * @param options Batching and parallelism options.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_fetch_multi(yk_database_handle_t dbh,
                           int32_t mode,
                           size_t count,
                           const void* const* keys,
                           const size_t* ksizes,
                           yk_keyvalue_callback_t cb,
                           void* uargs,
                           const yk_fetch_options_t* options);

/**
 * @brief Bulk version of yk_fetch meant to fetch multiple values at once
 * from a bulk representing the data (array of size_t followed by the actual keys).
 *
 * The options argument, if provided, will indicate how the processing is done.
 *
 * @param dbh Database handle.
 * @param mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param count
 * @param keys keys data.
 * @param ksizes Size of the keys.
 * @param cb Callback to invoke on the key/value pair.
 * @param uargs Argument for the callback.
 * @param options Batching and parallelism options.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_fetch_bulk(yk_database_handle_t dbh,
                          int32_t mode,
                          size_t count,
                          const char* origin,
                          hg_bulk_t data,
                          size_t offset,
                          size_t size,
                          yk_keyvalue_callback_t cb,
                          void* uargs,
                          const yk_fetch_options_t* options);
/**
 * @brief Erase a key/value pair associated with the given key.
 * Note that this function will not return an error if the key
 * does not exist.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] key Key to erase.
 * @param[in] ksize Size of the key.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_erase(yk_database_handle_t dbh,
                     int32_t mode,
                     const void* key,
                     size_t ksize);

/**
 * @brief Erase multiple key/value pairs.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of key/value pairs.
 * @param[in] keys Array of keys.
 * @param[in] ksizes Array of key sizes.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_erase_multi(yk_database_handle_t dbh,
                           int32_t mode,
                           size_t count,
                           const void* const* keys,
                           const size_t* ksizes);

/**
 * @brief Erase multiple key/value pairs. Contrary
 * to yk_erase_multi, the keys are packed into a single
 * contiguous buffer.
 *
 * @param dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param count Number of key/value pairs.
 * @param keys Packed keys.
 * @param ksizes Size of the keys.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_erase_packed(yk_database_handle_t dbh,
                            int32_t mode,
                            size_t count,
                            const void* keys,
                            const size_t* ksizes);

/**
 * @brief Low-level erase operation based on a bulk handle.
 * This function will take the data in [offset, offset+size[
 * from the bulk handle and interpret it as follows:
 * - The first count * sizeof(size_t) bytes store the key sizes.
 * - The next N bytes store keys back to back, where
 *   N = sum of key sizes
 * Origin represents the address of the process that created
 * the bulk handle. If NULL, the bulk handle is considered to
 * have been created by the calling process.
 *
 * This function is useful in situation where a process received
 * a bulk handle from another process and wants to forward it to
 * an YOKAN provider.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] count Number of key/values in the bulk data.
 * @param[in] origin Address of the process that created the bulk handle.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] size Size of the payload in the bulk handle.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_erase_bulk(yk_database_handle_t dbh,
                          int32_t mode,
                          size_t count,
                          const char* origin,
                          hg_bulk_t data,
                          size_t offset,
                          size_t size);


/**
 * @brief Lists up to count keys from from_key (included if
 * inclusive is set to true), filtering keys if a filter is provided.
 *
 * Unless a specific mode is used to change it, the filter is
 * considered as a prefix that the keys must start with.
 *
 * If a key size is too small to hold the key, the size will be
 * set to YOKAN_SIZE_TOO_SMALL.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] from_key Starting key.
 * @param[in] from_ksize Starting key size.
 * @param[in] filter Key filter.
 * @param[in] filter_size Filter size.
 * @param[in] count Max keys to read (in) / number actually read (out).
 * @param[out] keys Array of buffers to hold keys.
 * @param[inout] ksizes Array of key sizes.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_list_keys(yk_database_handle_t dbh,
                         int32_t mode,
                         const void* from_key,
                         size_t from_ksize,
                         const void* filter,
                         size_t filter_size,
                         size_t count,
                         void* const* keys,
                         size_t* ksizes);

/**
 * @brief Same as yk_list_keys but using a contiguous buffer to hold keys.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] from_key Starting key.
 * @param[in] from_ksize Starting key size.
 * @param[in] filter Key filter.
 * @param[in] filter_size Filter size.
 * @param[in] count Max keys to read.
 * @param[out] keys Buffer to hold keys.
 * @param[in] keys_buf_size Size of the buffer to hold keys.
 * @param[out] ksizes Array of key sizes.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_list_keys_packed(yk_database_handle_t dbh,
                                int32_t mode,
                                const void* from_key,
                                size_t from_ksize,
                                const void* filter,
                                size_t filter_size,
                                size_t count,
                                void* keys,
                                size_t keys_buf_size,
                                size_t* ksizes);

/**
 * @brief Low-level list_keys operation using a bulk handle.
 * This function will take the data in [offset, offset+size[
 * from the bulk handle and interpret it as follows:
 * - The first from_ksize bytes represent the start key.
 * - The next filter_size byres represent the filter.
 * - The next count * sizeof(size_t) bytes represent the key sizes.
 * - The next N bytes store keys back to back, where
 *   N = sum of key sizes
 * Origin represents the address of the process that created
 * the bulk handle. If NULL, the bulk handle is considered to
 * have been created by the calling process.
 *
 * This function is useful in situation where a process received
 * a bulk handle from another process and wants to forward it to
 * an YOKAN provider.
 *
 * Note: the bulk handle must have been created with HG_BULK_READWRITE.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] from_ksize Starting key size.
 * @param[in] filter_size Filter size.
 * @param[in] origin Origin address.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] keys_buf_size Total size allocated for keys.
 * @param[in] packed Whether keys are packed on the client.
 * @param[in] count Max keys to read.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_list_keys_bulk(yk_database_handle_t dbh,
                              int32_t mode,
                              size_t from_ksize,
                              size_t filter_size,
                              const char* origin,
                              hg_bulk_t data,
                              size_t offset,
                              size_t keys_buf_size,
                              bool packed,
                              size_t count);


/**
 * @brief Lists up to count key/value pairs from from_key (included if
 * inclusive is set to true), filtering keys if a filter is provided.
 *
 * Unless a specific mode is used to change it, the filter is
 * considered as a prefix that the keys must start with.
 *
 * If a key size is too small to hold the key, the size will be
 * set to YOKAN_SIZE_TOO_SMALL.
 * If a value size is too small to hold the value, the size will be
 * set to YOKAN_SIZE_TOO_SMALL.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] from_key Starting key.
 * @param[in] from_ksize Starting key size.
 * @param[in] filter Key filter.
 * @param[in] filter_size Filter size.
 * @param[in] count Max keys to read.
 * @param[out] keys Array of buffers to hold keys.
 * @param[inout] ksizes Array of key sizes.
 * @param[out] values Array of buffers to hold values.
 * @param[inout] vsizes Array of value sizes.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_list_keyvals(yk_database_handle_t dbh,
                            int32_t mode,
                            const void* from_key,
                            size_t from_ksize,
                            const void* filter,
                            size_t filter_size,
                            size_t count,
                            void* const* keys,
                            size_t* ksizes,
                            void* const* values,
                            size_t* vsizes);

/**
 * @brief Same as yk_list_keyvals but using contiguous buffers
 * to hold keys and values.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] from_key Starting key.
 * @param[in] from_ksize Starting key size.
 * @param[in] filter Key filter.
 * @param[in] filter_size Filter size.
 * @param[in] count Max keys to read.
 * @param[out] keys Buffer to hold keys.
 * @param[in] keys_buf_size Size of the buffer to hold keys.
 * @param[out] ksizes Array of key sizes.
 * @param[out] values Buffer to hold values.
 * @param[in] vals_buf_size Size of the buffer to hold values.
 * @param[out] vsizes Array of value sizes.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_list_keyvals_packed(yk_database_handle_t dbh,
                                   int32_t mode,
                                   const void* from_key,
                                   size_t from_ksize,
                                   const void* filter,
                                   size_t filter_size,
                                   size_t count,
                                   void* keys,
                                   size_t keys_buf_size,
                                   size_t* ksizes,
                                   void* values,
                                   size_t vals_buf_size,
                                   size_t* vsizes);

/**
 * @brief Low-level list_keyvals operation using a bulk handle.
 * This function will take the data in [offset, offset+size[
 * from the bulk handle and interpret it as follows:
 * - The first from_ksize bytes represent the start key.
 * - The next filter_size byres represent the filter.
 * - The next count * sizeof(size_t) bytes represent the key sizes.
 * - The next count * sizeof(size_t) bytes represent the value sizes.
 * - The next key_buf_size bytes will store keys back to back
 * - The next val_buf_size bytes will store values back to back
 * Origin represents the address of the process that created
 * the bulk handle. If NULL, the bulk handle is considered to
 * have been created by the calling process.
 *
 * This function is useful in situation where a process received
 * a bulk handle from another process and wants to forward it to
 * an YOKAN provider.
 *
 * Note: the bulk handle must have been created with HG_BULK_READWRITE.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] from_ksize Starting key size.
 * @param[in] filter_size Filter size.
 * @param[in] origin Origin address.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] key_buf_size Size of the buffer allocated for keys.
 * @param[in] val_buf_size Size of the buffer allocated for values.
 * @param[in] packed Whether the data is packed on the client side.
 * @param[in] count Max keys to read.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_list_keyvals_bulk(yk_database_handle_t dbh,
                                 int32_t mode,
                                 size_t from_ksize,
                                 size_t filter_size,
                                 const char* origin,
                                 hg_bulk_t data,
                                 size_t offset,
                                 size_t key_buf_size,
                                 size_t val_buf_size,
                                 bool packed,
                                 size_t count);

typedef struct yk_iter_options {
    unsigned recv_batch_size; /* how many items to receive at once */
} yk_iter_options_t;

/**
 * @brief Iterate up to count keys from from_key (included if
 * inclusive is set in the mode), filtering keys if a filter is provided.
 *
 * Unless a specific mode is used to change it, the filter is
 * considered as a prefix that the keys must start with.
 *
 * If count is set to 0, this function will iterate until the end.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] from_key Starting key.
 * @param[in] from_ksize Starting key size.
 * @param[in] filter Key filter.
 * @param[in] filter_size Filter size.
 * @param[in] count Max keys to read, 0 for all.
 * @param[in] cb Callback to call in each key.
 * @param[in] uargs Callback user-argument.
 * @param[in] options Options.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_iter_keys(yk_database_handle_t dbh,
                         int32_t mode,
                         const void* from_key,
                         size_t from_ksize,
                         const void* filter,
                         size_t filter_size,
                         size_t count,
                         yk_keyvalue_callback_t cb,
                         void* uargs,
                         const yk_iter_options_t* options);

/**
 * @brief Iterate up to count key/value pairs from from_key (included if
 * inclusive is set in the mode), filtering keys if a filter is provided.
 *
 * Unless a specific mode is used to change it, the filter is
 * considered as a prefix that the keys must start with.
 *
 * If count is set to 0, this function will iterate until the end.
 *
 * @param[in] dbh Database handle.
 * @param[in] mode 0 or bitwise "or" of YOKAN_MODE_* flags.
 * @param[in] from_key Starting key.
 * @param[in] from_ksize Starting key size.
 * @param[in] filter Key filter.
 * @param[in] filter_size Filter size.
 * @param[in] count Max keys to read, 0 for all.
 * @param[in] cb Callback to call in each key.
 * @param[in] uargs Callback user-argument.
 * @param[in] options Options.
 *
 * @return YOKAN_SUCCESS or corresponding error code.
 */
yk_return_t yk_iter_keyvals(yk_database_handle_t dbh,
                            int32_t mode,
                            const void* from_key,
                            size_t from_ksize,
                            const void* filter,
                            size_t filter_size,
                            size_t count,
                            yk_keyvalue_callback_t cb,
                            const yk_iter_options_t* options);
#ifdef __cplusplus
}
#endif

#endif
