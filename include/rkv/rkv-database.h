/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_DATABASE_H
#define __RKV_DATABASE_H

#include <stdbool.h>
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

/**
 * @brief Put a single key/value pair into the database.
 *
 * @param[in] dbh Database handle.
 * @param[in] key Key.
 * @param[in] ksize Size of the key.
 * @param[in] value Value.
 * @param[in] vsize Size of the value.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_put(rkv_database_handle_t dbh,
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
 * @param[in] count Number of key/value pairs.
 * @param[in] keys Array of pointers to keys.
 * @param[in] ksizes Array of key sizes.
 * @param[in] values Array of pointers to values.
 * @param[in] vsizes Array of value sizes.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_put_multi(rkv_database_handle_t dbh,
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
 * @param[in] count Number of key/value pairs.
 * @param[in] keys Buffer containing keys.
 * @param[in] ksizes Array of key sizes.
 * @param[in] values Buffer containing values.
 * @param[in] vsizes Array of value sizes.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_put_packed(rkv_database_handle_t dbh,
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
 * an RKV provider.
 *
 * @param[in] dbh Database handle.
 * @param[in] count Number of key/values in the bulk data.
 * @param[in] origin Address of the process that created the bulk handle.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] size Size of the payload in the bulk handle.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_put_bulk(rkv_database_handle_t dbh,
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
 * @param[in] key Key.
 * @param[in] ksize Size of the key.
 * @param[out] exists Whether the key exists.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_exists(rkv_database_handle_t dbh,
                        const void* key,
                        size_t ksize,
                        uint8_t* exists);

/**
 * @brief Check if the list of keys exist in the database.
 * The flags argument is a pointer to an arrat of size
 * ceil(count/8). Each bit (not byte!) in this array indicates
 * the presence (1) or absence (0) of a corresponding key.
 * The rkv_unpack_exists_flag function can be used to return
 * proper booleans from this array.
 *
 * @param[in] dbh Database handle.
 * @param[in] count Number of keys to check.
 * @param[in] keys Array of pointers to keys.
 * @param[in] ksizes Array of key sizes.
 * @param[out] flags Bitfield indicating whether each key exists.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_exists_multi(rkv_database_handle_t dbh,
                              size_t count,
                              const void* const* keys,
                              const size_t* ksizes,
                              uint8_t* flags);

/**
 * @brief Same as rkv_exists_multi but keys are packed
 * contiguously in memory.
 *
 * @param dbh Database handle.
 * @param count Number of keys to check.
 * @param keys Packed keys.
 * @param ksizes Array of key sizes.
 * @param flags Bitfield indicating whether each key exists.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_exists_packed(rkv_database_handle_t dbh,
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
 * an RKV provider.
 *
 * Note: the bulk handle must have been created with HG_BULK_READWRITE.
 *
 * @param[in] dbh Database handle.
 * @param[in] count Number of key/values in the bulk data.
 * @param[in] origin Address of the process that created the bulk handle.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] size Size of the payload in the bulk handle.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_exists_bulk(rkv_database_handle_t dbh,
                             size_t count,
                             const char* origin,
                             hg_bulk_t data,
                             size_t offset,
                             size_t size);

/**
 * @brief Interpret the bitfield returned by the rkv_exists_multi,
 * rkv_exists_packed, and rkv_exists_bulk operation, taking the flags
 * array (as passed to these operations) and an index i, and returning
 * whether key i exists.
 *
 * @param flags bitfield.
 * @param i index of the key.
 *
 * @return whether key at index i exists.
 */
static inline bool rkv_unpack_exists_flag(const uint8_t* flags, size_t i)
{
    uint8_t mask = 1 << (i%8);
    return flags[i/8] & mask;
}

/**
 * @brief Get the length of the value associated with a key.
 *
 * @param[in] dbh Database Handle.
 * @param[in] key Key.
 * @param[in] ksize Size of the key.
 * @param[out] vsize Size of the value.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_length(rkv_database_handle_t dbh,
                        const void* key,
                        size_t ksize,
                        size_t* vsize);

/**
 * @brief Get the size of the values associates with a list of keys.
 * The vsizes pointer should point to a preallocated array of count
 * elements.
 *
 * Note that contrary to rkv_length, which will return RKV_ERR_KEY_NOT_FOUND
 * if the key is not found, the rkv_length_multi function will return
 * RKV_SUCCESS (even when none of the keys are found) and size of keys
 * not found will be set to RKV_KEY_NOT_FOUND.
 *
 * @param[in] dbh Database handle.
 * @param[in] count Number of keys.
 * @param[in] keys Array of pointers to keys.
 * @param[in] ksizes Array of key sizes.
 * @param[out] vsizes Resulting array of value sizes.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_length_multi(rkv_database_handle_t dbh,
                              size_t count,
                              const void* const* keys,
                              const size_t* ksizes,
                              size_t* vsizes);

/**
 * @brief Same as rkv_length_multi but the keys are packed
 * contiguously in memory.
 *
 * @param[in] dbh Database handle.
 * @param[in] count Number of keys.
 * @param[in] keys Packed keys.
 * @param[in] ksizes Array of key sizes.
 * @param[out] vsizes Resulting array of value sizes.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_length_packed(rkv_database_handle_t dbh,
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
 * an RKV provider.
 *
 * Note: the bulk handle must have been created with HG_BULK_READWRITE.
 *
 * @param[in] dbh Database handle.
 * @param[in] count Number of key/values in the bulk data.
 * @param[in] origin Address of the process that created the bulk handle.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] size Size of the payload in the bulk handle.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_length_bulk(rkv_database_handle_t dbh,
                             size_t count,
                             const char* origin,
                             hg_bulk_t data,
                             size_t offset,
                             size_t size);

/**
 * @brief Get the value associated with a key. The vsize argument
 * provides the initial length of the value buffer, and is set to the
 * actual value size upon success of this function. If the key
 * if not found, the function will return RKV_ERR_KEY_NOT_FOUND.
 * If the key is found but the value buffer is too small to hold
 * the value, the function will return RKV_ERR_BUFFER_SIZE. In these
 * cases the vsize parameter will be set to RKV_KEY_NOT_FOUND and
 * RKV_SIZE_TOO_SMALL respectively.
 *
 * @param[in] dbh Database handle.
 * @param[in] key Key.
 * @param[in] ksize Size of the key.
 * @param[out] value Value buffer.
 * @param[inout] vsize Size of the buffer, then set to size of the value.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_get(rkv_database_handle_t dbh,
                     const void* key,
                     size_t ksize,
                     void* value,
                     size_t* vsize);

/**
 * @brief Get the values associated with a set of keys.
 * For any key that is not found, the corresponding value size
 * will be set to RKV_KEY_NOT_FOUND.
 * For any key that is found but for which the provided
 * value buffer is too small for the value, the corresponding
 * value size will be set to RKV_SIZE_TOO_SMALL.
 *
 * @param[in] dbh Database handle.
 * @param[in] count Number of key/value pairs.
 * @param[in] keys Array of keys.
 * @param[in] ksizes Arrat of key sizes.
 * @param[out] values Array of value buffers.
 * @param[inout] vsizes Array of buffer sizes, set to value sizes.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_get_multi(rkv_database_handle_t dbh,
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
 * will be set to RKV_KEY_NOT_FOUND.
 * Once the function reaches a key whose associated value is
 * too big to fit in the buffer, the rest of the value sizes
 * will be set to RKV_SIZE_TOO_SMALL.
 *
 * @param[in] dbh Database handle.
 * @param[in] count Number of key/value pairs.
 * @param[in] keys Packed keys.
 * @param[in] ksizes Arrat of key sizes.
 * @param[in] vbufsize Total memory available in values buffer.
 * @param[out] values Buffer to hold packed values.
 * @param[out] vsizes Value sizes.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_get_packed(rkv_database_handle_t dbh,
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
 * an RKV provider.
 *
 * Note: the "packed" argument is important. It specifies whether
 * the process that created the bulk handle did so by exposing a single
 * contiguous buffer in which packed values are meant to be stored
 * (in which case the value sizes do not matter as an input), or
 * if individual buffers were exposed to hold each value (in which case
 * the value sizes do matter as an input).
 *
 * @param[in] dbh Database handle.
 * @param[in] count Number of key/values in the bulk data.
 * @param[in] origin Address of the process that created the bulk handle.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] size Size of the payload in the bulk handle.
 * @param[in] packed Whether values are packed on the client end.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_get_bulk(rkv_database_handle_t dbh,
                          size_t count,
                          const char* origin,
                          hg_bulk_t data,
                          size_t offset,
                          size_t size,
                          bool packed);

/**
 * @brief Erase a key/value pair associated with the given key.
 * Note that this function will not return an error if the key
 * does not exist.
 *
 * @param[in] dbh Database handle.
 * @param[in] key Key to erase.
 * @param[in] ksize Size of the key.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_erase(rkv_database_handle_t dbh,
                       const void* key,
                       size_t ksize);

/**
 * @brief Erase multiple key/value pairs.
 *
 * @param[in] dbh Database handle.
 * @param[in] count Number of key/value pairs.
 * @param[in] keys Array of keys.
 * @param[in] ksizes Array of key sizes.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_erase_multi(rkv_database_handle_t dbh,
                             size_t count,
                             const void* const* keys,
                             const size_t* ksizes);

/**
 * @brief Erase multiple key/value pairs. Contrary
 * to rkv_erase_multi, the keys are packed into a single
 * contiguous buffer.
 *
 * @param dbh Database handle.
 * @param count Number of key/value pairs.
 * @param keys Packed keys.
 * @param ksizes Size of the keys.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_erase_packed(rkv_database_handle_t dbh,
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
 * an RKV provider.
 *
 * @param[in] dbh Database handle.
 * @param[in] count Number of key/values in the bulk data.
 * @param[in] origin Address of the process that created the bulk handle.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] size Size of the payload in the bulk handle.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_erase_bulk(rkv_database_handle_t dbh,
                            size_t count,
                            const char* origin,
                            hg_bulk_t data,
                            size_t offset,
                            size_t size);


/**
 * @brief Lists up to count keys from from_key (included if
 * inclusive is set to true), filtering by prefix if the prefix
 * is provided.
 *
 * If a key size is too small to hold the key, the size will be
 * set to RKV_SIZE_TOO_SMALL.
 *
 * @param[in] dbh Database handle.
 * @param[in] inclusive Whether to include from_key in the result.
 * @param[in] from_key Starting key.
 * @param[in] from_ksize Starting key size.
 * @param[in] prefix Key prefix.
 * @param[in] prefix_size Prefix size.
 * @param[inout] count Max keys to read (in) / number actually read (out).
 * @param[out] keys Array of buffers to hold keys.
 * @param[inout] ksizes Array of key sizes.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_list_keys(rkv_database_handle_t dbh,
                           bool inclusive,
                           const void* from_key,
                           size_t from_ksize,
                           const void* prefix,
                           size_t prefix_size,
                           size_t* count,
                           void* const* keys,
                           size_t* ksizes);

/**
 * @brief Same as rkv_list_keys but using a contiguous buffer to hold keys.
 *
 * @param[in] dbh Database handle.
 * @param[in] inclusive Whether to include from_key in the result.
 * @param[in] from_key Starting key.
 * @param[in] from_ksize Starting key size.
 * @param[in] prefix Key prefix.
 * @param[in] prefix_size Prefix size.
 * @param[inout] count Max keys to read (in) / number actually read (out).
 * @param[out] keys Buffer to hold keys.
 * @param[in] keys_buf_size Size of the buffer to hold keys.
 * @param[out] ksizes Array of key sizes.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_list_keys_packed(rkv_database_handle_t dbh,
                                  bool inclusive,
                                  const void* from_key,
                                  size_t from_ksize,
                                  const void* prefix,
                                  size_t prefix_size,
                                  size_t* count,
                                  void* keys,
                                  size_t keys_buf_size,
                                  size_t* ksizes);

/**
 * @brief Low-level list_keys operation using a bulk handle.
 * This function will take the data in [offset, offset+size[
 * from the bulk handle and interpret it as follows:
 * - The first from_ksize bytes represent the start key.
 * - The next prefix_size byres represent the prefix.
 * - The next count * sizeof(size_t) bytes represent the key sizes.
 * - The next N bytes store keys back to back, where
 *   N = sum of key sizes
 * Origin represents the address of the process that created
 * the bulk handle. If NULL, the bulk handle is considered to
 * have been created by the calling process.
 *
 * This function is useful in situation where a process received
 * a bulk handle from another process and wants to forward it to
 * an RKV provider.
 *
 * Note: the bulk handle must have been created with HG_BULK_READWRITE.
 *
 * @param[in] dbh Database handle.
 * @param[in] inclusive Whether to include from_key in the result.
 * @param[in] from_ksize Starting key size.
 * @param[in] prefix_size Prefix size.
 * @param[in] origin Origin address.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] keys_buf_size Total size allocated for keys.
 * @param[in] packed Whether keys are packed on the client.
 * @param[inout] count Max keys to read (in) / number actually read (out).
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_list_keys_bulk(rkv_database_handle_t dbh,
                                bool inclusive,
                                size_t from_ksize,
                                size_t prefix_size,
                                const char* origin,
                                hg_bulk_t data,
                                size_t offset,
                                size_t keys_buf_size,
                                bool packed,
                                size_t* count);


/**
 * @brief Lists up to count key/value pairs from from_key (included if
 * inclusive is set to true), filtering keys by prefix if the prefix
 * is provided.
 *
 * If a key size is too small to hold the key, the size will be
 * set to RKV_SIZE_TOO_SMALL.
 * If a value size is too small to hold the value, the size will be
 * set to RKV_SIZE_TOO_SMALL.
 *
 * @param[in] dbh Database handle.
 * @param[in] inclusive Whether to include from_key in the result.
 * @param[in] from_key Starting key.
 * @param[in] from_ksize Starting key size.
 * @param[in] prefix Key prefix.
 * @param[in] prefix_size Prefix size.
 * @param[inout] count Max keys to read (in) / number actually read (out).
 * @param[out] keys Array of buffers to hold keys.
 * @param[inout] ksizes Array of key sizes.
 * @param[out] values Array of buffers to hold values.
 * @param[inout] vsizes Array of value sizes.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_list_keyvals(rkv_database_handle_t dbh,
                              bool inclusive,
                              const void* from_key,
                              size_t from_ksize,
                              const void* prefix,
                              size_t prefix_size,
                              size_t* count,
                              void* const* keys,
                              size_t* ksizes,
                              void* const* values,
                              size_t* vsizes);

/**
 * @brief Same as rkv_list_keyvals but using contiguous buffers
 * to hold keys and values.
 *
 * @param[in] dbh Database handle.
 * @param[in] inclusive Whether to include from_key in the result.
 * @param[in] from_key Starting key.
 * @param[in] from_ksize Starting key size.
 * @param[in] prefix Key prefix.
 * @param[in] prefix_size Prefix size.
 * @param[inout] count Max keys to read (in) / number actually read (out).
 * @param[out] keys Buffer to hold keys.
 * @param[in] keys_buf_size Size of the buffer to hold keys.
 * @param[out] ksizes Array of key sizes.
 * @param[out] values Buffer to hold values.
 * @param[in] vals_buf_size Size of the buffer to hold values.
 * @param[out] vsizes Array of value sizes.
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_list_keyvals_packed(rkv_database_handle_t dbh,
                                     bool inclusive,
                                     const void* from_key,
                                     size_t from_ksize,
                                     const void* prefix,
                                     size_t prefix_size,
                                     size_t* count,
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
 * - The next prefix_size byres represent the prefix.
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
 * an RKV provider.
 *
 * Note: the bulk handle must have been created with HG_BULK_READWRITE.
 *
 * @param[in] dbh Database handle.
 * @param[in] inclusive Whether to include from_key in the result.
 * @param[in] from_ksize Starting key size.
 * @param[in] prefix_size Prefix size.
 * @param[in] origin Origin address.
 * @param[in] data Bulk handle containing the data.
 * @param[in] offset Offset at which the payload starts in the bulk handle.
 * @param[in] key_buf_size Size of the buffer allocated for keys.
 * @param[in] val_buf_size Size of the buffer allocated for values.
 * @param[in] packed Whether the data is packed on the client side.
 * @param[inout] count Max keys to read (in) / number actually read (out).
 *
 * @return RKV_SUCCESS or corresponding error code.
 */
rkv_return_t rkv_list_keyvals_bulk(rkv_database_handle_t dbh,
                                   bool inclusive,
                                   size_t from_ksize,
                                   size_t prefix_size,
                                   const char* origin,
                                   hg_bulk_t data,
                                   size_t offset,
                                   size_t key_buf_size,
                                   size_t val_buf_size,
                                   bool packed,
                                   size_t* count);

#ifdef __cplusplus
}
#endif

#endif
