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

rkv_return_t rkv_put(rkv_database_handle_t dbh,
                     const void* key,
                     size_t ksize,
                     const void* value,
                     size_t vsize);

rkv_return_t rkv_put_bulk(rkv_database_handle_t dbh,
                          size_t count,
                          const char* origin,
                          hg_bulk_t data,
                          size_t offset,
                          size_t size);

rkv_return_t rkv_put_multi(rkv_database_handle_t dbh,
                           size_t count,
                           const void* const* keys,
                           const size_t* ksizes,
                           const void* const* values,
                           const size_t* vsizes);

rkv_return_t rkv_put_packed(rkv_database_handle_t dbh,
                            size_t count,
                            const void* keys,
                            const size_t* ksizes,
                            const void* values,
                            const size_t* vsizes);

rkv_return_t rkv_exists(rkv_database_handle_t dbh,
                        const void* key,
                        size_t ksize,
                        bool* exists);

rkv_return_t rkv_exists_multi(rkv_database_handle_t dbh,
                              size_t count,
                              const void* const* keys,
                              const size_t* ksizes,
                              bool* exist);

rkv_return_t rkv_exists_packed(rkv_database_handle_t dbh,
                               size_t count,
                               const void* keys,
                               const size_t ksizes,
                               bool* exist);

rkv_return_t rkv_length(rkv_database_handle_t dbh,
                        const void* key,
                        size_t ksize,
                        size_t* vsize);

rkv_return_t rkv_length_multi(rkv_database_handle_t dbh,
                              size_t count,
                              const void* const* keys,
                              const size_t* ksizes,
                              size_t* vsizes);

rkv_return_t rkv_length_packed(rkv_database_handle_t dbh,
                               size_t count,
                               const void* keys,
                               const size_t* ksizes,
                               size_t* vsizes);

rkv_return_t rkv_get(rkv_database_handle_t dbh,
                     const void* key,
                     size_t ksize,
                     void* value,
                     size_t* vsize);

rkv_return_t rkv_get_bulk(rkv_database_handle_t dbh,
                          size_t count,
                          const char* origin,
                          hg_bulk_t data,
                          size_t offset,
                          size_t size,
                          bool packed);

rkv_return_t rkv_get_multi(rkv_database_handle_t dbh,
                           size_t count,
                           const void* const* keys,
                           const size_t* ksizes,
                           void* const* values,
                           size_t* vsizes);

rkv_return_t rkv_get_packed(rkv_database_handle_t dbh,
                            size_t count,
                            const void* keys,
                            const size_t* ksizes,
                            size_t vbufsize,
                            void* values,
                            size_t* vsizes);

rkv_return_t rkv_erase_bulk(rkv_database_handle_t dbh,
                            size_t count,
                            const char* origin,
                            hg_bulk_t data,
                            size_t offset,
                            size_t size);

rkv_return_t rkv_erase(rkv_database_handle_t dbh,
                       const void* key,
                       size_t ksize);

rkv_return_t rkv_erase_multi(rkv_database_handle_t dbh,
                             size_t count,
                             const void* const* keys,
                             const size_t* ksizes);

rkv_return_t rkv_erase_packed(rkv_database_handle_t dbh,
                              size_t count,
                              const void* keys,
                              const size_t* ksizes);

rkv_return_t rkv_list_keys(rkv_database_handle_t dbh,
                           size_t max,
                           bool inclusive,
                           const void* from_key,
                           size_t from_ksize,
                           const void* prefix,
                           size_t prefix_size,
                           void* const* keys,
                           size_t* ksizes);

rkv_return_t rkv_list_keys_packed(rkv_database_handle_t dbh,
                                  size_t max,
                                  bool inclusive,
                                  const void* from_key,
                                  size_t from_ksize,
                                  const void* prefix,
                                  size_t prefix_size,
                                  void* keys,
                                  size_t* ksizes);

rkv_return_t rkv_list_keyvals(rkv_database_handle_t dbh,
                              size_t max,
                              bool inclusive,
                              const void* from_key,
                              size_t from_ksize,
                              const void* prefix,
                              size_t prefix_size,
                              void* const* keys,
                              size_t* ksizes,
                              void* const* values,
                              size_t* vsizes);

rkv_return_t rkv_list_keyvals_packed(rkv_database_handle_t dbh,
                                     size_t max,
                                     bool inclusive,
                                     const void* from_key,
                                     size_t from_ksize,
                                     const void* prefix,
                                     size_t prefix_size,
                                     void* keys,
                                     size_t* ksizes,
                                     void* values,
                                     size_t* vsizes);

#ifdef __cplusplus
}
#endif

#endif
