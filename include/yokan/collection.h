/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_COLLECTION_H
#define __YOKAN_COLLECTION_H

#include <stdbool.h>
#include <margo.h>
#include <yokan/common.h>
#include <yokan/database.h>
#include <yokan/client.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Create a collection in the specified database.
 *
 * @param[in] dbh Database handle
 * @param[in] mode Mode
 * @param[in] name Name of the collection (null-terminated)
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_create(yk_database_handle_t dbh,
                           int32_t mode,
                           const char* name);

/**
 * @brief Erase the collection from the underlying database.
 *
 * @param[in] dbh Database handle
 * @param[in] mode Mode
 * @param[in] collection Collection name (null-terminated)
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_drop(yk_database_handle_t dbh,
                         int32_t mode,
                         const char* collection);

/**
 * @brief Check if the collection exists in the underlying database.
 *
 * @param[in] dbh Database handle
 * @param[in] mode Mode
 * @param[in] collection Collection name (null-terminated)
 * @param[out] flag set to 1 if the collection exists, 0 otherwise
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_exists(yk_database_handle_t dbh,
                           int32_t mode,
                           const char* collection,
                           uint8_t* flag);

/**
 * @brief Store a record into the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] record Record to store
 * @param[in] size Size of the record
 * @param[out] id Resulting record id
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_store(yk_database_handle_t dbh,
                          const char* collection,
                          int32_t mode,
                          const void* record,
                          size_t size,
                          yk_id_t* id);

/**
 * @brief Store multiple records into the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of records to store
 * @param[in] records Array of records
 * @param[in] rsizes Array of record sizes
 * @param[out] ids Resulting record ids
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_store_multi(yk_database_handle_t dbh,
                                const char* collection,
                                int32_t mode,
                                size_t count,
                                const void* const* records,
                                const size_t* rsizes,
                                yk_id_t* ids);

/**
 * @brief Same as yk_coll_store_multi but the records
 * are packed contiguously in memory.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of records to store
 * @param[in] records Packed records
 * @param[in] rsizes Array of record sizes
 * @param[out] ids Resulting record ids
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_store_packed(yk_database_handle_t dbh,
                                 const char* collection,
                                 int32_t mode,
                                 size_t count,
                                 const void* records,
                                 const size_t* rsizes,
                                 yk_id_t* ids);

/**
 * @brief Load a record from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[int] id Record id
 * @param[out] record Record to load
 * @param[inout] size Size of the buffer (in) / record (out)
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_load(yk_database_handle_t dbh,
                         const char* collection,
                         int32_t mode,
                         yk_id_t id,
                         void* record,
                         size_t* size);

/**
 * @brief Load multiple records from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of records to store
 * @param[in] ids Record ids
 * @param[out] records Array of records
 * @param[inout] rsizes Array of buffer/record sizes
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_load_multi(yk_database_handle_t dbh,
                               const char* collection,
                               int32_t mode,
                               size_t count,
                               const yk_id_t* ids,
                               void* const* records,
                               size_t* rsizes);

/**
 * @brief Same as yk_coll_load_multi but the records
 * are packed contiguously in memory.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of records to store
 * @param[in] ids Record ids
 * @param[in] rbufsize Record buffer size
 * @param[out] records Packed records
 * @param[out] rsizes Array of record sizes
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_load_packed(yk_database_handle_t dbh,
                                const char* collection,
                                int32_t mode,
                                size_t count,
                                const yk_id_t* ids,
                                size_t rbufsize,
                                void* records,
                                size_t* rsizes);

/**
 * @brief Low-level load operation based on a bulk handle.
 * This function will take the data in [offset, offset+size[
 * from the bulk handle and interpret it as follows:
 * - The first count * sizeof(size_t) bytes store the record sizes.
 * - The next N bytes store records back to back, where
 *   N = sum of record sizes
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
 * contiguous buffer in which packed records are meant to be stored
 * (in which case the record sizes do not matter as an input), or
 * if individual buffers were exposed to hold each records (in which case
 * the record sizes do matter as an input).
 *
 * @param dbh Database handle
 * @param collection Collection name
 * @param mode Mode
 * @param count Number of records
 * @param ids Record ids
 * @param origin Origin address
 * @param data Bulk handle containing record sizes and data
 * @param offset Offset in the bulk handle
 * @param size Size of the bulk handle (from offset)
 * @param packed Whether records are packed
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_load_bulk(yk_database_handle_t dbh,
                              const char* collection,
                              int32_t mode,
                              size_t count,
                              const yk_id_t* ids,
                              const char* origin,
                              hg_bulk_t data,
                              size_t offset,
                              size_t size,
                              bool packed);

/**
 * @brief Get the number of records currently stored in the
 * collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[out] count Number of records stored
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_size(yk_database_handle_t dbh,
                         const char* collection,
                         int32_t mode,
                         size_t* count);

/**
 * @brief Get the last record id of the collection.
 * This value corresponds to the id of the next record
 * that will be stored in the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[out] id Last record id
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_last_id(yk_database_handle_t dbh,
                            const char* collection,
                            int32_t mode,
                            yk_id_t* id);

#if 0
/**
 * @brief Fetch a record from the collection. The response
 * argument must have been allocated using yk_response_alloc.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] id Id of the record to fetch
 * @param[in] response Response from which to extract the record
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_fetch(yk_database_handle_t dbh,
                          const char* collection,
                          int32_t mode,
                          yk_id_t id,
                          yk_response_t response);

/**
 * @brief Fetch multiple records from the collection.
 * The response argument must have been allocated using
 * yk_response_alloc.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of records to fetch
 * @param[in] ids Ids of the records to fetch
 * @param[in] response Response from which to extract the records
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_fetch_multi(yk_database_handle_t dbh,
                                const char* collection,
                                int32_t mode,
                                size_t count,
                                const yk_id_t* ids,
                                yk_response_t response);

/**
 * @brief Filter the collection, returning up to max records
 * that match specified filter, starting from the given start_id.
 * The type of filter used is specified by the mode parameter.
 * By default (YOKAN_MODE_DEFAULT), or if filter is NULL and
 * fsize is 0, all the records are returned (same as
 * yk_coll_fetch_multi for the range of ids
 * [ start_id, start_id + max_records ]).
 * The response argument must have been allocated using
 * yk_response_alloc.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] start_id Start record id
 * @param[in] max_records Max number of records to return
 * @param[in] filter Filter data
 * @param[in] fsize Size of the filter
 * @param[in] response Resulting response handle
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_filter(yk_database_handle_t dbh,
                           const char* collection,
                           int32_t mode,
                           yk_id_t start_id,
                           size_t max_records,
                           const void* filter,
                           size_t fsize,
                           yk_response_t response);
#endif

/**
 * @brief Update a record from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] id Record id
 * @param[in] record New record content
 * @param[in] size Size of the record
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_update(yk_database_handle_t dbh,
                           const char* collection,
                           int32_t mode,
                           yk_id_t id,
                           const void* record,
                           size_t size);

/**
 * @brief Update multiple records.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of records to update
 * @param[in] ids Ids of the records to update
 * @param[in] records Array of pointers to record data
 * @param[in] rsizes Array of record sizes
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_update_multi(yk_database_handle_t dbh,
                                 const char* collection,
                                 int32_t mode,
                                 size_t count,
                                 const yk_id_t* ids,
                                 const void* const* records,
                                 const size_t* rsizes);

/**
 * @brief Update multiple records that are contiguous in memory.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of records to update
 * @param[in] ids Ids of the records to update
 * @param[in] records Contiguous buffer of records
 * @param[in] rsizes Array of record sizes
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_update_packed(yk_database_handle_t dbh,
                                  const char* collection,
                                  int32_t mode,
                                  size_t count,
                                  const yk_id_t* ids,
                                  const void* records,
                                  const size_t* rsizes);

/**
 * @brief Low-level version of update that takes an already
 * create bulk handle. The bulk handle is interpreted the same
 * way as in yk_coll_store_bulk.
 *
 * @param dbh Database handle
 * @param name Collection name
 * @param mode Mode
 * @param count Number of records
 * @param ids Record ids
 * @param origin Origin address
 * @param data Bulk containing data
 * @param offset Offset in the bulk in which the data is
 * @param size Size of the bulk region (after offset)
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_update_bulk(yk_database_handle_t dbh,
                                           const char* name,
                                           int32_t mode,
                                           size_t count,
                                           const yk_id_t* ids,
                                           const char* origin,
                                           hg_bulk_t data,
                                           size_t offset,
                                           size_t size);

/**
 * @brief Erase a record from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] id Id of the record to erase
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_erase(yk_database_handle_t dbh,
                          const char* collection,
                          int32_t mode,
                          yk_id_t id);

/**
 * @brief Erase multiple records from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of records to erase
 * @param[in] ids Ids of the records to erase
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_coll_erase_multi(yk_database_handle_t dbh,
                                const char* collection,
                                int32_t mode,
                                size_t count,
                                const yk_id_t* ids);

#ifdef __cplusplus
}
#endif

#endif
