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
yk_return_t yk_collection_create(yk_database_handle_t dbh,
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
yk_return_t yk_collection_drop(yk_database_handle_t dbh,
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
yk_return_t yk_collection_exists(yk_database_handle_t dbh,
                                 int32_t mode,
                                 const char* collection,
                                 uint8_t* flag);

/**
 * @brief Store a document into the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] document Record to store
 * @param[in] size Size of the document
 * @param[out] id Resulting document id
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_store(yk_database_handle_t dbh,
                          const char* collection,
                          int32_t mode,
                          const void* document,
                          size_t size,
                          yk_id_t* id);

/**
 * @brief Store multiple documents into the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of documents to store
 * @param[in] documents Array of documents
 * @param[in] rsizes Array of document sizes
 * @param[out] ids Resulting document ids
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_store_multi(yk_database_handle_t dbh,
                               const char* collection,
                               int32_t mode,
                               size_t count,
                               const void* const* documents,
                               const size_t* rsizes,
                               yk_id_t* ids);

/**
 * @brief Same as yk_doc_store_multi but the documents
 * are packed contiguously in memory.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of documents to store
 * @param[in] documents Packed documents
 * @param[in] rsizes Array of document sizes
 * @param[out] ids Resulting document ids
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_store_packed(yk_database_handle_t dbh,
                                const char* collection,
                                int32_t mode,
                                size_t count,
                                const void* documents,
                                const size_t* rsizes,
                                yk_id_t* ids);

/**
 * @brief Load a document from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[int] id Record id
 * @param[out] data Buffer to load the document
 * @param[inout] size Size of the buffer (in) / document (out)
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_load(yk_database_handle_t dbh,
                        const char* collection,
                        int32_t mode,
                        yk_id_t id,
                        void* data,
                        size_t* size);

/**
 * @brief Load multiple documents from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of documents to store
 * @param[in] ids Record ids
 * @param[out] documents Array of documents
 * @param[inout] rsizes Array of buffer/document sizes
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_load_multi(yk_database_handle_t dbh,
                               const char* collection,
                               int32_t mode,
                               size_t count,
                               const yk_id_t* ids,
                               void* const* documents,
                               size_t* rsizes);

/**
 * @brief Same as yk_doc_load_multi but the documents
 * are packed contiguously in memory.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of documents to store
 * @param[in] ids Record ids
 * @param[in] rbufsize Record buffer size
 * @param[out] documents Packed documents
 * @param[out] rsizes Array of document sizes
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_load_packed(yk_database_handle_t dbh,
                                const char* collection,
                                int32_t mode,
                                size_t count,
                                const yk_id_t* ids,
                                size_t rbufsize,
                                void* documents,
                                size_t* rsizes);

/**
 * @brief Low-level load operation based on a bulk handle.
 * This function will take the data in [offset, offset+size[
 * from the bulk handle and interpret it as follows:
 * - The first count * sizeof(size_t) bytes store the document sizes.
 * - The next N bytes store documents back to back, where
 *   N = sum of document sizes
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
 * contiguous buffer in which packed documents are meant to be stored
 * (in which case the document sizes do not matter as an input), or
 * if individual buffers were exposed to hold each documents (in which case
 * the document sizes do matter as an input).
 *
 * @param dbh Database handle
 * @param collection Collection name
 * @param mode Mode
 * @param count Number of documents
 * @param ids Record ids
 * @param origin Origin address
 * @param data Bulk handle containing document sizes and data
 * @param offset Offset in the bulk handle
 * @param size Size of the bulk handle (from offset)
 * @param packed Whether documents are packed
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_load_bulk(yk_database_handle_t dbh,
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
 * @brief Get the size of a document from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[int] id Record id
 * @param[out] size Size document
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_size(yk_database_handle_t dbh,
                        const char* collection,
                        int32_t mode,
                        yk_id_t id,
                        size_t* size);

/**
 * @brief Get the size of multiple documents from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of documents to store
 * @param[in] ids Record ids
 * @param[out] rsizes Array of document sizes
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_size_multi(yk_database_handle_t dbh,
                              const char* collection,
                              int32_t mode,
                              size_t count,
                              const yk_id_t* ids,
                              size_t* rsizes);

/**
 * @brief Get the number of documents currently stored in the
 * collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[out] count Number of documents stored
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_collection_size(yk_database_handle_t dbh,
                               const char* collection,
                               int32_t mode,
                               size_t* count);

/**
 * @brief Get the last document id of the collection.
 * This value corresponds to the id of the next document
 * that will be stored in the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[out] id Last document id
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_collection_last_id(yk_database_handle_t dbh,
                                  const char* collection,
                                  int32_t mode,
                                  yk_id_t* id);

#if 0
/**
 * @brief Fetch a document from the collection. The response
 * argument must have been allocated using yk_response_alloc.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] id Id of the document to fetch
 * @param[in] response Response from which to extract the document
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_fetch(yk_database_handle_t dbh,
                         const char* collection,
                         int32_t mode,
                         yk_id_t id,
                         yk_response_t response);

/**
 * @brief Fetch multiple documents from the collection.
 * The response argument must have been allocated using
 * yk_response_alloc.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of documents to fetch
 * @param[in] ids Ids of the documents to fetch
 * @param[in] response Response from which to extract the documents
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_fetch_multi(yk_database_handle_t dbh,
                               const char* collection,
                               int32_t mode,
                               size_t count,
                               const yk_id_t* ids,
                               yk_response_t response);

/**
 * @brief Filter the collection, returning up to max documents
 * that match specified filter, starting from the given start_id.
 * The type of filter used is specified by the mode parameter.
 * By default (YOKAN_MODE_DEFAULT), or if filter is NULL and
 * fsize is 0, all the documents are returned (same as
 * yk_doc_fetch_multi for the range of ids
 * [ start_id, start_id + max_documents ]).
 * The response argument must have been allocated using
 * yk_response_alloc.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] start_id Start document id
 * @param[in] max_documents Max number of documents to return
 * @param[in] filter Filter data
 * @param[in] fsize Size of the filter
 * @param[in] response Resulting response handle
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_filter(yk_database_handle_t dbh,
                          const char* collection,
                          int32_t mode,
                          yk_id_t start_id,
                          size_t max_documents,
                          const void* filter,
                          size_t fsize,
                          yk_response_t response);
#endif

/**
 * @brief Update a document from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] id Record id
 * @param[in] document New document content
 * @param[in] size Size of the document
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_update(yk_database_handle_t dbh,
                          const char* collection,
                          int32_t mode,
                          yk_id_t id,
                          const void* document,
                          size_t size);

/**
 * @brief Update multiple documents.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of documents to update
 * @param[in] ids Ids of the documents to update
 * @param[in] documents Array of pointers to document data
 * @param[in] rsizes Array of document sizes
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_update_multi(yk_database_handle_t dbh,
                                const char* collection,
                                int32_t mode,
                                size_t count,
                                const yk_id_t* ids,
                                const void* const* documents,
                                const size_t* rsizes);

/**
 * @brief Update multiple documents that are contiguous in memory.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of documents to update
 * @param[in] ids Ids of the documents to update
 * @param[in] documents Contiguous buffer of documents
 * @param[in] rsizes Array of document sizes
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_update_packed(yk_database_handle_t dbh,
                                 const char* collection,
                                 int32_t mode,
                                 size_t count,
                                 const yk_id_t* ids,
                                 const void* documents,
                                 const size_t* rsizes);

/**
 * @brief Low-level version of update that takes an already
 * create bulk handle. The bulk handle is interpreted the same
 * way as in yk_doc_store_bulk.
 *
 * @param dbh Database handle
 * @param name Collection name
 * @param mode Mode
 * @param count Number of documents
 * @param ids Record ids
 * @param origin Origin address
 * @param data Bulk containing data
 * @param offset Offset in the bulk in which the data is
 * @param size Size of the bulk region (after offset)
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_update_bulk(yk_database_handle_t dbh,
                               const char* name,
                               int32_t mode,
                               size_t count,
                               const yk_id_t* ids,
                               const char* origin,
                               hg_bulk_t data,
                               size_t offset,
                               size_t size);

/**
 * @brief Erase a document from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] id Id of the document to erase
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_erase(yk_database_handle_t dbh,
                         const char* collection,
                         int32_t mode,
                         yk_id_t id);

/**
 * @brief Erase multiple documents from the collection.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of documents to erase
 * @param[in] ids Ids of the documents to erase
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_erase_multi(yk_database_handle_t dbh,
                               const char* collection,
                               int32_t mode,
                               size_t count,
                               const yk_id_t* ids);

#ifdef __cplusplus
}
#endif

#endif