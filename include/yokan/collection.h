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
 * @brief Type of callback used by the fetch and iter functions.
 *
 * @param void* User-provided arguments.
 * @param size_t Iteration index.
 * @param yk_id_t Record id
 * @param const void* Document data.
 * @param size_t Size of the document.
 *
 * @return YK_SUCCESS or other error code.
 */
typedef yk_return_t (*yk_document_callback_t)(void*, size_t, yk_id_t, const void*, size_t);

/**
 * @brief Type of callback used by the fetch_bulk and iter_bulk functions.
 * For fetch_bulk, the bulk handle will expose the count size_t document sizes
 * followed by the actual documents. For iter_bulk, it will contain the count yk_id_t
 * document IDs, then the sizes, then the documents.
 *
 * @param void* User-provided arguments.
 * @param size_t Start iteration index.
 * @param size_t Number of documents in the bulk.
 * @param hg_bulk_t Bulk containing the documents.
 * @param hg_addr_t Address the bulk originates from.
 * @param size_t Total size of the memory exposed by the bulk handle.
 *
 * @return YK_SUCCESS or other error code.
 */
typedef yk_return_t (*yk_document_bulk_callback_t)(void*, size_t, size_t, hg_bulk_t, hg_addr_t, size_t);

/**
 * @brief Create a collection in the specified database.
 *
 * @param[in] dbh Database handle
 * @param[in] name Name of the collection (null-terminated)
 * @param[in] mode Mode
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_collection_create(yk_database_handle_t dbh,
                                 const char* name,
                                 int32_t mode);

/**
 * @brief Erase the collection from the underlying database.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection name (null-terminated)
 * @param[in] mode Mode
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_collection_drop(yk_database_handle_t dbh,
                               const char* collection,
                               int32_t mode);

/**
 * @brief Check if the collection exists in the underlying database.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection name (null-terminated)
 * @param[in] mode Mode
 * @param[out] flag set to 1 if the collection exists, 0 otherwise
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_collection_exists(yk_database_handle_t dbh,
                                 const char* collection,
                                 int32_t mode,
                                 uint8_t* flag);

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
 * @brief Store a document using the low-level bulk handle.
 * The payload is considered from offset to offset+size in
 * the bulk handle. See src/client/doc_store.cpp for
 * information on how data in this buffer should be structured.
 *
 * @param dbh Database handle
 * @param collection Collection
 * @param mode Mode
 * @param count Number of records
 * @param origin Origin address
 * @param data Bulk handle
 * @param offset Offset in the bulk handle
 * @param size Size of the payload
 * @param ids Resulting document ids
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_store_bulk(yk_database_handle_t dbh,
                              const char* collection,
                              int32_t mode,
                              size_t count,
                              const char* origin,
                              hg_bulk_t data,
                              size_t offset,
                              size_t size,
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
 * @brief Fetch a document from the collection, calling a function
 * on the document when it is received.
 * This function is a version of yk_doc_load that does not need to
 * know the size of the document ahead of time.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] id Record id
 * @param[in] cb Callback to call on the document
 * @param[in] uargs Arguments for the callback
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_fetch(yk_database_handle_t dbh,
                         const char* collection,
                         int32_t mode,
                         yk_id_t id,
                         yk_document_callback_t cb,
                         void* uargs);

/**
 * @brief Options to provide to yk_doc_fetch_multi.
 */
typedef struct yk_doc_fetch_options {
    ABT_pool pool;       /* pool in which to run the callback */
    unsigned batch_size; /* documents are sent back in batches of this size */
} yk_doc_fetch_options_t;

/**
 * @brief Fetch documents from the collection, calling a function
 * on the documents when they are received.
 * This function is a version of yk_doc_load_multi that does not need to
 * know the size of the document ahead of time.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of ids
 * @param[in] ids Record ids
 * @param[in] cb Callback to call on the document
 * @param[in] uargs Arguments for the callback
 * @param[in] options Extra options
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_fetch_multi(yk_database_handle_t dbh,
                               const char* collection,
                               int32_t mode,
                               size_t count,
                               const yk_id_t* ids,
                               yk_document_callback_t cb,
                               void* uargs,
                               const yk_doc_fetch_options_t* options);

/**
 * @brief Fetch documents from the collection, calling a function
 * on the bulk handle containing the documents. This function will not pull
 * the documents from the received bulk handle, leaving the caller an
 * opportunity to forward it to another process.
 *
 * The bulk handle received will contain count*size_t document sizes,
 * followed by the actual documents.
 *
 * @param[in] dbh Database handle
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] count Number of ids
 * @param[in] ids Record ids
 * @param[in] cb Callback to call on the document
 * @param[in] uargs Arguments for the callback
 * @param[in] options Extra options
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_fetch_bulk(yk_database_handle_t dbh,
                              const char* collection,
                              int32_t mode,
                              size_t count,
                              const yk_id_t* ids,
                              yk_document_bulk_callback_t cb,
                              void* uargs,
                              const yk_doc_fetch_options_t* options);

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
yk_return_t yk_doc_length(yk_database_handle_t dbh,
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
yk_return_t yk_doc_length_multi(yk_database_handle_t dbh,
                                const char* collection,
                                int32_t mode,
                                size_t count,
                                const yk_id_t* ids,
                                size_t* rsizes);

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

/**
 * @brief List up to max documents starting at start_id
 * Contrary to yk_list_keys and yk_list_keyvals, for which the
 * filter is applied to the key, the filter here is applied to
 * the document's content. By default, the filter is ignored.
 * If fewer than max documents are returned, the extra doc_sizes
 * are set to YOKAN_NO_MORE_DOCS.
 *
 * Important: start_id is inclusive, whether YOKAN_MODE_INCLUSIVE
 * is set or not. To exclude it, simply start at start_id+1.
 *
 * @param[in] dbh Database handle.
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] start_id Starting document id
 * @param[in] filter Filter content
 * @param[in] filter_size Filter size
 * @param[in] max Maximum number of documents to return
 * @param[out] ids Ids of the documents returned
 * @param[out] docs Buffers in which to receive documents
 * @param[inout] doc_sizes Buffers/document sizes
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_list(yk_database_handle_t dbh,
                        const char* collection,
                        int32_t mode,
                        yk_id_t start_id,
                        const void* filter,
                        size_t filter_size,
                        size_t max,
                        yk_id_t* ids,
                        void* const* docs,
                        size_t* doc_sizes);

/**
 * Same as yk_doc_list but uses a single buffer to hold documents
 * back to back.
 *
 * @param[in] dbh Database handle.
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] start_id Starting document id
 * @param[in] filter Filter content
 * @param[in] filter_size Filter size
 * @param[in] max Maximum number of documents to return
 * @param[out] ids Ids of the documents returned
 * @param[in] bufsize Size of the document buffer
 * @param[out] docs Buffers in which to receive documents
 * @param[out] doc_sizes Document sizes
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_list_packed(yk_database_handle_t dbh,
                               const char* collection,
                               int32_t mode,
                               yk_id_t start_id,
                               const void* filter,
                               size_t filter_size,
                               size_t max,
                               yk_id_t* ids,
                               size_t bufsize,
                               void* docs,
                               size_t* doc_sizes);

/**
 * @brief Version of yk_doc_list that works on an already
 * formed bulk handle. This bulk handle should expose the filter,
 * then the array of doc sizes, then the array of ids,
 * then the buffer in which to store the documents.
 *
 * @param dbh Database handle
 * @param collection Collection
 * @param mode Mode
 * @param from_id Starting id
 * @param filter_size Filter size
 * @param origin Origina address (or NULL if calling process)
 * @param data Bulk handle
 * @param offset Offset in the bulk handle
 * @param docs_buf_size Total buffer size to store documents
 * @param packed Whether documents should be packed
 * @param count Maximum number of documents to return
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_list_bulk(yk_database_handle_t dbh,
                             const char* collection,
                             int32_t mode,
                             yk_id_t from_id,
                             size_t filter_size,
                             const char* origin,
                             hg_bulk_t data,
                             size_t offset,
                             size_t docs_buf_size,
                             bool packed,
                             size_t count);

/**
 * @brief Options to pass to yk_doc_iter.
 */
typedef struct yk_doc_iter_options {
    unsigned batch_size;
    ABT_pool pool;
} yk_doc_iter_options_t;

/**
 * @brief Iterate through up to max documents starting at start_id,
 * calling the callback on each document.
 *
 * Important: start_id is inclusive, whether YOKAN_MODE_INCLUSIVE
 * is set or not. To exclude it, simply start at start_id+1.
 *
 * @param[in] dbh Database handle.
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] start_id Starting document id
 * @param[in] filter Filter content
 * @param[in] filter_size Filter size
 * @param[in] max Maximum number of documents to return
 * @param[in] cb Callback to call on each document
 * @param[in] uargs Arguments for the callback
 * @param[in] options Options
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_iter(yk_database_handle_t dbh,
                        const char* collection,
                        int32_t mode,
                        yk_id_t start_id,
                        const void* filter,
                        size_t filter_size,
                        size_t max,
                        yk_document_callback_t cb,
                        void* uargs,
                        const yk_doc_iter_options_t* options);

/**
 * @brief Iterate through up to max documents starting at start_id,
 * calling the callback on the bulk handle holding the documents.
 *
 * @param[in] dbh Database handle.
 * @param[in] collection Collection
 * @param[in] mode Mode
 * @param[in] start_id Starting document id
 * @param[in] filter Filter content
 * @param[in] filter_size Filter size
 * @param[in] max Maximum number of documents to return
 * @param[in] cb Callback to call on the bulk
 * @param[in] uargs Arguments for the callback
 * @param[in] options Options
 *
 * @return YOKAN_SUCCESS or error code defined in common.h
 */
yk_return_t yk_doc_iter_bulk(yk_database_handle_t dbh,
                             const char* collection,
                             int32_t mode,
                             yk_id_t start_id,
                             const void* filter,
                             size_t filter_size,
                             size_t max,
                             yk_document_bulk_callback_t cb,
                             void* uargs,
                             const yk_doc_iter_options_t* options);


#ifdef __cplusplus
}
#endif

#endif
