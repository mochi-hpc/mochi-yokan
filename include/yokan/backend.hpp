/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_BACKEND_H
#define __YOKAN_BACKEND_H

#include <limits>
#include <vector>
#include <string>
#include <unordered_map>
#include <functional>
#include <stdexcept>
#include <memory>
#include <list>
#include <yokan/common.h>
#include <yokan/usermem.hpp>
#include <yokan/filters.hpp>
#include <yokan/migration.hpp>

template <typename T> class __YOKANBackendRegistration;

namespace yokan {

/**
 * @brief The BitField class is used for the "exists" operations
 * to expose user memory with bitwise operations.
 */
struct BitField {
    uint8_t* data = nullptr; /*!< Pointer to the data */
    size_t   size = 0;       /*!< Number of bits in the bitfield */

    struct BitFieldAccessor {
        uint8_t* data;
        uint8_t  mask;

        operator bool() const {
            return *data & mask;
        }

        BitFieldAccessor& operator=(bool b) {
            if(b) *data |= mask;
            else *data &= ~mask;
            return *this;
        }
    };

    template<typename I>
    inline BitFieldAccessor operator[](I index) {
        uint8_t mask = 1 << (index % 8);
        return BitFieldAccessor{data + (index/8), mask};
    }
};

/**
 * @brief Status returned by all the backend functions.
 */
enum class Status : uint8_t {
    OK           = YOKAN_SUCCESS,
    InvalidType  = YOKAN_ERR_INVALID_BACKEND,
    InvalidConf  = YOKAN_ERR_INVALID_CONFIG,
    InvalidArg   = YOKAN_ERR_INVALID_ARGS,
    InvalidID    = YOKAN_ERR_INVALID_ID,
    NotFound     = YOKAN_ERR_KEY_NOT_FOUND,
    SizeError    = YOKAN_ERR_BUFFER_SIZE,
    KeyExists    = YOKAN_ERR_KEY_EXISTS,
    NotSupported = YOKAN_ERR_OP_UNSUPPORTED,
    Corruption   = YOKAN_ERR_CORRUPTION,
    IOError      = YOKAN_ERR_IO,
    Incomplete   = YOKAN_ERR_INCOMPLETE,
    TimedOut     = YOKAN_ERR_TIMEOUT,
    Aborted      = YOKAN_ERR_ABORTED,
    Busy         = YOKAN_ERR_BUSY,
    Expired      = YOKAN_ERR_EXPIRED,
    TryAgain     = YOKAN_ERR_TRY_AGAIN,
    System       = YOKAN_ERR_SYSTEM,
    Canceled     = YOKAN_ERR_CANCELED,
    Permission   = YOKAN_ERR_PERMISSION,
    InvalidMode  = YOKAN_ERR_MODE,
    Migrated     = YOKAN_ERR_MIGRATED,
    Other        = YOKAN_ERR_OTHER
};

/**
 * @brief Size used for a UserMem value when the key was not found.
 */
constexpr auto KeyNotFound = YOKAN_KEY_NOT_FOUND;

/**
 * @brief Size used for a UserMem value when the provided buffer
 * was too small to hold the value.
 */
constexpr auto BufTooSmall = YOKAN_SIZE_TOO_SMALL;

/**
 * @brief Abstract embedded database object.
 */
class DatabaseInterface {

    public:

    DatabaseInterface() = default;
    virtual ~DatabaseInterface() = default;
    DatabaseInterface(const DatabaseInterface&) = default;
    DatabaseInterface(DatabaseInterface&&) = default;
    DatabaseInterface& operator=(const DatabaseInterface&) = default;
    DatabaseInterface& operator=(DatabaseInterface&&) = default;

    /**
     * @brief Get the name of the backend (e.g. "map").
     *
     * @return The name of the backend.
     */
    virtual std::string type() const = 0;

    /**
     * @brief Get the internal configuration as a JSON-formatted string.
     *
     * @return Backend configuration.
     */
    virtual std::string config() const = 0;

    /**
     * @brief Destroy the resources (files, etc.) associated with the database.
     */
    virtual void destroy() = 0;


    /**
     * @brief Check if the backend supports the specified mode.
     *
     * @param mode Mode.
     *
     * @return true/false.
     */
    virtual bool supportsMode(int32_t mode) const {
        (void)mode;
        return false;
    }

    /**
     * @brief Check if the backend is sorted (list functions
     * will return keys in some defined order, either alphabetical
     * or custom).
     *
     * @return true/false.
     */
    virtual bool isSorted() const = 0;

    /**
     * @brief Get the number of key/value pairs stored.
     *
     * @param mode Mode.
     * @param c Result.
     *
     * @return Status.
     */
    virtual Status count(int32_t mode, uint64_t* c) const {
        (void)mode;
        (void)c;
        return Status::NotSupported;
    }

    /**
     * @brief Check if the provided keys exist. The keys are packed
     * into a single buffer. ksizes provides a pointer to the memory
     * holding the key sizes. The number of keys is conveyed by
     * ksizes.size and b.size, which should be equal (otherwise
     * Status::InvalidArg is returned).
     *
     * @param mode Mode.
     * @param keys Packed keys.
     * @param ksizes Packed key sizes.
     * @param b Memory segment holding booleans indicating whether each key exists.
     *
     * @return Status.
     */
    virtual Status exists(int32_t mode, const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BitField& b) const {
        (void)mode;
        (void)keys;
        (void)ksizes;
        (void)b;
        return Status::NotSupported;
    }

    /**
     * @brief Get the size of values associated with the keys. The keys are packed
     * into a single buffer. ksizes provides a pointer to the memory holding the
     * key sizes. vsizes provides a pointer to the memory where the value sizes need
     * to be stored. The number of keys is conveyed by ksizes.size and vsizes.size,
     * which should be equal (otherwise Status::InvalidArg is returned).
     *
     * @param mode Mode.
     * @param keys Packed keys.
     * @param ksizes Packed key sizes
     * @param b Memory segment to store value sizes.
     *
     * @return Status.
     */
    virtual Status length(int32_t mode,
                          const UserMem& keys,
                          const BasicUserMem<size_t>& ksizes,
                          BasicUserMem<size_t>& vsizes) const {
        (void)mode;
        (void)keys;
        (void)ksizes;
        (void)vsizes;
        return Status::NotSupported;
    }

    /**
     * @brief Put multiple key/value pairs into the database.
     * The keys, ksizes, values, and vsizes are packed into user-provided
     * memory segments. The number of key/value pairs is conveyed by
     * ksizes.size and vsizes.size, which should be equal.
     *
     * @param [in] mode Mode.
     * @param [in] keys Keys to put.
     * @param [in] ksizes Key sizes.
     * @param [in] vals Values to put.
     * @param [in] vsizes Value sizes.
     *
     * @return Status.
     */
    virtual Status put(int32_t mode,
                       const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       const UserMem& vals,
                       const BasicUserMem<size_t>& vsizes) {
        (void)mode;
        (void)keys;
        (void)vals;
        (void)ksizes;
        (void)vsizes;
        return Status::NotSupported;
    }

    /**
     * @brief Get values associated of keys.
     * vsizes is used both as input (to know where to place data in vals
     * and how much is available to each value) and as output (to store
     * the actual size of each value).
     *
     * This function expects (and will not check) that
     * - ksizes.size == vsizes.size
     * - the sum of ksizes <= keys.size
     * - the sum of vsizes <= vals.size
     *
     * Note: this function is not const because it can potentially
     * call erase() if a YOKAN_MODE_CONSUME is specified, for instance.
     *
     * @param mode Mode.
     * @param packed whether data is packed.
     * @param keys Keys to get.
     * @param ksizes Size of the keys.
     * @param vals Values to get.
     * @param vsizes Size of the values.
     *
     * @return Status.
     */
    virtual Status get(int32_t mode, bool packed, const UserMem& keys,
                       const BasicUserMem<size_t>& ksizes,
                       UserMem& vals,
                       BasicUserMem<size_t>& vsizes) {
        (void)mode;
        (void)packed;
        (void)keys;
        (void)vals;
        (void)ksizes;
        (void)vsizes;
        return Status::NotSupported;
    }

    using FetchCallback = std::function<Status(const UserMem& key, const UserMem& val)>;

    /**
     * @brief Get values associated of keys and pass them successively
     * to the provided callback function.
     *
     * Note: this function is not const because it can potentially
     * call erase() if a YOKAN_MODE_CONSUME is specified, for instance.
     *
     * @param mode Mode.
     * @param packed whether data is packed.
     * @param keys Keys to get.
     * @param ksizes Size of the keys.
     * @param func Function to call on the value.
     *
     * @return Status.
     */
    virtual Status fetch(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes,
                         const FetchCallback& func) {
        (void)mode;
        (void)keys;
        (void)ksizes;
        (void)func;
        return Status::NotSupported;
    }

    /**
     * @brief Erase a set of key/value pairs. Keys are packed into
     * a single buffer. The number of keys is conveyed by ksizes.size.
     *
     * @param [in] mode Mode.
     * @param [in] keys Keys to erase.
     * @param [in] ksizes Size of the keys.
     *
     * @return Status.
     */
    virtual Status erase(int32_t mode, const UserMem& keys,
                         const BasicUserMem<size_t>& ksizes) {
        (void)mode;
        (void)keys;
        (void)ksizes;
        return Status::NotSupported;
    }

    /**
     * @brief This version of listKeys uses a single contiguous buffer
     * to hold all the keys. Their size is stored in the keySizes user-allocated
     * buffer. After a successful call, keySizes.size holds the number of keys read.
     * The function will try to read up to keySizes.size keys.
     *
     * keySizes is considered an input and an output. As input, it provides the
     * size that should be used for each keys in the keys buffer. As an output,
     * it stores the actual size of each key.
     *
     * @param [in] mode Mode.
     * @param [in] packed Whether data is packed.
     * @param [in] fromKey Starting key.
     * @param [in] filter Key filter.
     * @param [out] keys Resulting keys.
     * @param [inout] keySizes Resulting key sizes.
     *
     * @return Status.
     */
    virtual Status listKeys(int32_t mode, bool packed, const UserMem& fromKey,
                            const std::shared_ptr<KeyValueFilter>& filter,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const {
        (void)mode;
        (void)packed;
        (void)fromKey;
        (void)filter;
        (void)keys;
        (void)keySizes;
        return Status::NotSupported;
    }

    /**
     * @brief Same as listKeys but also returns the values.
     */
    virtual Status listKeyValues(int32_t mode, bool packed,
                                 const UserMem& fromKey,
                                 const std::shared_ptr<KeyValueFilter>& filter,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const {
        (void)mode;
        (void)packed;
        (void)fromKey;
        (void)filter;
        (void)keys;
        (void)keySizes;
        (void)vals;
        (void)valSizes;
        return Status::NotSupported;
    }

    using IterCallback = std::function<Status(const UserMem& key, const UserMem& val)>;

    /**
     * @brief Iterate through the key/value pairs, calling the provided
     * function on each key/value pair.
     *
     * @param mode Mode.
     * @param max Max number of key/values pairs to list (0 to list everything).
     * @param fromKey Starting key.
     * @param filter Key filter.
     * @param func Function to call on each key.
     *
     * @return Status.
     */
    virtual Status iter(int32_t mode, uint64_t max, const UserMem& fromKey,
                        const std::shared_ptr<KeyValueFilter>& filter,
                        bool ignore_values,
                        const IterCallback& func) const {
        (void)max;
        (void)mode;
        (void)fromKey;
        (void)filter;
        (void)ignore_values;
        (void)func;
        return Status::NotSupported;
    }

    /**
     * @brief Create a collection in the underlying database.
     *
     * @param mode Mode
     * @param name Collection name
     *
     * @return Status
     */
    virtual Status collCreate(int32_t mode, const char* name) {
        (void)mode;
        (void)name;
        return Status::NotSupported;
    }

    /**
     * @brief Erase a collection from the underlying database.
     *
     * @param mode Mode
     * @param name Collection name
     *
     * @return Status
     */
    virtual Status collDrop(int32_t mode, const char* name) {
        (void)mode;
        (void)name;
        return Status::NotSupported;
    }

    /**
     * @brief Check if a collection exists in the underlying database.
     *
     * @param mode Mode
     * @param name Collection name
     * @param flag Set to true if the collection exists
     *
     * @return Status
     */
    virtual Status collExists(int32_t mode, const char* name, bool* flag) const {
        (void)mode;
        (void)name;
        (void)flag;
        return Status::NotSupported;
    }

    /**
     * @brief Get the last id in the collection (i.e. the id that the next document
     * stored will have).
     *
     * @param mode Mode
     * @param name Collection name
     * @param id Last id
     *
     * @return Status
     */
    virtual Status collLastID(int32_t mode, const char* name, yk_id_t* id) const {
        (void)mode;
        (void)name;
        (void)id;
        return Status::NotSupported;
    }

    /**
     * @brief Get the collection size (may be different from LastID if some
     * documents have been erased).
     *
     * @param mode Mode
     * @param name Collection name
     * @param size Size
     *
     * @return Status
     */
    virtual Status collSize(int32_t mode, const char* name, size_t* size) const {
        (void)mode;
        (void)name;
        (void)size;
        return Status::NotSupported;
    }

    /**
     * @brief Get the size of document associated with ids.
     *
     * @param collection Name of the collection.
     * @param mode Mode.
     * @param ids Buffer storing ids of the documents.
     * @param sizes Buffer in which to put sizes of the documents.
     *
     * @return Status.
     */
    virtual Status docSize(const char* collection,
                           int32_t mode,
                           const BasicUserMem<yk_id_t>& ids,
                           BasicUserMem<size_t>& sizes) const {
        (void)collection;
        (void)mode;
        (void)ids;
        (void)sizes;
        return Status::NotSupported;
    }

    /**
     * @brief Store multiple documents into the database.
     *
     * @param [in] collection Name of the collection.
     * @param [in] mode Mode.
     * @param [in] documents Buffer containing documents to put.
     * @param [in] size Buffer containing the sizes of documents.
     * @param [in] ids Buffer to store resulting document ids.
     *
     * @return Status.
     */
    virtual Status docStore(const char* collection,
                            int32_t mode,
                            const UserMem& documents,
                            const BasicUserMem<size_t>& sizes,
                            BasicUserMem<yk_id_t>& ids) {
        (void)collection;
        (void)mode;
        (void)documents;
        (void)sizes;
        (void)ids;
        return Status::NotSupported;
    }

    /**
     * @brief Update multiple documents in the database.
     *
     * @param [in] collection Name of the collection.
     * @param [in] mode Mode.
     * @param [in] ids Buffer containing document ids.
     * @param [in] documents Buffer containing documents to put.
     * @param [in] size Buffer containing the sizes of documents.
     *
     * @return Status.
     */
    virtual Status docUpdate(const char* collection,
                             int32_t mode,
                             const BasicUserMem<yk_id_t>& ids,
                             const UserMem& documents,
                             const BasicUserMem<size_t>& sizes) {
        (void)collection;
        (void)mode;
        (void)documents;
        (void)sizes;
        (void)ids;
        return Status::NotSupported;
    }

    /**
     * @brief Load documents associated of ids.
     * sizes is used both as input (to know where to place data in documents
     * and how much is available to each document) and as output (to store
     * the actual size of each value).
     *
     * @param collection Name of the collection.
     * @param mode Mode.
     * @param packed whether data is packed.
     * @param ids Buffer containing document ids.
     * @param documents Documents to get.
     * @param sizes Size of the documents.
     *
     * @return Status.
     */
    virtual Status docLoad(const char* collection,
                           int32_t mode, bool packed,
                           const BasicUserMem<yk_id_t>& ids,
                           UserMem& documents,
                           BasicUserMem<size_t>& sizes) {
        (void)collection;
        (void)mode;
        (void)packed;
        (void)ids;
        (void)documents;
        (void)sizes;
        return Status::NotSupported;
    }

    using DocFetchCallback = std::function<Status(yk_id_t, const UserMem& doc)>;

    /**
     * @brief Get the documents associated withe the ids and pass them successively
     * to the provided callback function.
     *
     * Note: this function is not const because it can potentially
     * call erase() if a YOKAN_MODE_CONSUME is specified, for instance.
     *
     * @param collection Collection name.
     * @param mode Mode.
     * @param ids Ids.
     * @param func Function to call on the documents.
     *
     * @return Status.
     */
    virtual Status docFetch(const char* collection,
                            int32_t mode, const BasicUserMem<yk_id_t>& ids,
                            const DocFetchCallback& func) {
        (void)collection;
        (void)mode;
        (void)ids;
        (void)func;
        return Status::NotSupported;
    }


    /**
     * @brief Erase a set of documents.
     *
     * @param [in] collection Collection name.
     * @param [in] mode Mode.
     * @param [in] ids Ids of the documents to erase.
     *
     * @return Status.
     */
    virtual Status docErase(const char* collection,
                            int32_t mode, const BasicUserMem<yk_id_t>& ids) {
        (void)mode;
        (void)ids;
        (void)collection;
        return Status::NotSupported;
    }

    /**
     * @brief List documents from the collection.
     *
     * @param[in] collection Collection
     * @param[in] mode Mode
     * @param[in] packed Whether data is packed in the document buffer
     * @param[in] from_id Starting document id
     * @param[in] filter Filter
     * @param[out] ids Resulting ids
     * @param[out] documents Resulting documents
     * @param[inout] doc_sizes Buffer sizes / Resulting document sizes
     *
     * @return Status.
     */
    virtual Status docList(const char* collection,
                           int32_t mode, bool packed,
                           yk_id_t from_id,
                           const std::shared_ptr<DocFilter>& filter,
                           BasicUserMem<yk_id_t>& ids,
                           UserMem& documents,
                           BasicUserMem<size_t>& doc_sizes) const {
        (void)collection;
        (void)mode;
        (void)filter;
        (void)from_id;
        (void)packed;
        (void)ids;
        (void)documents;
        (void)doc_sizes;
        return Status::NotSupported;
    }

    using DocIterCallback = std::function<Status(yk_id_t, const UserMem&)>;

    /**
     * @brief Iterate through the documents, calling the provided
     * function on each id/document pair.
     *
     * @param collection Collection
     * @param mode Mode.
     * @param max Max number of documents to list (0 to list everything).
     * @param from_id starting ID.
     * @param filter Document filter.
     * @param func Function to call on each key.
     *
     * @return Status.
     */
    virtual Status docIter(const char* collection,
                           int32_t mode, uint64_t max, yk_id_t from_id,
                           const std::shared_ptr<DocFilter>& filter,
                           const DocIterCallback& func) const {
        (void)collection;
        (void)max;
        (void)mode;
        (void)from_id;
        (void)filter;
        (void)func;
        return Status::NotSupported;
    }

    /**
     * @brief Set the provided unique_ptr to point to a MigrationHandle
     * that can be used by the provider to retrieve the files used by
     * the database.
     */
    virtual Status startMigration(std::unique_ptr<MigrationHandle>& mh) {
        (void)mh;
        return Status::NotSupported;
    }

};

/**
 * @brief The DatabaseFactory is used by the provider to build
 * key/value store instances of various types.
 */
class DatabaseFactory {

    template <typename T> friend class ::__YOKANBackendRegistration;

    public:

    DatabaseFactory() = delete;

    /**
     * @brief Create a DatabaseInterface object of a specified
     * type and return a pointer to it. It is the respondibility of
     * the user to call delete on this pointer.
     *
     * If the function fails, it will return a Status error.
     *
     * @param backendType Name of the backend.
     * @param jsonConfig Json-formatted configuration string.
     * @param kvs Created DatabaseInterface pointer.
     *
     * @return Status.
     */
    static Status makeDatabase(
            const std::string& backendType,
            const std::string& jsonConfig,
            DatabaseInterface** kvs) {
        if(!hasBackendType(backendType)) return Status::InvalidType;
        return make_fn[backendType](jsonConfig, kvs);
    }

    static Status recoverDatabase(
            const std::string& backendType,
            const std::string& dbConfig,
            const std::string& migrationConfig,
            const std::list<std::string>& files,
            DatabaseInterface** kvs) {
        if(!hasBackendType(backendType)) return Status::InvalidType;
        return recover_fn[backendType](dbConfig, migrationConfig, files, kvs);
    }

    /**
     * @brief Check if the backend type is available in the factory.
     *
     * @param backendType Name of the backend.
     *
     * @return whether the backend type is available.
     */
    static inline bool hasBackendType(const std::string& backendType) {
        return make_fn.count(backendType) != 0;
    }

    private:

    static std::unordered_map<
                std::string,
                std::function<Status(
                        const std::string&,
                        DatabaseInterface**)>>
        make_fn; /*!< Map of factory functions for each backend type */
    static std::unordered_map<
                std::string,
                std::function<Status(
                        const std::string&,
                        const std::string&,
                        const std::list<std::string>&,
                        DatabaseInterface**)>>
        recover_fn; /*!< Map of factory functions for each backend type */
};

}

/**
 * @brief This macro should be used to register new backend types.
 */
#define YOKAN_REGISTER_BACKEND(__backend_name, __backend_type) \
    static __YOKANBackendRegistration<__backend_type>          \
    __yk##__backend_name##_backend(#__backend_name)

template <typename T> class __YOKANBackendRegistration {


    template<typename ... U> using void_t = void;

    template<typename U> using recover_t =
        decltype(U::recover("", "", {}, nullptr));

    template<typename U, typename = void_t<>>
    struct has_recover : std::false_type {};

    template<typename U>
    struct has_recover<U, void_t<recover_t<U>>> : std::true_type {};

    template<typename U, typename ... Args>
    static auto call_recover(const std::true_type&, Args&&... args) {
        return U::recover(std::forward<Args>(args)...);
    }

    template<typename U, typename ... Args>
    static auto call_recover(const std::false_type&, Args&&...) {
        return yokan::Status::NotSupported;
    }

    public:

    __YOKANBackendRegistration(const std::string &backend_name) {
        ::yokan::DatabaseFactory::make_fn[backend_name] =
            [](const std::string &config, ::yokan::DatabaseInterface** kvs) {
                return T::create(config, kvs);
            };
        ::yokan::DatabaseFactory::recover_fn[backend_name] =
            [](const std::string &database_config,
               const std::string& migration_config,
               const std::list<std::string>& files,
               ::yokan::DatabaseInterface** kvs) {
                return call_recover<T>(has_recover<T>{}, database_config, migration_config, files, kvs);
            };
    }
};

using yk_database = ::yokan::DatabaseInterface;
using yk_database_t = ::yokan::DatabaseInterface*;

#endif
