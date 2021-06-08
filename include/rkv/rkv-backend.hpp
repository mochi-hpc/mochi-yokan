/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_BACKEND_H
#define __RKV_BACKEND_H

#include <limits>
#include <vector>
#include <string>
#include <unordered_map>
#include <functional>
#include <rkv/rkv-common.h>

template <typename T> class __RKVBackendRegistration;

namespace rkv {

/**
 * @brief Wrapper for user memory (equivalent to
 * some backend's notion of Slice, or to a C++17 std::string_view)
 *
 * @tparam T Type of data held.
 */
template<typename T>
struct BasicUserMem {
    T*     data = nullptr; /*!< Pointer to the data */
    size_t size = 0;       /*!< Number of elements of type T in the buffer */
};

/**
 * @brief UserMem is short for BasicUserMem<void>.
 * Its size field represents a number of bytes for
 * a buffer of unspecified type.
 */
using UserMem = BasicUserMem<void>;

/**
 * @brief Status returned by all the backend functions.
 */
enum class Status : uint8_t {
    OK          = RKV_SUCCESS,
    InvalidArg  = RKV_ERR_INVALID_ARGS,
    NotFound    = RKV_ERR_KEY_NOT_FOUND,
    SizeError   = RKV_ERR_BUFFER_SIZE,
    KeyExists   = RKV_ERR_KEY_EXISTS
};

/**
 * @brief Size used for a UserMem value when the key was not found.
 */
constexpr auto KeyNotFound = std::numeric_limits<size_t>::max();

/**
 * @brief Size used for a UserMem value when the provided buffer
 * was too small to hold the value.
 */
constexpr auto BufTooSmall = std::numeric_limits<size_t>::max()-1;

/**
 * @brief Abstract embedded key/value storage object.
 */
class KeyValueStoreInterface {

    public:

    KeyValueStoreInterface() = default;
    virtual ~KeyValueStoreInterface() = default;
    KeyValueStoreInterface(const KeyValueStoreInterface&) = default;
    KeyValueStoreInterface(KeyValueStoreInterface&&) = default;
    KeyValueStoreInterface& operator=(const KeyValueStoreInterface&) = default;
    KeyValueStoreInterface& operator=(KeyValueStoreInterface&&) = default;

    /**
     * @brief Get the name of the backend (e.g. "map").
     *
     * @return The name of the backend.
     */
    virtual std::string name() const = 0;

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
     * @brief Check if the provided key exists.
     *
     * @param [in] key Key.
     * @param [out] b Boolean indicating whether the key exists.
     *
     * @return Status.
     */
    virtual Status exists(const UserMem& key, bool& b) const = 0;

    /**
     * @brief Check if the provided keys exist. The provided output vector
     * will be resized to match keys.size().
     *
     * @param [in] keys Keys.
     * @param [out] b Vector of booleans indicating whether each key exists.
     *
     * @return Status.
     */
    virtual Status existsMulti(const std::vector<UserMem>& keys,
                               std::vector<bool>& b) const = 0;

    /**
     * @brief Get the length of the value associated with the provided key.
     * If the key does not exist, the size is set to KeyNotFound
     * and the function returns Status::NotFound.
     *
     * @param [in] key Key.
     * @param [out] size Size of the value.
     *
     * @return Status.
     */
    virtual Status length(const UserMem& key, size_t& size) const = 0;

    /**
     * @brief Get the length of the values associated with the provided keys.
     * If any key does not exists, the corresponding size will be set to
     * KeyNotFound. Keys that have been found will have valid value sizes.
     * The size vector will be resized to keys.size().
     *
     * @param [in] keys Keys.
     * @param [out] size  Sizes of values.
     *
     * @return Status.
     */
    virtual Status lengthMulti(const std::vector<UserMem>& keys,
                               std::vector<size_t>& size) const = 0;

    /**
     * @brief Put a new key/value pair into the database.
     *
     * @param [in] key Key to put.
     * @param [in] value Value to put.
     *
     * @return Status.
     */
    virtual Status put(const UserMem& key, const UserMem& value) = 0;

    /**
     * @brief Put multiple key/value pairs into the database.
     * The two provided vectors must have the same size, otherwise
     * none of the key/value pairs is inserted and Status::InvalidArg
     * is returned instead.
     *
     * @param [in] keys Keys to put.
     * @param [in] vals Values to put.
     *
     * @return Status.
     */
    virtual Status putMulti(const std::vector<UserMem>& keys,
                            const std::vector<UserMem>& vals) = 0;

    /**
     * @brief Get the value associated with a given key.
     * The user must provide the memory into which to store the value.
     * Upon successfully copying the value into the user-provided memory,
     * the value.size is set to the size of the value.
     * If the value does not supply enough memory, SizeError is returned
     * and the size of value is set to the required size.
     *
     * @param [in] key Key.
     * @param [out] value Value.
     *
     * @return Status.
     */
    virtual Status get(const UserMem& key, UserMem& value) const = 0;

    /**
     * @brief Get values associated with multiple keys. The keys and
     * values vectors must be the same size, otherwise none of the
     * values will be read and the function will return Status::InvalidArg.
     * For each value correctly found, if the provided UserMem has sufficient
     * space, the value's data will be copied into the UserMem's data field
     * and the UserMem's size will be changed to the value size. If the
     * provided memory is insufficient, the value will not be copied
     * and the size of the UserMem will be set to BufTooSmall. If the key
     * was not found, the value's UserMem will be set to KeyNotFound.
     *
     * @param [in] keys Keys to get.
     * @param [out] values Values.
     *
     * @return Status.
     */
    virtual Status getMulti(const std::vector<UserMem>& keys,
                            std::vector<UserMem>& values) const = 0;

    /**
     * @brief Get the value associated with a given key.
     * This version uses an std::string instead of a UserMem so the
     * size of the value does not need to be known in advance.
     *
     * @param [in] key Key.
     * @param [out] value Value.
     *
     * @return Status.
     */
    virtual Status get(const UserMem& key, std::string& value) const = 0;

    /**
     * @brief Get values associated with multiple keys.
     * This version uses an std::string instead of a UserMem so the
     * size of the values do not need to be known in advance.
     * The keys and values vectors do not need to have the same size,
     * the value vector will be resized as needed. Since this version
     * of getMulti does not enable setting the value size to KeyNotFound
     * if the key is not found, the defaultValue argument is used for
     * any value whose key is not found.
     *
     * @param [in] keys Array of keys.
     * @param [out] values Array of values.
     * @param [in] defaultValue Value for keys not found.
     *
     * @return Status.
     */
    virtual Status getMulti(const std::vector<UserMem>& keys,
                            std::vector<std::string>& values,
                            const std::string& defaultValue = "") const = 0;

    /**
     * @brief Erase a key/value pair identified by the specified key.
     *
     * @param [in] key Key to erase.
     *
     * @return Status.
     */
    virtual Status erase(const UserMem& key) = 0;

    /**
     * @brief Erase a set of key/value pairs.
     *
     * @param [in] keys Keys to erase.
     *
     * @return Status.
     */
    virtual Status eraseMulti(const std::vector<UserMem>& keys) = 0;

    /**
     * @brief Lists up to max keys from fromKey (included
     * if inclusive is set to true), returning only the keys with the provided
     * prefix. Using UserMem::Null for fromKey will make the list start from
     * the beginning.
     * Using a UserMem of size 0 for the prefix makes the function ignore the prefix.
     *
     * Note: fromKey does not need to be a key that exists in the database.
     * It is used as lower bound.
     *
     * @param [in] fromKey Starting key.
     * @param [in] inclusive Whether to include the start key if found.
     * @param [in] prefix Prefix to filter with.
     * @param [in] max Maximum number of keys to return.
     * @param [out] keys Resulting keys.
     *
     * @return Status.
     */
    virtual Status listKeys(const UserMem& fromKey,
                            bool inclusive, const UserMem& prefix,
                            size_t max, std::vector<std::string>& keys) const = 0;

    /**
     * @brief This version of listKeys uses a vector of UserMem instead of
     * a vector of strings to hold the resulting keys. The maximum number
     * of keys is not specified since the size of the provided vector
     * gives this information. The vector will be resized down to match
     * the number of keys found. If the provided UserMem objects in the
     * keys vector don't provide enough space, this function will set their
     * size field to BufTooSmall.
     *
     * @param [in] fromKey Starting key.
     * @param [in] inclusive Whether to include the start key if found.
     * @param [in] prefix Prefix to filter with.
     * @param [out] keys Resulting keys.
     *
     * @return Status.
     */
    virtual Status listKeys(const UserMem& fromKey,
                            bool inclusive, const UserMem& prefix,
                            std::vector<UserMem>& keys) const = 0;

    /**
     * @brief This version of listKeys uses a single contiguous buffer
     * to hold all the keys. Their size is stored in the keySizes user-allocated
     * buffer. After a successful call, keySizes.size holds the number of keys read.
     * The function will try to read up to keySizes.size keys.
     *
     * @param [in] fromKey Starting key.
     * @param [in] inclusive Whether to include the start key if found.
     * @param [in] prefix Prefix to filter with.
     * @param [out] keys Resulting keys.
     * @param [out] keySizes Resulting key sizes.
     *
     * @return Status.
     */
    virtual Status listKeys(const UserMem& fromKey,
                            bool inclusive, const UserMem& prefix,
                            UserMem& keys, BasicUserMem<size_t>& keySizes) const = 0;

    /**
     * @brief Same as listKeys but also returns the values.
     */
    virtual Status listKeyValues(const UserMem& fromKey,
                                 bool inclusive, const UserMem& prefix, size_t max,
                                 std::vector<std::string>& keys,
                                 std::vector<std::string>& vals) const = 0;

    /**
     * @brief Same as listKeys but also returns the values.
     */
    virtual Status listKeyValues(const UserMem& fromKey,
                                 bool inclusive, const UserMem& prefix,
                                 std::vector<UserMem>& keys, std::vector<UserMem>& vals) const = 0;

    /**
     * @brief Same as listKeys but also returns the values.
     */
    virtual Status listKeyValues(const UserMem& fromKey,
                                 bool inclusive, const UserMem& prefix,
                                 UserMem& keys,
                                 BasicUserMem<size_t>& keySizes,
                                 UserMem& vals,
                                 BasicUserMem<size_t>& valSizes) const = 0;
};

/**
 * @brief The KeyValueStoreFactory is used by the provider to build
 * key/value store instances of various types.
 */
class KeyValueStoreFactory {

    template <typename T> friend class ::__RKVBackendRegistration;

    public:

    KeyValueStoreFactory() = delete;

    /**
     * @brief Create a KeyValueStoreInterface object of a specified
     * type and return a pointer to it. It is the respondibility of
     * the user to call delete on this pointer.
     *
     * If the function fails, it will return a nullptr.
     *
     * @param backendType Name of the backend.
     * @param jsonConfig Json-formatted configuration string.
     *
     * @return KeyValueStoreInterface pointer.
     */
    static KeyValueStoreInterface* makeKeyValueStore(
            const std::string& backendType,
            const std::string& jsonConfig) {
        if(!hasBackendType(backendType))
            return nullptr;
        return make_fn[backendType](jsonConfig);
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
                std::function<KeyValueStoreInterface*(const std::string&)>>
        make_fn; /*!< Map of factory functions for each backend type */
};

}

/**
 * @brief This macro should be used to register new backend types.
 */
#define RKV_REGISTER_BACKEND(__backend_name, __backend_type) \
    static __RKVBackendRegistration<__backend_type>          \
    __rkv##__backend_name##_backend(#__backend_name)

template <typename T> class __RKVBackendRegistration {

    public:

    __RKVBackendRegistration(const std::string &backend_name) {
        rkv::KeyValueStoreFactory::make_fn[backend_name] =
            [](const std::string &config) {
                return T::create(config);
            };
    }
};

using rkv_database = rkv::KeyValueStoreInterface;
using rkv_database_t = rkv::KeyValueStoreInterface*;

#endif
