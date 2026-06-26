/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_DATABASE_HPP
#define __YOKAN_DATABASE_HPP

#include <yokan/database.h>
#include <yokan/collection.h>
#include <yokan/cxx/exception.hpp>
#include <yokan/cxx/extras.hpp>
#include <vector>
#include <functional>
#include <memory>
#include <utility>

namespace yokan {

class Client;
class Collection;

class Database {

    friend class Client;
    friend class Collection;

    public:

    Database() = default;

    Database(yk_database_handle_t db,
             bool copy=true,
             std::shared_ptr<yk_client> owner = nullptr)
    : m_client{std::move(owner)}
    , m_db{db, yk_database_handle_release} {
        if(copy) {
            auto err = yk_database_handle_ref_incr(handle());
            YOKAN_CONVERT_AND_THROW(err);
        }
    }

    Database(const Database& other) = default;

    Database(Database&& other) = default;

    Database& operator=(const Database& other) = default;

    Database& operator=(Database&& other) = default;

    ~Database() = default;

    template <typename... Extras>
    size_t count(int32_t mode = YOKAN_MODE_DEFAULT, Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        size_t c;
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_count(handle(), mode, &c);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_count(handle(), mode | YOKAN_MODE_EXTRA, &c,
                           YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                           YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
        return c;
    }

    template <typename... Extras>
    void put(const void* key,
             size_t ksize,
             const void* value,
             size_t vsize,
             int32_t mode = YOKAN_MODE_DEFAULT,
             Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_put(handle(), mode, key, ksize, value, vsize);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_put(handle(), mode | YOKAN_MODE_EXTRA,
                         key, ksize, value, vsize,
                         YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                         YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void putMulti(size_t count,
                  const void* const* keys,
                  const size_t* ksizes,
                  const void* const* values,
                  const size_t* vsizes,
                  int32_t mode = YOKAN_MODE_DEFAULT,
                  Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_put_multi(handle(), mode, count,
                keys, ksizes, values, vsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_put_multi(handle(), mode | YOKAN_MODE_EXTRA, count,
                keys, ksizes, values, vsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void putPacked(size_t count,
                   const void* keys,
                   const size_t* ksizes,
                   const void* values,
                   const size_t* vsizes,
                   int32_t mode = YOKAN_MODE_DEFAULT,
                   Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_put_packed(handle(), mode, count,
                keys, ksizes, values, vsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_put_packed(handle(), mode | YOKAN_MODE_EXTRA, count,
                keys, ksizes, values, vsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void putBulk(size_t count,
                 const char* origin,
                 hg_bulk_t data,
                 size_t offset,
                 size_t size,
                 int32_t mode = YOKAN_MODE_DEFAULT,
                 Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_put_bulk(handle(), mode, count,
                origin, data, offset, size);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_put_bulk(handle(), mode | YOKAN_MODE_EXTRA, count,
                origin, data, offset, size,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    bool exists(const void* key,
                size_t ksize,
                int32_t mode = YOKAN_MODE_DEFAULT,
                Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        uint8_t e;
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_exists(handle(), mode, key, ksize, &e);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_exists(handle(), mode | YOKAN_MODE_EXTRA, key, ksize, &e,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
        return static_cast<bool>(e);
    }

    template <typename... Extras>
    std::vector<bool> existsMulti(
            size_t count,
            const void* const* keys,
            const size_t* ksizes,
            int32_t mode = YOKAN_MODE_DEFAULT,
            Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        std::vector<uint8_t> flags(1+count/8);
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_exists_multi(handle(), mode, count, keys, ksizes, flags.data());
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_exists_multi(handle(), mode | YOKAN_MODE_EXTRA, count,
                keys, ksizes, flags.data(),
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
        std::vector<bool> result(count);
        for(size_t i = 0; i < count; i++) {
            result[i] = yk_unpack_exists_flag(flags.data(), i);
        }
        return result;
    }

    template <typename... Extras>
    std::vector<bool> existsPacked(
            size_t count,
            const void* keys,
            const size_t* ksizes,
            int32_t mode = YOKAN_MODE_DEFAULT,
            Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        std::vector<uint8_t> flags(1+count/8);
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_exists_packed(handle(), mode, count, keys, ksizes, flags.data());
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_exists_packed(handle(), mode | YOKAN_MODE_EXTRA, count,
                keys, ksizes, flags.data(),
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
        std::vector<bool> result(count);
        for(size_t i = 0; i < count; i++) {
            result[i] = yk_unpack_exists_flag(flags.data(), i);
        }
        return result;
    }

    template <typename... Extras>
    void existsBulk(
            size_t count,
            const char* origin,
            hg_bulk_t data,
            size_t offset,
            size_t size,
            int32_t mode = YOKAN_MODE_DEFAULT,
            Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_exists_bulk(handle(),
                mode, count, origin, data, offset, size);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_exists_bulk(handle(),
                mode | YOKAN_MODE_EXTRA, count, origin, data, offset, size,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    size_t length(const void* key,
                  size_t ksize,
                  int32_t mode = YOKAN_MODE_DEFAULT,
                  Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        size_t vsize;
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_length(handle(), mode, key, ksize, &vsize);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_length(handle(), mode | YOKAN_MODE_EXTRA, key, ksize, &vsize,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
        return vsize;
    }

    template <typename... Extras>
    void lengthMulti(size_t count,
                     const void* const* keys,
                     const size_t* ksizes,
                     size_t* vsizes,
                     int32_t mode = YOKAN_MODE_DEFAULT,
                     Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_length_multi(handle(), mode, count, keys, ksizes, vsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_length_multi(handle(), mode | YOKAN_MODE_EXTRA, count,
                keys, ksizes, vsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void lengthPacked(size_t count,
                      const void* keys,
                      const size_t* ksizes,
                      size_t* vsizes,
                      int32_t mode = YOKAN_MODE_DEFAULT,
                      Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_length_packed(handle(), mode, count, keys, ksizes, vsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_length_packed(handle(), mode | YOKAN_MODE_EXTRA, count,
                keys, ksizes, vsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void lengthBulk(size_t count,
                    const char* origin,
                    hg_bulk_t data,
                    size_t offset,
                    size_t size,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_length_bulk(handle(), mode, count, origin, data, offset, size);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_length_bulk(handle(), mode | YOKAN_MODE_EXTRA, count,
                origin, data, offset, size,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void get(const void* key,
             size_t ksize,
             void* value,
             size_t* vsize,
             int32_t mode = YOKAN_MODE_DEFAULT,
             Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_get(handle(), mode, key, ksize, value, vsize);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_get(handle(), mode | YOKAN_MODE_EXTRA, key, ksize, value, vsize,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void getMulti(size_t count,
                  const void* const* keys,
                  const size_t* ksizes,
                  void* const* values,
                  size_t* vsizes,
                  int32_t mode = YOKAN_MODE_DEFAULT,
                  Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_get_multi(handle(), mode, count,
                keys, ksizes, values, vsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_get_multi(handle(), mode | YOKAN_MODE_EXTRA, count,
                keys, ksizes, values, vsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void getPacked(size_t count,
                   const void* keys,
                   const size_t* ksizes,
                   size_t vbufsize,
                   void* values,
                   size_t* vsizes,
                   int32_t mode = YOKAN_MODE_DEFAULT,
                   Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_get_packed(handle(), mode, count,
                keys, ksizes, vbufsize, values, vsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_get_packed(handle(), mode | YOKAN_MODE_EXTRA, count,
                keys, ksizes, vbufsize, values, vsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void getBulk(size_t count,
                 const char* origin,
                 hg_bulk_t data,
                 size_t offset,
                 size_t size,
                 bool packed,
                 int32_t mode = YOKAN_MODE_DEFAULT,
                 Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_get_bulk(handle(), mode, count, origin,
                data, offset, size, packed);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_get_bulk(handle(), mode | YOKAN_MODE_EXTRA, count, origin,
                data, offset, size, packed,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void fetch(const void* key,
               size_t ksize,
               yk_keyvalue_callback_t cb,
               void* uargs,
               int32_t mode = YOKAN_MODE_DEFAULT,
               Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_fetch(handle(), mode, key, ksize, cb, uargs);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_fetch(handle(), mode | YOKAN_MODE_EXTRA, key, ksize, cb, uargs,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    using fetch_callback_type =
        std::function<yk_return_t(size_t,const void*,size_t,const void*,size_t)>;

    static yk_return_t _fetch_dispatch(void* uargs, size_t index,
                                       const void* key, size_t ksize,
                                       const void* val, size_t vsize) {
            const fetch_callback_type* cb_ptr =
                static_cast<const fetch_callback_type*>(uargs);
            return (*cb_ptr)(index, key, ksize, val, vsize);
    }

    template <typename... Extras>
    void fetch(const void* key,
               size_t ksize,
               const fetch_callback_type& cb,
               int32_t mode = YOKAN_MODE_DEFAULT,
               Extras&&... extras) const {
        fetch(key, ksize, _fetch_dispatch, (void*)&cb, mode,
              std::forward<Extras>(extras)...);
    }

    template <typename... Extras>
    void fetchPacked(size_t count,
                     const void* keys,
                     const size_t* ksizes,
                     yk_keyvalue_callback_t cb,
                     void* uargs,
                     const yk_fetch_options_t* options = nullptr,
                     int32_t mode = YOKAN_MODE_DEFAULT,
                     Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_fetch_packed(
                handle(), mode, count, keys, ksizes, cb, uargs, options);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_fetch_packed(
                handle(), mode | YOKAN_MODE_EXTRA, count, keys, ksizes, cb, uargs, options,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void fetchPacked(size_t count,
                     const void* keys,
                     const size_t* ksizes,
                     const fetch_callback_type& cb,
                     const yk_fetch_options_t* options = nullptr,
                     int32_t mode = YOKAN_MODE_DEFAULT,
                     Extras&&... extras) const {
        fetchPacked(count, keys, ksizes, _fetch_dispatch, (void*)&cb,
                    options, mode, std::forward<Extras>(extras)...);
    }

    template <typename... Extras>
    void fetchMulti(size_t count,
                    const void* const* keys,
                    const size_t* ksizes,
                    yk_keyvalue_callback_t cb,
                    void* uargs,
                    const yk_fetch_options_t* options = nullptr,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_fetch_multi(
                handle(), mode, count, keys, ksizes, cb, uargs, options);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_fetch_multi(
                handle(), mode | YOKAN_MODE_EXTRA, count, keys, ksizes, cb, uargs, options,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void fetchMulti(size_t count,
                    const void* const* keys,
                    const size_t* ksizes,
                    const fetch_callback_type& cb,
                    const yk_fetch_options_t* options = nullptr,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        fetchMulti(count, keys, ksizes, _fetch_dispatch, (void*)&cb,
                   options, mode, std::forward<Extras>(extras)...);
    }

    template <typename... Extras>
    void fetchBulk(size_t count,
                   const char* origin,
                   hg_bulk_t data,
                   size_t offset,
                   size_t size,
                   yk_keyvalue_callback_t cb,
                   void* uargs,
                   const yk_fetch_options_t* options = nullptr,
                   int32_t mode = YOKAN_MODE_DEFAULT,
                   Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_fetch_bulk(
                handle(), mode, count, origin, data, offset, size, cb, uargs, options);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_fetch_bulk(
                handle(), mode | YOKAN_MODE_EXTRA, count, origin, data, offset, size,
                cb, uargs, options,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void fetchBulk(size_t count,
                   const char* origin,
                   hg_bulk_t data,
                   size_t offset,
                   size_t size,
                   const fetch_callback_type& cb,
                   const yk_fetch_options_t* options = nullptr,
                   int32_t mode = YOKAN_MODE_DEFAULT,
                   Extras&&... extras) const {
        fetchBulk(count, origin, data, offset, size,
                  _fetch_dispatch, (void*)&cb, options, mode,
                  std::forward<Extras>(extras)...);
    }

    template <typename... Extras>
    void erase(const void* key,
               size_t ksize,
               int32_t mode = YOKAN_MODE_DEFAULT,
               Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_erase(handle(), mode, key, ksize);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_erase(handle(), mode | YOKAN_MODE_EXTRA, key, ksize,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void eraseMulti(size_t count,
                    const void* const* keys,
                    const size_t* ksizes,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_erase_multi(handle(), mode, count, keys, ksizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_erase_multi(handle(), mode | YOKAN_MODE_EXTRA, count,
                keys, ksizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void erasePacked(size_t count,
                     const void* keys,
                     const size_t* ksizes,
                     int32_t mode = YOKAN_MODE_DEFAULT,
                     Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_erase_packed(handle(), mode, count, keys, ksizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_erase_packed(handle(), mode | YOKAN_MODE_EXTRA, count,
                keys, ksizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void eraseBulk(size_t count,
                   const char* origin,
                   hg_bulk_t data,
                   size_t offset,
                   size_t size,
                   int32_t mode = YOKAN_MODE_DEFAULT,
                   Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_erase_bulk(handle(), mode, count,
                origin, data, offset, size);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_erase_bulk(handle(), mode | YOKAN_MODE_EXTRA, count,
                origin, data, offset, size,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void eraseRange(const void* prefix,
                    size_t prefix_size,
                    int32_t mode = YOKAN_MODE_DEFAULT,
                    Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_erase_range(handle(), mode, prefix, prefix_size);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_erase_range(handle(), mode | YOKAN_MODE_EXTRA,
                prefix, prefix_size,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void listKeys(const void* from_key,
                  size_t from_ksize,
                  const void* filter,
                  size_t filter_size,
                  size_t count,
                  void* const* keys,
                  size_t* ksizes,
                  int32_t mode = YOKAN_MODE_DEFAULT,
                  Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_list_keys(handle(), mode, from_key,
                from_ksize, filter, filter_size, count, keys, ksizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_list_keys(handle(), mode | YOKAN_MODE_EXTRA, from_key,
                from_ksize, filter, filter_size, count, keys, ksizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void listKeysPacked(
            const void* from_key,
            size_t from_ksize,
            const void* filter,
            size_t filter_size,
            size_t count,
            void* keys,
            size_t keys_buf_size,
            size_t* ksizes,
            int32_t mode = YOKAN_MODE_DEFAULT,
            Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_list_keys_packed(handle(), mode, from_key,
                from_ksize, filter, filter_size, count, keys,
                keys_buf_size, ksizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_list_keys_packed(handle(), mode | YOKAN_MODE_EXTRA, from_key,
                from_ksize, filter, filter_size, count, keys,
                keys_buf_size, ksizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void listKeysBulk(
            size_t from_ksize,
            size_t filter_size,
            const char* origin,
            hg_bulk_t data,
            size_t offset,
            size_t keys_buf_size,
            bool packed,
            size_t count,
            int32_t mode = YOKAN_MODE_DEFAULT,
            Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_list_keys_bulk(handle(), mode, from_ksize,
                filter_size, origin, data, offset, keys_buf_size,
                packed, count);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_list_keys_bulk(handle(), mode | YOKAN_MODE_EXTRA, from_ksize,
                filter_size, origin, data, offset, keys_buf_size,
                packed, count,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void listKeyVals(const void* from_key,
                     size_t from_ksize,
                     const void* filter,
                     size_t filter_size,
                     size_t count,
                     void* const* keys,
                     size_t* ksizes,
                     void* const* values,
                     size_t* vsizes,
                     int32_t mode = YOKAN_MODE_DEFAULT,
                     Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_list_keyvals(handle(), mode, from_key,
                from_ksize, filter, filter_size, count, keys, ksizes, values, vsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_list_keyvals(handle(), mode | YOKAN_MODE_EXTRA, from_key,
                from_ksize, filter, filter_size, count, keys, ksizes, values, vsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void listKeyValsPacked(
            const void* from_key,
            size_t from_ksize,
            const void* filter,
            size_t filter_size,
            size_t count,
            void* keys,
            size_t keys_buf_size,
            size_t* ksizes,
            void* vals,
            size_t vals_buf_size,
            size_t* vsizes,
            int32_t mode = YOKAN_MODE_DEFAULT,
            Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_list_keyvals_packed(handle(), mode, from_key,
                from_ksize, filter, filter_size, count, keys,
                keys_buf_size, ksizes, vals, vals_buf_size, vsizes);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_list_keyvals_packed(handle(), mode | YOKAN_MODE_EXTRA, from_key,
                from_ksize, filter, filter_size, count, keys,
                keys_buf_size, ksizes, vals, vals_buf_size, vsizes,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void listKeyValsBulk(
            size_t from_ksize,
            size_t filter_size,
            const char* origin,
            hg_bulk_t data,
            size_t offset,
            size_t keys_buf_size,
            size_t vals_buf_size,
            bool packed,
            size_t count,
            int32_t mode = YOKAN_MODE_DEFAULT,
            Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_list_keyvals_bulk(handle(), mode, from_ksize,
                filter_size, origin, data, offset, keys_buf_size,
                vals_buf_size, packed, count);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_list_keyvals_bulk(handle(), mode | YOKAN_MODE_EXTRA, from_ksize,
                filter_size, origin, data, offset, keys_buf_size,
                vals_buf_size, packed, count,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void iter(const void* from_key,
              size_t from_ksize,
              const void* filter,
              size_t filter_size,
              size_t count,
              yk_keyvalue_callback_t cb,
              void* uargs,
              const yk_iter_options_t* options = nullptr,
              int32_t mode = YOKAN_MODE_DEFAULT,
              Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_iter(handle(), mode, from_key, from_ksize,
                          filter, filter_size, count, cb, uargs, options);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_iter(handle(), mode | YOKAN_MODE_EXTRA, from_key, from_ksize,
                          filter, filter_size, count, cb, uargs, options,
                          YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                          YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    using iter_callback_type =
        std::function<yk_return_t(size_t,const void*,size_t,const void*,size_t)>;

    static yk_return_t _iter_dispatch(void* uargs, size_t index,
                                      const void* key, size_t ksize,
                                      const void* val, size_t vsize) {
            const iter_callback_type* cb_ptr =
                static_cast<const iter_callback_type*>(uargs);
            return (*cb_ptr)(index, key, ksize, val, vsize);
    }

    template <typename... Extras>
    void iter(const void* from_key,
              size_t from_ksize,
              const void* filter,
              size_t filter_size,
              size_t count,
              const iter_callback_type& cb,
              const yk_iter_options_t* options = nullptr,
              int32_t mode = YOKAN_MODE_DEFAULT,
              Extras&&... extras) const {
        iter(from_key, from_ksize, filter, filter_size, count,
             _iter_dispatch, (void*)&cb, options, mode,
             std::forward<Extras>(extras)...);
    }

    template <typename... Extras>
    void createCollection(const char* name,
                          int32_t mode = YOKAN_MODE_DEFAULT,
                          Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_collection_create(handle(), name, mode);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_collection_create(handle(), name, mode | YOKAN_MODE_EXTRA,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    void dropCollection(const char* name,
                        int32_t mode = YOKAN_MODE_DEFAULT,
                        Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_collection_drop(handle(), name, mode);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_collection_drop(handle(), name, mode | YOKAN_MODE_EXTRA,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
    }

    template <typename... Extras>
    bool collectionExists(const char* name,
                          int32_t mode = YOKAN_MODE_DEFAULT,
                          Extras&&... extras) const {
        detail::check_known_extras<Extras...>();
        uint8_t flag;
        yk_return_t err;
        if constexpr (sizeof...(Extras) == 0) {
            err = yk_collection_exists(handle(), name, mode, &flag);
        } else {
            const auto t = detail::extract_extra<Timeout>(
                               std::forward<Extras>(extras)...);
            err = yk_collection_exists(handle(), name, mode | YOKAN_MODE_EXTRA, &flag,
                YOKAN_EXTRA_TIMEOUT_MS, t.ms,
                YOKAN_EXTRA_END);
        }
        YOKAN_CONVERT_AND_THROW(err);
        return static_cast<bool>(flag);
    }

    yk_database_handle_t handle() const {
        return m_db.get();
    }

    private:

    std::shared_ptr<yk_client>          m_client;
    std::shared_ptr<yk_database_handle> m_db;

};

}

#endif
