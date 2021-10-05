/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_DATABASE_HPP
#define __RKV_DATABASE_HPP

#include <rkv/rkv-database.h>
#include <rkv/cxx/rkv-exception.hpp>

namespace rkv {

class Client;

class Database {

    friend class Client;

    public:

    Database(rkv_database_handle_t db, bool copy=true)
    : m_db(db) {
        if(copy && (m_db != RKV_DATABASE_HANDLE_NULL)) {
            auto err = rkv_database_handle_ref_incr(m_db);
            RKV_CONVERT_AND_THROW(err);
        }
    }

    Database(rkv_client_t client,
             hg_addr_t addr,
             uint16_t provider_id,
             rkv_database_id_t database_id)
    {
        auto err = rkv_database_handle_create(client,
            addr, provider_id, database_id, &m_db);
        RKV_CONVERT_AND_THROW(err);
    }

    Database(const Database& other)
    : m_db(other.m_db) {
        if(m_db != RKV_DATABASE_HANDLE_NULL) {
            auto err = rkv_database_handle_ref_incr(m_db);
            RKV_CONVERT_AND_THROW(err);
        }
    }

    Database(Database&& other)
    : m_db(other.m_db) {
        other.m_db = RKV_DATABASE_HANDLE_NULL;
    }

    Database& operator=(const Database& other) {
        if(m_db == other.m_db || &other == this)
            return *this;
        if(m_db != RKV_DATABASE_HANDLE_NULL) {
            auto err = rkv_database_handle_release(m_db);
            RKV_CONVERT_AND_THROW(err);
        }
        m_db = other.m_db;
        if(m_db != RKV_DATABASE_HANDLE_NULL) {
            auto err = rkv_database_handle_ref_incr(m_db);
            RKV_CONVERT_AND_THROW(err);
        }
        return *this;
    }

    Database& operator=(Database&& other) {
        if(m_db == other.m_db || &other == this)
            return *this;
        if(m_db != RKV_DATABASE_HANDLE_NULL) {
            auto err = rkv_database_handle_release(m_db);
            RKV_CONVERT_AND_THROW(err);
        }
        m_db = other.m_db;
        other.m_db = RKV_DATABASE_HANDLE_NULL;
        return *this;
    }

    ~Database() {
        if(m_db != RKV_DATABASE_HANDLE_NULL) {
            rkv_database_handle_release(m_db);
        }
    }

    size_t count(int32_t mode = RKV_MODE_DEFAULT) const {
        size_t c;
        auto err = rkv_count(m_db, mode, &c);
        RKV_CONVERT_AND_THROW(err);
    }

    void put(const void* key,
             size_t ksize,
             const void* value,
             size_t vsize,
             int32_t mode = RKV_MODE_DEFAULT) const {
        auto err = rkv_put(m_db, mode, key, ksize, value, vsize);
        RKV_CONVERT_AND_THROW(err);
    }

    void putMulti(size_t count,
                  const void* const* keys,
                  const size_t* ksizes,
                  const void* const* values,
                  const size_t* vsizes,
                  int32_t mode = RKV_MODE_DEFAULT) const {
        auto err = rkv_put_multi(m_db, mode, count,
            keys, ksizes, values, vsizes);
        RKV_CONVERT_AND_THROW(err);
    }

    void putPacked(size_t count,
                   const void* keys,
                   const size_t* ksizes,
                   const void* values,
                   const size_t* vsizes,
                   int32_t mode = RKV_MODE_DEFAULT) const {
        auto err = rkv_put_packed(m_db, mode, count,
            keys, ksizes, values, vsizes);
        RKV_CONVERT_AND_THROW(err);
    }

    void putBulk(size_t count,
                 const char* origin,
                 hg_bulk_t data,
                 size_t offset,
                 size_t size,
                 int32_t mode = RKV_MODE_DEFAULT) const {
        auto err = rkv_put_bulk(m_db, mode, count,
            origin, data, offset, size);
        RKV_CONVERT_AND_THROW(err);
    }

    bool exists(const void* key,
                size_t ksize,
                int32_t mode = RKV_MODE_DEFAULT) const {
        uint8_t e;
        auto err = rkv_exists(m_db, mode, key, ksize, &e);
        RKV_CONVERT_AND_THROW(err);
        return static_cast<bool>(e);
    }

    std::vector<bool> existsMulti(
            size_t count,
            const void* const* keys,
            const size_t* ksizes,
            int32_t mode = RKV_MODE_DEFAULT) const {
        std::vector<uint8_t> flags(1+count/8);
        auto err = rkv_exists_multi(m_db, mode, count, keys, ksizes, flags.data());
        RKV_CONVERT_AND_THROW(err);
        std::vector<bool> result(count);
        for(size_t i = 0; i < count; i++) {
            result[i] = rkv_unpack_exists_flag(flags.data(), i);
        }
        return result;
    }

    std::vector<bool> existsPacked(
            size_t count,
            const void* keys,
            const size_t* ksizes,
            int32_t mode = RKV_MODE_DEFAULT) const {
        std::vector<uint8_t> flags(1+count/8);
        auto err = rkv_exists_packed(m_db, mode, count, keys, ksizes, flags.data());
        RKV_CONVERT_AND_THROW(err);
        std::vector<bool> result(count);
        for(size_t i = 0; i < count; i++) {
            result[i] = rkv_unpack_exists_flag(flags.data(), i);
        }
        return result;
    }

    void existsBulk(
            size_t count,
            const char* origin,
            hg_bulk_t data,
            size_t offset,
            size_t size,
            int32_t mode = RKV_MODE_DEFAULT) const {
        auto err = rkv_exists_bulk(m_db,
            mode, count, origin, data, offset, size);
        RKV_CONVERT_AND_THROW(err);
    }

    private:

    rkv_database_handle_t m_db = RKV_DATABASE_HANDLE_NULL;

};

#if 0

rkv_return_t rkv_length(rkv_database_handle_t dbh,
                        int32_t mode,
                        const void* key,
                        size_t ksize,
                        size_t* vsize);

rkv_return_t rkv_length_multi(rkv_database_handle_t dbh,
                              int32_t mode,
                              size_t count,
                              const void* const* keys,
                              const size_t* ksizes,
                              size_t* vsizes);

rkv_return_t rkv_length_packed(rkv_database_handle_t dbh,
                               int32_t mode,
                               size_t count,
                               const void* keys,
                               const size_t* ksizes,
                               size_t* vsizes);

rkv_return_t rkv_length_bulk(rkv_database_handle_t dbh,
                             int32_t mode,
                             size_t count,
                             const char* origin,
                             hg_bulk_t data,
                             size_t offset,
                             size_t size);

rkv_return_t rkv_get(rkv_database_handle_t dbh,
                     int32_t mode,
                     const void* key,
                     size_t ksize,
                     void* value,
                     size_t* vsize);

rkv_return_t rkv_get_multi(rkv_database_handle_t dbh,
                           int32_t mode,
                           size_t count,
                           const void* const* keys,
                           const size_t* ksizes,
                           void* const* values,
                           size_t* vsizes);

rkv_return_t rkv_get_packed(rkv_database_handle_t dbh,
                            int32_t mode,
                            size_t count,
                            const void* keys,
                            const size_t* ksizes,
                            size_t vbufsize,
                            void* values,
                            size_t* vsizes);

rkv_return_t rkv_get_bulk(rkv_database_handle_t dbh,
                          int32_t mode,
                          size_t count,
                          const char* origin,
                          hg_bulk_t data,
                          size_t offset,
                          size_t size,
                          bool packed);

rkv_return_t rkv_erase(rkv_database_handle_t dbh,
                       int32_t mode,
                       const void* key,
                       size_t ksize);

rkv_return_t rkv_erase_multi(rkv_database_handle_t dbh,
                             int32_t mode,
                             size_t count,
                             const void* const* keys,
                             const size_t* ksizes);

rkv_return_t rkv_erase_packed(rkv_database_handle_t dbh,
                              int32_t mode,
                              size_t count,
                              const void* keys,
                              const size_t* ksizes);

rkv_return_t rkv_erase_bulk(rkv_database_handle_t dbh,
                            int32_t mode,
                            size_t count,
                            const char* origin,
                            hg_bulk_t data,
                            size_t offset,
                            size_t size);


rkv_return_t rkv_list_keys(rkv_database_handle_t dbh,
                           int32_t mode,
                           const void* from_key,
                           size_t from_ksize,
                           const void* filter,
                           size_t filter_size,
                           size_t count,
                           void* const* keys,
                           size_t* ksizes);

rkv_return_t rkv_list_keys_packed(rkv_database_handle_t dbh,
                                  int32_t mode,
                                  const void* from_key,
                                  size_t from_ksize,
                                  const void* filter,
                                  size_t filter_size,
                                  size_t count,
                                  void* keys,
                                  size_t keys_buf_size,
                                  size_t* ksizes);

rkv_return_t rkv_list_keys_bulk(rkv_database_handle_t dbh,
                                int32_t mode,
                                size_t from_ksize,
                                size_t filter_size,
                                const char* origin,
                                hg_bulk_t data,
                                size_t offset,
                                size_t keys_buf_size,
                                bool packed,
                                size_t count);


rkv_return_t rkv_list_keyvals(rkv_database_handle_t dbh,
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

rkv_return_t rkv_list_keyvals_packed(rkv_database_handle_t dbh,
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

rkv_return_t rkv_list_keyvals_bulk(rkv_database_handle_t dbh,
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

#endif
}

#endif
