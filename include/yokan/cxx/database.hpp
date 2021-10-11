/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_DATABASE_HPP
#define __YOKAN_DATABASE_HPP

#include <yokan/database.h>
#include <yokan/cxx/exception.hpp>

namespace yokan {

class Client;

class Database {

    friend class Client;

    public:

    Database(yk_database_handle_t db, bool copy=true)
    : m_db(db) {
        if(copy && (m_db != YOKAN_DATABASE_HANDLE_NULL)) {
            auto err = yk_database_handle_ref_incr(m_db);
            YOKAN_CONVERT_AND_THROW(err);
        }
    }

    Database(yk_client_t client,
             hg_addr_t addr,
             uint16_t provider_id,
             yk_database_id_t database_id)
    {
        auto err = yk_database_handle_create(client,
            addr, provider_id, database_id, &m_db);
        YOKAN_CONVERT_AND_THROW(err);
    }

    Database(const Database& other)
    : m_db(other.m_db) {
        if(m_db != YOKAN_DATABASE_HANDLE_NULL) {
            auto err = yk_database_handle_ref_incr(m_db);
            YOKAN_CONVERT_AND_THROW(err);
        }
    }

    Database(Database&& other)
    : m_db(other.m_db) {
        other.m_db = YOKAN_DATABASE_HANDLE_NULL;
    }

    Database& operator=(const Database& other) {
        if(m_db == other.m_db || &other == this)
            return *this;
        if(m_db != YOKAN_DATABASE_HANDLE_NULL) {
            auto err = yk_database_handle_release(m_db);
            YOKAN_CONVERT_AND_THROW(err);
        }
        m_db = other.m_db;
        if(m_db != YOKAN_DATABASE_HANDLE_NULL) {
            auto err = yk_database_handle_ref_incr(m_db);
            YOKAN_CONVERT_AND_THROW(err);
        }
        return *this;
    }

    Database& operator=(Database&& other) {
        if(m_db == other.m_db || &other == this)
            return *this;
        if(m_db != YOKAN_DATABASE_HANDLE_NULL) {
            auto err = yk_database_handle_release(m_db);
            YOKAN_CONVERT_AND_THROW(err);
        }
        m_db = other.m_db;
        other.m_db = YOKAN_DATABASE_HANDLE_NULL;
        return *this;
    }

    ~Database() {
        if(m_db != YOKAN_DATABASE_HANDLE_NULL) {
            yk_database_handle_release(m_db);
        }
    }

    size_t count(int32_t mode = YOKAN_MODE_DEFAULT) const {
        size_t c;
        auto err = yk_count(m_db, mode, &c);
        YOKAN_CONVERT_AND_THROW(err);
        return c;
    }

    void put(const void* key,
             size_t ksize,
             const void* value,
             size_t vsize,
             int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_put(m_db, mode, key, ksize, value, vsize);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void putMulti(size_t count,
                  const void* const* keys,
                  const size_t* ksizes,
                  const void* const* values,
                  const size_t* vsizes,
                  int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_put_multi(m_db, mode, count,
            keys, ksizes, values, vsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void putPacked(size_t count,
                   const void* keys,
                   const size_t* ksizes,
                   const void* values,
                   const size_t* vsizes,
                   int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_put_packed(m_db, mode, count,
            keys, ksizes, values, vsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void putBulk(size_t count,
                 const char* origin,
                 hg_bulk_t data,
                 size_t offset,
                 size_t size,
                 int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_put_bulk(m_db, mode, count,
            origin, data, offset, size);
        YOKAN_CONVERT_AND_THROW(err);
    }

    bool exists(const void* key,
                size_t ksize,
                int32_t mode = YOKAN_MODE_DEFAULT) const {
        uint8_t e;
        auto err = yk_exists(m_db, mode, key, ksize, &e);
        YOKAN_CONVERT_AND_THROW(err);
        return static_cast<bool>(e);
    }

    std::vector<bool> existsMulti(
            size_t count,
            const void* const* keys,
            const size_t* ksizes,
            int32_t mode = YOKAN_MODE_DEFAULT) const {
        std::vector<uint8_t> flags(1+count/8);
        auto err = yk_exists_multi(m_db, mode, count, keys, ksizes, flags.data());
        YOKAN_CONVERT_AND_THROW(err);
        std::vector<bool> result(count);
        for(size_t i = 0; i < count; i++) {
            result[i] = yk_unpack_exists_flag(flags.data(), i);
        }
        return result;
    }

    std::vector<bool> existsPacked(
            size_t count,
            const void* keys,
            const size_t* ksizes,
            int32_t mode = YOKAN_MODE_DEFAULT) const {
        std::vector<uint8_t> flags(1+count/8);
        auto err = yk_exists_packed(m_db, mode, count, keys, ksizes, flags.data());
        YOKAN_CONVERT_AND_THROW(err);
        std::vector<bool> result(count);
        for(size_t i = 0; i < count; i++) {
            result[i] = yk_unpack_exists_flag(flags.data(), i);
        }
        return result;
    }

    void existsBulk(
            size_t count,
            const char* origin,
            hg_bulk_t data,
            size_t offset,
            size_t size,
            int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_exists_bulk(m_db,
            mode, count, origin, data, offset, size);
        YOKAN_CONVERT_AND_THROW(err);
    }

    size_t length(const void* key,
                  size_t ksize,
                  int32_t mode = YOKAN_MODE_DEFAULT) const {
        size_t vsize;
        auto err = yk_length(m_db, mode, key, ksize, &vsize);
        YOKAN_CONVERT_AND_THROW(err);
        return vsize;
    }

    void lengthMulti(size_t count,
                     const void* const* keys,
                     const size_t* ksizes,
                     size_t* vsizes,
                     int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_length_multi(m_db, mode, count, keys, ksizes, vsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void lengthPacked(size_t count,
                      const void* keys,
                      const size_t* ksizes,
                      size_t* vsizes,
                      int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_length_packed(m_db, mode, count, keys, ksizes, vsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void lengthBulk(size_t count,
                    const char* origin,
                    hg_bulk_t data,
                    size_t offset,
                    size_t size,
                    int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_length_bulk(m_db, mode, count, origin, data, offset, size);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void get(const void* key,
             size_t ksize,
             void* value,
             size_t* vsize,
             int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_get(m_db, mode, key, ksize, value, vsize);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void getMulti(size_t count,
                  const void* const* keys,
                  const size_t* ksizes,
                  void* const* values,
                  size_t* vsizes,
                  int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_get_multi(m_db, mode, count,
            keys, ksizes, values, vsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void getPacked(size_t count,
                   const void* keys,
                   const size_t* ksizes,
                   size_t vbufsize,
                   void* values,
                   size_t* vsizes,
                   int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_get_packed(m_db, mode, count,
            keys, ksizes, vbufsize, values, vsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void getBulk(size_t count,
                 const char* origin,
                 hg_bulk_t data,
                 size_t offset,
                 size_t size,
                 bool packed,
                 int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_get_bulk(m_db, mode, count, origin,
            data, offset, size, packed);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void erase(const void* key,
               size_t ksize,
               int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_erase(m_db, mode, key, ksize);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void eraseMulti(size_t count,
                    const void* const* keys,
                    const size_t* ksizes,
                    int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_erase_multi(m_db, mode, count, keys, ksizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void erasePacked(size_t count,
                     const void* keys,
                     const size_t* ksizes,
                     int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_erase_packed(m_db, mode, count, keys, ksizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void eraseBulk(size_t count,
                   const char* origin,
                   hg_bulk_t data,
                   size_t offset,
                   size_t size,
                   int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_erase_bulk(m_db, mode, count,
            origin, data, offset, size);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void listKeys(const void* from_key,
                  size_t from_ksize,
                  const void* filter,
                  size_t filter_size,
                  size_t count,
                  void* const* keys,
                  size_t* ksizes,
                  int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_list_keys(m_db, mode, from_key,
            from_ksize, filter, filter_size, count, keys, ksizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void listKeysPacked(
            const void* from_key,
            size_t from_ksize,
            const void* filter,
            size_t filter_size,
            size_t count,
            void* keys,
            size_t keys_buf_size,
            size_t* ksizes,
            int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_list_keys_packed(m_db, mode, from_key,
            from_ksize, filter, filter_size, count, keys,
            keys_buf_size, ksizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void listKeysBulk(
            size_t from_ksize,
            size_t filter_size,
            const char* origin,
            hg_bulk_t data,
            size_t offset,
            size_t keys_buf_size,
            bool packed,
            size_t count,
            int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_list_keys_bulk(m_db, mode, from_ksize,
            filter_size, origin, data, offset, keys_buf_size,
            packed, count);
        YOKAN_CONVERT_AND_THROW(err);
    }

    void listKeyVals(const void* from_key,
                     size_t from_ksize,
                     const void* filter,
                     size_t filter_size,
                     size_t count,
                     void* const* keys,
                     size_t* ksizes,
                     void* const* values,
                     size_t* vsizes,
                     int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_list_keyvals(m_db, mode, from_key,
            from_ksize, filter, filter_size, count, keys, ksizes, values, vsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

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
            int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_list_keyvals_packed(m_db, mode, from_key,
            from_ksize, filter, filter_size, count, keys,
            keys_buf_size, ksizes, vals, vals_buf_size, vsizes);
        YOKAN_CONVERT_AND_THROW(err);
    }

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
            int32_t mode = YOKAN_MODE_DEFAULT) const {
        auto err = yk_list_keyvals_bulk(m_db, mode, from_ksize,
            filter_size, origin, data, offset, keys_buf_size,
            vals_buf_size, packed, count);
        YOKAN_CONVERT_AND_THROW(err);
    }

    auto handle() const {
        return m_db;
    }

    private:

    yk_database_handle_t m_db = YOKAN_DATABASE_HANDLE_NULL;

};

}

#endif
