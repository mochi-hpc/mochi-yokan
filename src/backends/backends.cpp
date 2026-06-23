/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"
#include "yokan/filters.hpp"
#include <cstring>
#include <vector>
#include <memory>

namespace yokan {

std::unordered_map<std::string,
            std::function<Status(
                    const std::string&,
                    DatabaseInterface**)>>
    DatabaseFactory::make_fn;

std::unordered_map<std::string,
            std::function<Status(
                    const std::string&,
                    const std::string&,
                    const std::string&,
                    const std::list<std::string>&,
                    DatabaseInterface**)>>
    DatabaseFactory::recover_fn;

namespace {

/* Minimal in-process prefix filter used by the default DatabaseInterface::eraseRange
 * fallback. We don't go through FilterFactory because backends should not depend on
 * the server util library. */
struct EraseRangePrefixFilter : public KeyValueFilter {

    UserMem m_prefix;

    explicit EraseRangePrefixFilter(UserMem prefix) : m_prefix(std::move(prefix)) {}

    bool isPassthrough() const override { return false; }

    bool requiresValue() const override { return false; }

    bool check(const void* key, size_t ksize,
               const void* val, size_t vsize) const override {
        (void)val; (void)vsize;
        if(m_prefix.size > ksize) return false;
        return std::memcmp(key, m_prefix.data, m_prefix.size) == 0;
    }

    size_t keySizeFrom(const void* key, size_t ksize) const override {
        (void)key;
        return ksize;
    }

    size_t valSizeFrom(const void* val, size_t vsize) const override {
        (void)val;
        return vsize;
    }

    size_t keyCopy(void* dst, size_t max_dst_size,
                   const void* key, size_t ksize) const override {
        if(max_dst_size < ksize) return YOKAN_SIZE_TOO_SMALL;
        std::memcpy(dst, key, ksize);
        return ksize;
    }

    size_t valCopy(void* dst, size_t max_dst_size,
                   const void* val, size_t vsize) const override {
        if(max_dst_size < vsize) return YOKAN_SIZE_TOO_SMALL;
        std::memcpy(dst, val, vsize);
        return vsize;
    }

    bool shouldStop(const void* key, size_t ksize,
                    const void* val, size_t vsize) const override {
        (void)val; (void)vsize;
        if(m_prefix.size == 0) return false;
        auto x = std::memcmp(key, m_prefix.data,
                             std::min<size_t>(ksize, m_prefix.size));
        return x > 0;
    }
};

constexpr size_t ERASE_RANGE_BATCH = 256;
constexpr size_t ERASE_RANGE_KEYS_BUF = 64 * 1024;

} // namespace

Status DatabaseInterface::eraseRange(int32_t mode, const UserMem& prefix)
{
    /* Strip YOKAN_MODE_EXTRA (client-side marker) before forwarding to the
     * listKeys/erase virtuals — backends only see real modes. */
    int32_t op_mode = mode & ~YOKAN_MODE_EXTRA;
    auto filter = std::make_shared<EraseRangePrefixFilter>(prefix);

    std::vector<char> keys_buf(ERASE_RANGE_KEYS_BUF);
    std::vector<size_t> ksizes(ERASE_RANGE_BATCH);
    std::vector<char> last_key;

    for(;;) {
        UserMem fromKey{ last_key.data(), last_key.size() };
        UserMem keys{ keys_buf.data(), keys_buf.size() };
        BasicUserMem<size_t> ksizes_umem{ ksizes.data(), ksizes.size() };
        for(auto& s : ksizes) s = 0;

        auto status = listKeys(op_mode, true, fromKey,
                std::static_pointer_cast<KeyValueFilter>(filter),
                keys, ksizes_umem);
        if(status != Status::OK) return status;

        size_t produced = 0;
        size_t total_ksize = 0;
        for(size_t i = 0; i < ksizes_umem.size; i++) {
            size_t s = ksizes_umem[i];
            if(s == YOKAN_NO_MORE_KEYS) break;
            if(s == YOKAN_SIZE_TOO_SMALL) return Status::SizeError;
            produced += 1;
            total_ksize += s;
        }
        if(produced == 0) return Status::OK;

        /* Remember the last key we saw so the next listKeys starts strictly
         * after it (default listKeys semantics: exclusive of fromKey). */
        size_t last_off = 0;
        for(size_t i = 0; i + 1 < produced; i++) last_off += ksizes_umem[i];
        last_key.assign(keys_buf.data() + last_off,
                        keys_buf.data() + last_off + ksizes_umem[produced - 1]);

        UserMem erase_keys{ keys_buf.data(), total_ksize };
        BasicUserMem<size_t> erase_ksizes{ ksizes_umem.data, produced };
        status = erase(op_mode, erase_keys, erase_ksizes);
        if(status != Status::OK) return status;

        if(produced < ksizes.size()) return Status::OK;
    }
}

}
