/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_SERVER_HPP
#define __YOKAN_SERVER_HPP

#include <yokan/server.h>
#include <yokan/cxx/exception.hpp>

namespace yokan {

class Provider {

    public:

    Provider(margo_instance_id mid,
             uint16_t provider_id,
             const struct yk_provider_args* args)
    {
        m_mid = mid;
        auto err = yk_provider_register(mid, provider_id, args, &m_provider);
        YOKAN_CONVERT_AND_THROW(err);
        margo_provider_push_finalize_callback(
            mid, this, finalizeCallback, this);
    }

    Provider(margo_instance_id mid,
             uint16_t provider_id,
             const char* token="",
             const char* config="{}",
             ABT_pool pool=ABT_POOL_NULL,
             yk_bulk_cache_t cache=nullptr) {
        m_mid = mid;
        struct yk_provider_args args = {
            token, config, pool, cache
        };
        auto err = yk_provider_register(mid, provider_id, &args, &m_provider);
        YOKAN_CONVERT_AND_THROW(err);
        margo_provider_push_finalize_callback(
            mid, this, finalizeCallback, this);
    }

    ~Provider() {
        if(m_provider == YOKAN_PROVIDER_NULL)
            return;
        yk_provider_destroy(m_provider);
        margo_provider_pop_finalize_callback(m_mid, this);
    }

    Provider(Provider&& other)
    : m_mid(other.m_mid)
    , m_provider(other.m_provider) {
        other.m_provider = YOKAN_PROVIDER_NULL;
        margo_provider_pop_finalize_callback(m_mid, &other);
        margo_provider_push_finalize_callback(
            m_mid, this, finalizeCallback, this);
    }

    private:

    static void finalizeCallback(void* arg) {
        auto this_provider = static_cast<Provider*>(arg);
        if(this_provider->m_provider == YOKAN_PROVIDER_NULL)
            return;
        yk_provider_destroy(this_provider->m_provider);
        this_provider->m_provider = YOKAN_PROVIDER_NULL;
    }

    margo_instance_id m_mid = MARGO_INSTANCE_NULL;
    yk_provider_t m_provider = YOKAN_PROVIDER_NULL;
};

}

#endif
