/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_WATCHER_H
#define __RKV_WATCHER_H

#include <map>
#include <yokan/backend.hpp>
#include <abt.h>
#if __cplusplus >= 201703L
#include <string_view>
#else
#include <experimental/string_view>
#endif

namespace rkv {

/**
 * @brief The KeyWatcher class is a help class used in backends
 * and implementing a convenient way of watching and being notified
 * for modification in keys. Threads expecting a modification of
 * a key should first add the key using KeyWatcher::addKey. They
 * then have to unlock any lock/mutex that could prevent a writer
 * from adding the key. Finally, they call KeyWatcher::waitKey
 * to wait on the key they have just added.
 * A writer thread can call KeyWatcher::notifyKey to wake up only
 * the threads blocked waiting for a partucular key.
 *
 * Imporant: the memory address and size of the key passed to
 * waitKey should be the same as that passed to addKey.
 */
class KeyWatcher {

    public:

    enum Status {
        Pending    = 0, /* Initial status */
        KeyPresent = 1, /* Key has appeared */
        Timeout    = 2, /* Timeout or service shutting down */
        LogicError = 3  /* Logic error (e.g. waiting for a key not added */
    };

    private:

#if __cplusplus >= 201703L
    using string_view = std::string_view;
#else
    using string_view = std::experimental::string_view;
#endif

    struct  Entry {
        ABT_cond_memory m_cond   = ABT_COND_INITIALIZER;
        Status          m_status = Pending;

        Entry() = default;
        ~Entry() = default;
        Entry(Entry&&) = delete;
        Entry(const Entry&) = delete;
        Entry& operator=(Entry&&) = delete;
        Entry& operator=(const Entry&) = delete;
    };

    std::multimap<string_view, Entry> m_expected_keys;
    ABT_mutex                         m_mutex;
    ABT_cond_memory                   m_cond = ABT_COND_INITIALIZER;

    public:

    KeyWatcher() {
        ABT_mutex_create(&m_mutex);
    }

    ~KeyWatcher() {
        ABT_mutex_lock(m_mutex);
        if(!m_expected_keys.empty()) {
            for(auto it = m_expected_keys.begin(); it != m_expected_keys.end(); ++it) {
                auto& entry = it->second;
                entry.m_status = Status::Timeout;
                ABT_cond cond = ABT_COND_MEMORY_GET_HANDLE(&(entry.m_cond));
                ABT_cond_signal(cond);
            }
        }
        while(!m_expected_keys.empty())
            ABT_cond_wait(ABT_COND_MEMORY_GET_HANDLE(&m_cond), m_mutex);
        ABT_mutex_unlock(m_mutex);
        ABT_mutex_free(&m_mutex);
    }

    KeyWatcher(const KeyWatcher&) = delete;
    KeyWatcher(KeyWatcher&&) = delete;
    KeyWatcher& operator=(const KeyWatcher&) = delete;
    KeyWatcher& operator=(KeyWatcher&&) = delete;

    void addKey(const UserMem& key) {
        ABT_mutex_lock(m_mutex);
        m_expected_keys.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(key.data, key.size),
            std::forward_as_tuple()
        );
        ABT_mutex_unlock(m_mutex);
    }

    Status waitKey(const UserMem& key) {
        ABT_mutex_lock(m_mutex);
        auto range = m_expected_keys.equal_range(string_view{key.data, key.size});
        auto& it = range.first;
        while(it != range.second) {
            if(it->first.data() == key.data)
                break;
        }
        if(it == m_expected_keys.end()) {
            ABT_mutex_unlock(m_mutex);
            return Status::LogicError;
        }
        auto& entry = it->second;
        ABT_cond cond = ABT_COND_MEMORY_GET_HANDLE(&(entry.m_cond));
        while(entry.m_status == Status::Pending)
            ABT_cond_wait(cond, m_mutex);
        auto status = entry.m_status;
        m_expected_keys.erase(it);
        if(m_expected_keys.empty())
            ABT_cond_signal(ABT_COND_MEMORY_GET_HANDLE(&m_cond));
        ABT_mutex_unlock(m_mutex);
        return status;
    }

    void notifyKey(const UserMem& key) {
        ABT_mutex_lock(m_mutex);
        if(m_expected_keys.empty()) {
            ABT_mutex_unlock(m_mutex);
            return;
        }
        auto range = m_expected_keys.equal_range(string_view{key.data, key.size});
        for(auto it = range.first; it != range.second; ++it) {
            auto& entry = it->second;
            entry.m_status = Status::KeyPresent;
            ABT_cond cond = ABT_COND_MEMORY_GET_HANDLE(&(entry.m_cond));
            ABT_cond_signal(cond);
        }
        ABT_mutex_unlock(m_mutex);
    }

};

}

#endif
