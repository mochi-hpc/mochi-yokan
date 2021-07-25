/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_LOCKS_HPP
#define __RKV_LOCKS_HPP

#include <abt.h>

namespace rkv {

struct ScopedWriteLock {

    ScopedWriteLock() = default;

    ScopedWriteLock(ABT_rwlock lock)
    : m_lock(lock) {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_wrlock(m_lock);
    }

    ~ScopedWriteLock() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_unlock(m_lock);
    }

    void unlock() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_unlock(m_lock);
    }

    void lock() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_wrlock(m_lock);
    }

    ABT_rwlock m_lock = ABT_RWLOCK_NULL;
};

struct ScopedReadLock {

    ScopedReadLock(ABT_rwlock lock)
    : m_lock(lock) {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_rdlock(m_lock);
    }

    ~ScopedReadLock() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_unlock(m_lock);
    }

    void unlock() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_unlock(m_lock);
    }

    void lock() {
        if(m_lock != ABT_RWLOCK_NULL)
            ABT_rwlock_rdlock(m_lock);
    }

    ABT_rwlock m_lock = ABT_RWLOCK_NULL;
};

}
#endif
