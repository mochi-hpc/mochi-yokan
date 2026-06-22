/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_DL_H
#define __YOKAN_DL_H

#include "logging.h"
#include <abt.h>
#include <dlfcn.h>
#include <string>
#include <unordered_set>

namespace yokan {

class Linker {

    public:

    template<typename F>
    static F load(const std::string& file, const std::string& function) {
        void* handle;
        if(!file.empty())
            handle = dlopen(file.c_str(), RTLD_NOW | RTLD_GLOBAL);
        else
            handle = dlopen(nullptr, RTLD_NOW | RTLD_GLOBAL);
        if(!handle) {
           YOKAN_LOG_ERROR(0, "dlopen failed to open file %s (%s)",
                file.c_str(), dlerror());
           return (F)(nullptr);
        }
        F fun = (F)(dlsym(handle, function.c_str()));
        char* err = dlerror();
        if(err != nullptr) {
            YOKAN_LOG_ERROR(0, "dlsym failed to find symbol %s (%s)",
                function.c_str(), err);
        }
        return fun;
    }

    template<typename F>
    static F load(const std::string& descriptor) {
        auto p = descriptor.find(':');
        if(p == std::string::npos) {
            return load<F>("", descriptor);
        } else {
            return load<F>(descriptor.substr(0, p),
                           descriptor.substr(p+1));
        }
    }

    /* Idempotent: on a cache hit, returns immediately without taking
     * glibc's _dl_load_lock. Loaded handles are intentionally never
     * dlclose'd — filter shared libraries register std::function callbacks
     * into static FilterFactory maps whose code lives in the .so, so
     * unloading would leave dangling pointers. */
    static void open(const std::string& filename) {
        static ABT_mutex_memory s_mtx_mem = ABT_MUTEX_INITIALIZER;
        static std::unordered_set<std::string> s_loaded;
        ABT_mutex s_mtx = ABT_MUTEX_MEMORY_GET_HANDLE(&s_mtx_mem);

        ABT_mutex_lock(s_mtx);
        if(s_loaded.count(filename)) {
            ABT_mutex_unlock(s_mtx);
            return;
        }
        ABT_mutex_unlock(s_mtx);

        void* handle = dlopen(filename.c_str(), RTLD_NOW | RTLD_GLOBAL);
        if(!handle) {
           YOKAN_LOG_ERROR(0, "dlopen failed to open file %s (%s)",
                filename.c_str(), dlerror());
           return;
        }

        ABT_mutex_lock(s_mtx);
        s_loaded.insert(filename);
        ABT_mutex_unlock(s_mtx);
    }
};

}

#endif
