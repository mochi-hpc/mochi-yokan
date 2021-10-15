/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_DL_H
#define __YOKAN_DL_H

#include "logging.h"
#include <dlfcn.h>

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

    static void open(const std::string& filename) {
        void* handle;
        handle = dlopen(filename.c_str(), RTLD_NOW | RTLD_GLOBAL);
        if(!handle) {
           YOKAN_LOG_ERROR(0, "dlopen failed to open file %s (%s)",
                filename.c_str(), dlerror());
        }
    }
};

}

#endif
