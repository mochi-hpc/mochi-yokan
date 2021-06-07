/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"

namespace rkv {

std::unordered_map<std::string,
            std::function<KeyValueStoreInterface*(const std::string&)>>
    KeyValueStoreFactory::make_fn;

}
