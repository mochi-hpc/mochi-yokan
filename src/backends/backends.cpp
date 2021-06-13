/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "rkv/rkv-backend.hpp"

namespace rkv {

std::unordered_map<std::string,
            std::function<Status(const std::string&,KeyValueStoreInterface**)>>
    KeyValueStoreFactory::make_fn;

}
