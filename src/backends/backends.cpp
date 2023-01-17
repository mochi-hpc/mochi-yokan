/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "yokan/backend.hpp"

namespace yokan {

std::unordered_map<std::string,
            std::function<Status(const std::string&,DatabaseInterface**)>>
    DatabaseFactory::make_fn;

std::unordered_map<std::string,
            std::function<Status(const std::string&, const std::list<std::string>&, DatabaseInterface**)>>
    DatabaseFactory::recover_fn;

}
