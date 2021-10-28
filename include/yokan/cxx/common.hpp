/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_COMMON_HPP
#define __YOKAN_COMMON_HPP

#include <yokan/common.h>
#include <string>

template<typename STREAM>
STREAM& operator<<(STREAM& stream, const yk_database_id_t& id) {
    char out[37];
    yk_database_id_to_string(id, out);
    return stream << std::string(out, 36);
}

#endif
