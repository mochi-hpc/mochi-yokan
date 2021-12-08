/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_EXCEPTION_HPP
#define __YOKAN_EXCEPTION_HPP

#include <yokan/common.h>
#include <exception>

namespace yokan {

class Exception : public std::exception {

    public:

    Exception(yk_return_t code)
    : m_code(code) {}

    const char* what() const noexcept override {
        #define X(__err__, __msg__) case __err__: return __msg__;
        switch(m_code) {
            YOKAN_RETURN_VALUES
        }
        #undef X
        return "";
    }

    auto code() const {
        return m_code;
    }

    private:

    yk_return_t m_code;
};

#define YOKAN_CONVERT_AND_THROW(__err__) do { \
    if((__err__) != YOKAN_SUCCESS) {          \
        throw ::yokan::Exception((__err__));  \
    }                                       \
} while(0)

}
#endif
