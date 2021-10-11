/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_EXCEPTION_HPP
#define __RKV_EXCEPTION_HPP

#include <rkv/rkv-common.h>
#include <exception>

namespace rkv {

class Exception : public std::exception {

    public:

    Exception(rkv_return_t code)
    : m_code(code) {}

    const char* what() const noexcept override {
        #define X(__err__, __msg__) case __err__: return __msg__;
        switch(m_code) {
            RKV_RETURN_VALUES
        }
        #undef X
        return "";
    }

    private:

    rkv_return_t m_code;
};

#define RKV_CONVERT_AND_THROW(__err__) do { \
    if((__err__) != RKV_SUCCESS) {          \
        throw ::rkv::Exception((__err__));  \
    }                                       \
} while(0)

}
#endif
