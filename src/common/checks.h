/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_CHECKS_H
#define __YOKAN_CHECKS_H

#include "yokan/common.h"
#include "logging.h"

#define CHECK_HRET(__hret__, __fun__) \
    do { \
        if(__hret__ != HG_SUCCESS) { \
            YOKAN_LOG_ERROR(mid, #__fun__ " returned %d", __hret__); \
            return YOKAN_ERR_FROM_MERCURY; \
        } \
    } while(0)

#define CHECK_HRET_OUT(__hret__, __fun__) \
    do { \
        if(__hret__ != HG_SUCCESS) { \
            YOKAN_LOG_ERROR(mid, #__fun__ " returned %d", __hret__); \
            out.ret = YOKAN_ERR_FROM_MERCURY; \
            return; \
        } \
    } while(0)

#define CHECK_HRET_OUT_GOTO(__hret__, __fun__, __label__) \
    do { \
        if(__hret__ != HG_SUCCESS) { \
            YOKAN_LOG_ERROR(mid, #__fun__ " returned %d", __hret__); \
            out.ret = YOKAN_ERR_FROM_MERCURY; \
            goto __label__; \
        } \
    } while(0)

#define CHECK_RRET_OUT(__rret__, __fun__) \
    do { \
        if(__rret__ != REMI_SUCCESS) { \
            YOKAN_LOG_ERROR(mid, #__fun__ " returned %d", __rret__); \
            out.ret = YOKAN_ERR_FROM_REMI; \
            return; \
        } \
    } while(0)

#define CHECK_RRET_OUT_CANCEL(__rret__, __fun__, __mh__) \
    do { \
        if(__rret__ != REMI_SUCCESS) { \
            YOKAN_LOG_ERROR(mid, #__fun__ " returned %d", __rret__); \
            out.ret = YOKAN_ERR_FROM_REMI; \
            (__mh__)->cancel(); \
            return; \
        } \
    } while(0)

#define CHECK_MID(__mid__, __fun__) \
    do { \
        if(__mid__ == MARGO_INSTANCE_NULL) { \
            YOKAN_LOG_ERROR(__mid__, #__fun__ " returned invalid margo instance"); \
            out.ret = YOKAN_ERR_INVALID_MID; \
            return; \
        } \
    } while(0)

#define CHECK_PROVIDER(__pr__) \
    do { \
        if(!__pr__) { \
            YOKAN_LOG_ERROR(mid, "could not find provider"); \
            out.ret = YOKAN_ERR_INVALID_PROVIDER; \
            return; \
        } \
    } while(0)

#define CHECK_DATABASE(__db__) \
    do { \
        if(!__db__) { \
            YOKAN_LOG_ERROR(mid, "no database attached to this provider"); \
            out.ret = YOKAN_ERR_INVALID_DATABASE; \
            return; \
        } \
    } while(0)

#define CHECK_MODE_SUPPORTED(__db__, __mode__) \
    do { \
        if(!__db__->supportsMode(__mode__)) { \
            out.ret = YOKAN_ERR_MODE; \
            YOKAN_LOG_ERROR(mid, "mode not supported by database"); \
            return; \
        } \
    } while(0)

#define CHECK_BUFFER(__buf__) \
    do { \
        if(!__buf__) { \
            YOKAN_LOG_ERROR(mid, "could not get bulk buffer"); \
            out.ret = YOKAN_ERR_ALLOCATION; \
            return; \
        } \
    } while(0)

static constexpr int32_t incompatible_modes[][2] = {
    { YOKAN_MODE_APPEND, YOKAN_MODE_NEW_ONLY },
    { YOKAN_MODE_NEW_ONLY, YOKAN_MODE_EXIST_ONLY },
    { YOKAN_MODE_SUFFIX, YOKAN_MODE_LUA_FILTER },
    { YOKAN_MODE_LIB_FILTER, YOKAN_MODE_SUFFIX },
    { YOKAN_MODE_LUA_FILTER, YOKAN_MODE_LIB_FILTER },
    { 0, 0 }};

#define CHECK_MODE_VALID(__mode__) \
    do { \
        int __i = 0; \
        while(incompatible_modes[__i][0] != 0) { \
            if(((__mode__) & incompatible_modes[__i][0]) \
            && ((__mode__) & incompatible_modes[__i][1])) \
                return YOKAN_ERR_MODE; \
            __i += 1; \
        } \
    } while(0)

#endif
