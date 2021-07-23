/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_CHECKS_H
#define __RKV_CHECKS_H

#include "logging.h"

#define CHECK_HRET(__hret__, __fun__) \
    do { \
        if(__hret__ != HG_SUCCESS) { \
            RKV_LOG_ERROR(mid, #__fun__ " returned %d", __hret__); \
            return RKV_ERR_FROM_MERCURY; \
        } \
    } while(0)

#define CHECK_HRET_OUT(__hret__, __fun__) \
    do { \
        if(__hret__ != HG_SUCCESS) { \
            RKV_LOG_ERROR(mid, #__fun__ " returned %d", __hret__); \
            out.ret = RKV_ERR_FROM_MERCURY; \
            return; \
        } \
    } while(0)

#define CHECK_MID(__mid__, __fun__) \
    do { \
        if(__mid__ == MARGO_INSTANCE_NULL) { \
            RKV_LOG_ERROR(__mid__, #__fun__ " returned invalid margo instance"); \
            out.ret = RKV_ERR_INVALID_MID; \
            return; \
        } \
    } while(0)

#define CHECK_PROVIDER(__pr__) \
    do { \
        if(!__pr__) { \
            RKV_LOG_ERROR(mid, "could not find provider"); \
            out.ret = RKV_ERR_INVALID_PROVIDER; \
            return; \
        } \
    } while(0)

#define CHECK_DATABASE(__db__, __id__) \
    do { \
        if(!__db__) { \
            char __db_id_str__[37]; \
            rkv_database_id_to_string(__id__, __db_id_str__); \
            RKV_LOG_ERROR(mid, "Could not find database with id %s", __db_id_str__); \
            out.ret = RKV_ERR_INVALID_DATABASE; \
            return; \
        } \
    } while(0)

#define CHECK_MODE_SUPPORTED(__db__, __mode__) \
    do { \
        if(!__db__->supportsMode(__mode__)) { \
            out.ret = RKV_ERR_MODE; \
            RKV_LOG_ERROR(mid, "Mode not supported by database"); \
            return; \
        } \
    } while(0)

#define CHECK_BUFFER(__buf__) \
    do { \
        if(!__buf__) { \
            RKV_LOG_ERROR(mid, "could not get bulk buffer"); \
            out.ret = RKV_ERR_ALLOCATION; \
            return; \
        } \
    } while(0)

#endif
