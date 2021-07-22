/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __RKV_LOGGING_H
#define __RKV_LOGGING_H

#include <margo.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-zero-variadic-macro-arguments"

#define RKV_LOG_TRACE(__mid__, __msg__, ...) \
    margo_trace(__mid__, "[rkv] %s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)
#define RKV_LOG_DEBUG(__mid__, __msg__, ...) \
    margo_debug(__mid__, "[rkv] %s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)
#define RKV_LOG_INFO(__mid__, __msg__, ...) \
    margo_info(__mid__, "[rkv] %s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)
#define RKV_LOG_WARNING(__mid__, __msg__, ...) \
    margo_warning(__mid__, "[rkv] %s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)
#define RKV_LOG_ERROR(__mid__, __msg__, ...) \
    margo_error(__mid__, "[rkv] %s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)
#define RKV_LOG_CRITICAL(__mid__, __msg__, ...) \
    margo_critical(__mid__, "[rkv] %s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)

#pragma clang diagnostic pop
#endif
