/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_LOGGING_H
#define __YOKAN_LOGGING_H

#include <margo.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-zero-variadic-macro-arguments"
#endif

#define YOKAN_LOG_TRACE(__mid__, __msg__, ...) \
    margo_trace(__mid__, "[yokan] %s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)
#define YOKAN_LOG_DEBUG(__mid__, __msg__, ...) \
    margo_debug(__mid__, "[yokan] %s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)
#define YOKAN_LOG_INFO(__mid__, __msg__, ...) \
    margo_info(__mid__, "[yokan] " __msg__, ##__VA_ARGS__)
#define YOKAN_LOG_WARNING(__mid__, __msg__, ...) \
    margo_warning(__mid__, "[yokan] %s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)
#define YOKAN_LOG_ERROR(__mid__, __msg__, ...) \
    margo_error(__mid__, "[yokan] %s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)
#define YOKAN_LOG_CRITICAL(__mid__, __msg__, ...) \
    margo_critical(__mid__, "[yokan] %s:%d: " __msg__, __func__, __LINE__, ##__VA_ARGS__)

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#endif
