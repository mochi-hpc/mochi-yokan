/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef _YOKAN_EXTRAS_H
#define _YOKAN_EXTRAS_H

#include <stdarg.h>
#include "yokan/common.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Holds the values of any extra options parsed out of the variadic
 * tail of a yk_* function call. Fields are initialized to their no-op
 * defaults, so a struct that has not been touched by yk_extras_drain
 * behaves as if YOKAN_MODE_EXTRA had not been set.
 */
typedef struct yk_extra_opts {
    double timeout_ms; /* 0.0 = blocking forever */
} yk_extra_opts_t;

#define YK_EXTRA_OPTS_INIT { 0.0 }

/**
 * @brief Drain a va_list of (tag, value)... pairs terminated by
 * YOKAN_EXTRA_END, writing recognized options into *out. Unknown tags
 * are best-effort skipped by consuming a single pointer-sized va_arg
 * (so callers should keep all option values to <= sizeof(void*) or
 * one of the explicitly-handled types below). The caller is responsible
 * for va_start/va_end.
 *
 * Callers MUST only invoke this when YOKAN_MODE_EXTRA was set in mode,
 * otherwise the va_list is not guaranteed to hold any tag.
 */
static inline void yk_extras_drain(va_list ap, yk_extra_opts_t* out)
{
    for (;;) {
        int tag = va_arg(ap, int);
        if (tag == YOKAN_EXTRA_END)
            return;
        switch (tag) {
        case YOKAN_EXTRA_TIMEOUT_MS:
            out->timeout_ms = va_arg(ap, double);
            break;
        default:
            (void)va_arg(ap, void*);
            break;
        }
    }
}

/**
 * @brief Convenience macro: declare a local yk_extra_opts_t named `name`,
 * default-initialize it, and if mode has YOKAN_MODE_EXTRA set, drain the
 * variadic tail starting after `last_named_param` into it.
 *
 * Usage at the top of a yk_* function:
 *   YK_EXTRACT_EXTRAS(extras, mode, vsize);
 *   ...
 *   in.timeout_ms = extras.timeout_ms;
 */
#define YK_EXTRACT_EXTRAS(name, mode_var, last_named_param)             \
    yk_extra_opts_t name = YK_EXTRA_OPTS_INIT;                          \
    do {                                                                \
        if ((mode_var) & YOKAN_MODE_EXTRA) {                            \
            va_list _yk_ap;                                             \
            va_start(_yk_ap, last_named_param);                         \
            yk_extras_drain(_yk_ap, &name);                             \
            va_end(_yk_ap);                                             \
        }                                                               \
    } while (0)

/**
 * @brief When a public yk_* function delegates to another public yk_*
 * function and wants to preserve the caller's extra options, expand this
 * macro as the trailing arguments of the inner call. The inner mode must
 * have YOKAN_MODE_EXTRA set (use YK_MODE_WITH_EXTRA(mode)).
 *
 * Example:
 *   YK_EXTRACT_EXTRAS(extras, mode, vsize);
 *   return yk_put_packed(dbh, YK_MODE_WITH_EXTRA(mode), 1, key, &ksize,
 *                        value, &vsize, YK_REEMIT_EXTRAS(extras));
 */
#define YK_REEMIT_EXTRAS(extras) \
    YOKAN_EXTRA_TIMEOUT_MS, (extras).timeout_ms, YOKAN_EXTRA_END

#define YK_MODE_WITH_EXTRA(mode_var) ((mode_var) | YOKAN_MODE_EXTRA)

#ifdef __cplusplus
}
#endif

#endif
