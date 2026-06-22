/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_BULK_TIMEOUT_H
#define __YOKAN_BULK_TIMEOUT_H

#include <abt.h>

/* Compute the timeout (in milliseconds) to pass to a margo_bulk_*_timed
 * call inside a server RPC handler.
 *
 * Convention: the client tells the server an overall deadline of T ms for
 * the whole RPC; the server allots T/2 of that to the bulk-transfer
 * portion, and the remainder is implicitly left for backend work and
 * sending back the response. Each bulk transfer in the same handler gets
 * whatever wall-clock time is left in that T/2 budget.
 *
 * Special cases:
 *   - rpc_timeout_ms == 0.0 means "no client deadline" — return 0.0, which
 *     margo interprets as "block forever".
 *   - if the budget is already exhausted by the time we get here, return a
 *     tiny positive value so margo trips HG_TIMEOUT promptly rather than
 *     hanging or interpreting 0/negative as "no timeout".
 *
 * @param rpc_timeout_ms  in.timeout_ms (the client-supplied deadline)
 * @param t_start         ABT_get_wtime() snapshot taken at the top of the
 *                        handler, in seconds
 */
static inline double yk_bulk_timeout_ms(double rpc_timeout_ms, double t_start)
{
    if (rpc_timeout_ms == 0.0) return 0.0;
    const double budget_ms  = rpc_timeout_ms * 0.5;
    const double elapsed_ms = (ABT_get_wtime() - t_start) * 1000.0;
    const double remaining  = budget_ms - elapsed_ms;
    return remaining > 0.0 ? remaining : 0.001;
}

#endif
