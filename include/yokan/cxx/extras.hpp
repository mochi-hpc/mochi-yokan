/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __YOKAN_CXX_EXTRAS_HPP
#define __YOKAN_CXX_EXTRAS_HPP

#include <yokan/common.h>
#include <type_traits>
#include <utility>

namespace yokan {

/**
 * @brief Per-call timeout, in milliseconds.
 *
 * 0.0 means "blocking forever" (the default). Pass an instance of this
 * struct after the `mode` argument of any Database or Collection method to
 * cap the RPC duration.
 *
 * Example:
 *   db.put(k, ks, v, vs, YOKAN_MODE_DEFAULT, yokan::Timeout{500.0});
 */
struct Timeout {
    double ms = 0.0;
    constexpr Timeout() = default;
    constexpr explicit Timeout(double m) : ms(m) {}
};

namespace detail {

/**
 * @brief Return the value of the first Extra of type T in the pack, or
 * a default-constructed T if no such extra was supplied.
 */
template <typename T>
constexpr T extract_extra() { return T{}; }

template <typename T, typename First, typename... Rest>
constexpr T extract_extra(First&& first, Rest&&... rest) {
    if constexpr (std::is_same_v<std::decay_t<First>, T>)
        return std::forward<First>(first);
    else
        return extract_extra<T>(std::forward<Rest>(rest)...);
}

/**
 * @brief Compile-time check that every type in the pack is one of the
 * Yokan-recognized extra types. Update this list when a new extra is added.
 */
template <typename... Extras>
constexpr void check_known_extras() {
    static_assert(
        (std::is_same_v<std::decay_t<Extras>, Timeout> && ...),
        "Unsupported extra passed to a Yokan wrapper method. "
        "Allowed extras: yokan::Timeout.");
}

} // namespace detail
} // namespace yokan

#endif
