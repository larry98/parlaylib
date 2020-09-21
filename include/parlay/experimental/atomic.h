
#ifndef PARLAY_EXPERIMENTAL_ATOMIC_H_
#define PARLAY_EXPERIMENTAL_ATOMIC_H_

#include <cstdint>
#include <cstring>

#if defined(__GNUC__) && defined(MCX16)
#define PARLAY_EXPERIMENTAL_GNU_CAS
#endif

#if defined(_WIN32) || defined(__WIN32__) || defined(WIN32)
#include <intrin.h>
#define PARLAY_EXPERIMENTAL_WINDOWS_CAS
#endif

#include <atomic>

namespace parlay {
namespace experimental {

template<typename T>
bool atomic_compare_and_swap_16(T* obj, T expected, T desired) noexcept {
    static_assert(sizeof(T) == 16);
// If MCX16 is enabled, use a fast 16 byte CAS
#if defined(PARLAY_EXPERIMENTAL_GNU_CAS)
    __int128 expected_bits, desired_bits;
    std::memcpy(&expected_bits, &expected, sizeof(T));
    std::memcpy(&desired_bits, &desired, sizeof(T));
    return __sync_bool_compare_and_swap_16(reinterpret_cast<__int128*>(obj), expected_bits, desired_bits);
#elif defined(PARLAY_EXPERIMENTAL_WINDOWS_CAS)
    __int64 expected_bits[2];
    __int64 desired_bits[2];
    std::memcpy(&expected_bits, &expected, sizeof(T));
    std::memcpy(&desired_bits, &desired, sizeof(T));
    return _InterlockedCompareExchange128(reinterpret_cast<__int64 volatile*>(obj), desired_bits[1], desired_bits[0], reinterpret_cast<__int64*>(expected_bits));
#else
    // No 16-byte CAS available
#endif
}

}
}

#endif  // PARLAY_EXPERIMENTAL_ATOMIC_H_
