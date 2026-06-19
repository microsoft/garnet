// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

// Thread-local last-error channel surfaced across the C ABI. Init / cold-path code paths call
// set_last_error() with an actionable message; NativeDevice_GetLastError() returns the storage
// to the managed caller, which copies it into the TsavoriteException it throws.
//
// HOT-PATH DISCIPLINE: set_last_error MUST NOT be called from ScheduleOperation,
// IoCompletionCallback, DispatchUringCqe, or any code path executed once-per-IO. The vsnprintf +
// std::string assignment is far too expensive for the IO path. Reserve this channel for init
// (io_setup, io_uring_queue_init), File::Open, and other per-device cold paths.
//
// THREAD MODEL: storage is thread_local. NativeDevice_CreateWithBackend (etc.) must clear the
// error at entry and set it on every null-return path so the managed caller can read it on the
// SAME thread that called CreateWithBackend (which is guaranteed for synchronous P/Invoke).

#include <cstdarg>
#include <cstdio>
#include <string>

namespace native_device {

inline std::string& last_error_storage() {
    static thread_local std::string s;
    return s;
}

inline void clear_last_error() {
    last_error_storage().clear();
}

inline const char* get_last_error() {
    return last_error_storage().c_str();
}

inline void set_last_error(const char* fmt, ...) noexcept
#ifdef __GNUC__
    __attribute__((format(printf, 1, 2)))
#endif
;

// noexcept: error reporting is best-effort and must never itself unwind. This function is
// called from the C ABI exception firewall (native_device_wrapper.cc) and from device
// construction error paths — including the std::bad_alloc path — so the assignment to the
// thread-local std::string (which can throw bad_alloc) is wrapped and swallowed. A dropped
// message is acceptable; an exception escaping across the extern "C"/P-Invoke boundary is not.
inline void set_last_error(const char* fmt, ...) noexcept {
    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    (void)vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    try {
        last_error_storage() = buf;
    } catch (...) {
        // Best-effort: if recording the message allocates and fails, drop it rather than
        // letting the exception escape the firewall.
    }
}

}  // namespace native_device
