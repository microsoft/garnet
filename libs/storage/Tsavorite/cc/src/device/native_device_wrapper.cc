// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "native_device.h"
#include "native_device_error.h"
#include <algorithm>
#include <limits>
#include <string>
#include <stdexcept>
#include <utility>

#if defined(_WIN32) || defined(_WIN64)  
#define EXPORTED_SYMBOL __declspec(dllexport)  
#else  
#define EXPORTED_SYMBOL __attribute__((visibility("default")))  
#endif 

namespace {
/// Wraps a freshly-`new`-ed concrete NativeDeviceImpl<T>. If init failed inside the ctor,
/// surfaces the thread-local error message (already populated by the ctor), deletes the
/// instance, and returns nullptr. Otherwise upcasts to INativeDevice* for opaque handoff to
/// the managed caller.
template <typename DeviceT>
inline INativeDevice* FinalizeOrSurfaceError(DeviceT* device) {
    if (device == nullptr) {
        native_device::set_last_error("Native device allocation returned null.");
        return nullptr;
    }
    if (device->init_status() != FASTER::core::Status::Ok) {
        delete device;
        return nullptr;
    }
    return device;
}

/// Constructs a typed NativeDeviceImpl<T> with the requested segment size. The
/// FileSystemSegmentedFile ctor inside throws std::invalid_argument if segment_size is not a
/// positive power of two; catch it here so the C ABI never propagates a C++ exception. Other
/// exceptions (bad_alloc, etc.) are converted similarly.
template <typename DeviceT>
inline INativeDevice* TryConstructDevice(const char* file, uint64_t segment_size, bool omit_segment_id, int num_io_contexts,
                                         bool enablePrivileges, bool unbuffered, bool delete_on_close, int max_events) {
    try {
        return FinalizeOrSurfaceError(
            new DeviceT(std::string(file), segment_size, omit_segment_id, num_io_contexts, enablePrivileges, unbuffered, delete_on_close, max_events));
    } catch (const std::invalid_argument& e) {
        native_device::set_last_error("Invalid argument: %s", e.what());
        return nullptr;
    } catch (const std::bad_alloc&) {
        native_device::set_last_error("Out of memory allocating native device.");
        return nullptr;
    } catch (const std::exception& e) {
        native_device::set_last_error("Native device construction threw std::exception: %s", e.what());
        return nullptr;
    } catch (...) {
        native_device::set_last_error("Native device construction threw unknown exception.");
        return nullptr;
    }
}

/// Exception firewall for the C ABI. No C++ exception may unwind across the extern "C" boundary
/// into the .NET P/Invoke caller: on Linux that is undefined behavior and in practice calls
/// std::terminate(), which aborts/hangs the process — the "server won't shut down cleanly"
/// symptom, with no actionable message for the user to report. Every entry point routes its body
/// through one of these guards, which converts any escaped exception into (a) a populated
/// thread-local last_error (retrievable via NativeDevice_GetLastError, so the managed wrapper can
/// log it) and (b) a safe sentinel return, so the operation fails loudly instead of crashing.
///
/// The guards are nothrow: recording the message is itself wrapped so a (vanishingly unlikely)
/// allocation failure while building the string cannot re-escape. Cost is zero on the success
/// path (Itanium zero-cost exceptions); set_last_error runs only on the cold catch path, honoring
/// native_device_error.h's "never call set_last_error per-IO" discipline.

/// Sentinel returned by int-valued entry points whose normal failure code (-1) must remain
/// distinguishable from "the native call threw" so the managed side can log the message. Kept in
/// sync with NativeStorageDevice.NativeCABIExceptionSentinel.
constexpr int kCABIExceptionSentinel = (std::numeric_limits<int>::min)();

inline void record_cabi_exception(const char* fn_name, const char* what) noexcept {
    try {
        if (what != nullptr)
            native_device::set_last_error("%s threw an unhandled C++ exception: %s", fn_name, what);
        else
            native_device::set_last_error("%s threw an unhandled, non-std C++ exception.", fn_name);
    } catch (...) {
        // Recording the message must never itself escape the firewall.
    }
}

template <typename Fn>
inline auto CABIGuard(const char* fn_name, Fn&& body, decltype(std::declval<Fn>()()) sentinel) noexcept
    -> decltype(std::declval<Fn>()()) {
    try {
        return body();
    } catch (const std::exception& e) {
        record_cabi_exception(fn_name, e.what());
        return sentinel;
    } catch (...) {
        record_cabi_exception(fn_name, nullptr);
        return sentinel;
    }
}

template <typename Fn>
inline void CABIGuardVoid(const char* fn_name, Fn&& body) noexcept {
    try {
        body();
    } catch (const std::exception& e) {
        record_cabi_exception(fn_name, e.what());
    } catch (...) {
        record_cabi_exception(fn_name, nullptr);
    }
}
} // anonymous namespace

extern "C" {
	/// Returns the most recent error message produced by this thread inside a native_device
	/// API call. The returned pointer references thread-local storage owned by the native
	/// library; the managed caller must copy the string before invoking any other
	/// native_device API on the same thread. Returns "" (never nullptr) when there is no
	/// error to report.
	EXPORTED_SYMBOL const char* NativeDevice_GetLastError() {
		return native_device::get_last_error();
	}

	/// Creates a device using the specified backend with the requested segment size. Returns
	/// nullptr on any failure; call NativeDevice_GetLastError() on this thread to retrieve a
	/// descriptive error message (segment-size validation, OOM, kernel rejection of
	/// io_setup / io_uring_queue_init, missing FASTER_URING support, etc.).
	///
	/// `segment_size_bytes` must be a positive power of two and >= the device sector size;
	/// the native side enforces the power-of-two check.
	///
	/// When `omit_segment_id` is true, segment files are named `<file>` with no `.<idx>`
	/// suffix; only meaningful in unbounded single-segment mode (segment_size_bytes large
	/// enough that only segment 0 is ever addressed).
	///
	/// `num_io_contexts` selects the number of independent native IO contexts the backend
	/// creates per device. Callers with N>1 MUST drive completion via
	/// NativeDevice_QueueRunFor(device, ctx_idx, timeout) with one drainer thread per
	/// context (0..N-1); NativeDevice_QueueRun (without ctx_idx) is a compat scanner that
	/// drains all contexts. Ignored on Windows (IOCP).
	///
	/// `max_events` is the per-context kernel submission-ring depth (libaio io_setup maxevents
	/// / io_uring SQ entries). Callers size it up from the device throttle limit (the managed
	/// wrapper passes NextPowerOf2(throttle_limit)) so the ring can absorb the full in-flight
	/// burst the throttle permits, keeping io_submit / io_uring_get_sqe off their ring-full
	/// backoff spins. Values <= 0, or below the backend's floor, fall back to the default depth.
	/// Ignored on Windows (IOCP has no fixed submission-ring depth).
	EXPORTED_SYMBOL INativeDevice* NativeDevice_CreateWithBackend(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close, int32_t backend, uint64_t segment_size_bytes, bool omit_segment_id, int32_t num_io_contexts, int32_t max_events) {
		native_device::clear_last_error();
		if (file == nullptr) {
			native_device::set_last_error("NativeDevice_CreateWithBackend: 'file' argument is null.");
			return nullptr;
		}
		if (num_io_contexts < 1) num_io_contexts = 1;
		switch (backend) {
			case NativeDeviceBackend_Default:
				return TryConstructDevice<NativeDeviceDefault>(file, segment_size_bytes, omit_segment_id, num_io_contexts, enablePrivileges, unbuffered, delete_on_close, max_events);
#if !defined(_WIN32) && !defined(_WIN64)
			case NativeDeviceBackend_Libaio:
				return TryConstructDevice<NativeDeviceLibaio>(file, segment_size_bytes, omit_segment_id, num_io_contexts, enablePrivileges, unbuffered, delete_on_close, max_events);
#ifdef FASTER_URING
			case NativeDeviceBackend_Uring:
				return TryConstructDevice<NativeDeviceUring>(file, segment_size_bytes, omit_segment_id, num_io_contexts, enablePrivileges, unbuffered, delete_on_close, max_events);
#endif
#endif
			default:
				native_device::set_last_error("NativeDevice_CreateWithBackend: unknown or unavailable backend id %d.", backend);
				return nullptr;
		}
	}

	/// Reports which backends the loaded native library was built with.
	/// Bit 0 = libaio/default available, Bit 1 = io_uring available.
	EXPORTED_SYMBOL int32_t NativeDevice_AvailableBackends() {
		int32_t mask = 1; // default is always available
#if !defined(_WIN32) && !defined(_WIN64)
#ifdef FASTER_URING
		mask |= 2;
#endif
#endif
		return mask;
	}

	EXPORTED_SYMBOL void NativeDevice_Destroy(INativeDevice* device) {
		CABIGuardVoid("NativeDevice_Destroy", [&]() { delete device; });
	}

	EXPORTED_SYMBOL void NativeDevice_Reset(INativeDevice* device) {
		CABIGuardVoid("NativeDevice_Reset", [&]() { device->Reset(); });
	}

	EXPORTED_SYMBOL uint32_t NativeDevice_sector_size(INativeDevice* device) {
		return CABIGuard("NativeDevice_sector_size", [&]() { return device->sector_size(); }, 0u);
	}

	/// Reads back the segment size the native device was built with. The managed caller uses
	/// this to assert that what it passed to NativeDevice_CreateWithBackend round-trips
	/// correctly (defense in depth against ABI mismatches between the prebuilt .so and the
	/// C# wrapper).
	EXPORTED_SYMBOL uint64_t NativeDevice_GetSegmentSize(INativeDevice* device) {
		return CABIGuard("NativeDevice_GetSegmentSize", [&]() { return device->segment_size_bytes(); }, uint64_t{ 0 });
	}

	EXPORTED_SYMBOL FASTER::core::Status NativeDevice_ReadAsync(INativeDevice* device, uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
		return CABIGuard("NativeDevice_ReadAsync",
			[&]() { return device->ReadAsync(source, dest, length, callback, context); },
			FASTER::core::Status::IOError);
	}

	EXPORTED_SYMBOL FASTER::core::Status NativeDevice_WriteAsync(INativeDevice* device, const void* source, uint64_t dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
		return CABIGuard("NativeDevice_WriteAsync",
			[&]() { return device->WriteAsync(source, dest, length, callback, context); },
			FASTER::core::Status::IOError);
	}

	EXPORTED_SYMBOL int NativeDevice_CreateDir(INativeDevice* device, const char* dir, int delete_existing) {
		native_device::clear_last_error();
		if (device == nullptr) {
			native_device::set_last_error("NativeDevice_CreateDir: 'device' argument is null.");
			return -1;
		}
		if (dir == nullptr) {
			native_device::set_last_error("NativeDevice_CreateDir: 'dir' argument is null.");
			return -1;
		}
		return CABIGuard("NativeDevice_CreateDir",
			[&]() { return device->CreateDir(std::string(dir), delete_existing != 0); }, -1);
	}

	EXPORTED_SYMBOL bool NativeDevice_TryComplete(INativeDevice* device) {
		return CABIGuard("NativeDevice_TryComplete", [&]() { return device->TryComplete(); }, false);
	}

	EXPORTED_SYMBOL uint64_t NativeDevice_GetFileSize(INativeDevice* device, uint64_t segment) {
		return CABIGuard("NativeDevice_GetFileSize", [&]() { return device->GetFileSize(segment); }, uint64_t{ 0 });
	}

	EXPORTED_SYMBOL int NativeDevice_QueueRun(INativeDevice* device, int timeout_secs) {
		return CABIGuard("NativeDevice_QueueRun", [&]() { return device->QueueRun(timeout_secs); }, -1);
	}

	/// Per-context drain. ctx_idx must be in [0, NativeDevice_NumIoContexts(device)). Used
	/// by completion threads bound 1:1 to a shard. Returns -1 if ctx_idx is out of range, or
	/// kCABIExceptionSentinel if the drain threw (the managed completion worker logs the
	/// NativeDevice_GetLastError message in that case).
	EXPORTED_SYMBOL int NativeDevice_QueueRunFor(INativeDevice* device, int ctx_idx, int timeout_secs) {
		if (device == nullptr) return -1;
		return CABIGuard("NativeDevice_QueueRunFor",
			[&]() { return device->QueueRunFor(ctx_idx, timeout_secs); }, kCABIExceptionSentinel);
	}

	/// Same as NativeDevice_QueueRunFor but submits a no-op completion event that wakes
	/// any thread blocked in QueueRunFor on context `ctx_idx`. Used by NSD.Dispose() to
	/// unblock the completion drainer immediately rather than wait on its timeout.
	/// Returns 0 on success, -1 on failure.
	EXPORTED_SYMBOL int NativeDevice_WakeCompletionWorker(INativeDevice* device, int ctx_idx) {
		if (device == nullptr) return -1;
		return CABIGuard("NativeDevice_WakeCompletionWorker", [&]() { return device->Wake(ctx_idx); }, -1);
	}

	/// Number of submission/completion shards for `device`. >= 1.
	EXPORTED_SYMBOL int NativeDevice_NumIoContexts(INativeDevice* device) {
		if (device == nullptr) return 0;
		return CABIGuard("NativeDevice_NumIoContexts", [&]() { return device->num_io_contexts(); }, 0);
	}

	EXPORTED_SYMBOL void NativeDevice_RemoveSegment(INativeDevice* device, uint64_t segment) {
		CABIGuardVoid("NativeDevice_RemoveSegment", [&]() { device->RemoveSegment(segment); });
	}

	/// Returns the kernel's required direct-I/O alignment for `filename` (or the closest
	/// existing ancestor) on Linux 6.1+ kernels that populate statx(STATX_DIOALIGN); returns
	/// 512 otherwise. Always returns a power of two >= 512. Never sets last_error. The C#
	/// wrapper uses this to size SectorSize before any I/O is issued; the same probe
	/// is run by NativeDeviceImpl's ctor so its sector_size() reports an identical value,
	/// giving the wrapper's cross-check a real signal (any disagreement implies ABI / loaded-
	/// library drift, not a 4K-disk false positive). Best-effort: any unexpected exception is
	/// swallowed and falls back to 512 rather than unwinding across the P/Invoke boundary.
	EXPORTED_SYMBOL uint32_t NativeDevice_ProbeAlignment(const char* filename) {
		try {
			return native_device::ProbeDioAlignment(filename);
		} catch (...) {
			return 512;
		}
	}
}
