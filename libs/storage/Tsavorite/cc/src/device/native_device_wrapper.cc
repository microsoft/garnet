// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "native_device.h"
#include "native_device_error.h"
#include <algorithm>
#include <string>
#include <stdexcept>

#if !defined(_WIN32) && !defined(_WIN64)
#include <experimental/filesystem>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <errno.h>
#endif

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
                                         bool enablePrivileges, bool unbuffered, bool delete_on_close) {
    try {
        return FinalizeOrSurfaceError(
            new DeviceT(std::string(file), segment_size, omit_segment_id, num_io_contexts, enablePrivileges, unbuffered, delete_on_close));
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
	EXPORTED_SYMBOL INativeDevice* NativeDevice_CreateWithBackend(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close, int32_t backend, uint64_t segment_size_bytes, bool omit_segment_id, int32_t num_io_contexts) {
		native_device::clear_last_error();
		if (file == nullptr) {
			native_device::set_last_error("NativeDevice_CreateWithBackend: 'file' argument is null.");
			return nullptr;
		}
		if (num_io_contexts < 1) num_io_contexts = 1;
		switch (backend) {
			case NativeDeviceBackend_Default:
				return TryConstructDevice<NativeDeviceDefault>(file, segment_size_bytes, omit_segment_id, num_io_contexts, enablePrivileges, unbuffered, delete_on_close);
#if !defined(_WIN32) && !defined(_WIN64)
			case NativeDeviceBackend_Libaio:
				return TryConstructDevice<NativeDeviceLibaio>(file, segment_size_bytes, omit_segment_id, num_io_contexts, enablePrivileges, unbuffered, delete_on_close);
#ifdef FASTER_URING
			case NativeDeviceBackend_Uring:
				return TryConstructDevice<NativeDeviceUring>(file, segment_size_bytes, omit_segment_id, num_io_contexts, enablePrivileges, unbuffered, delete_on_close);
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
		delete device;
	}

	EXPORTED_SYMBOL void NativeDevice_Reset(INativeDevice* device) {
		device->Reset();
	}

	EXPORTED_SYMBOL uint32_t NativeDevice_sector_size(INativeDevice* device) {
		return device->sector_size();
	}

	/// Reads back the segment size the native device was built with. The managed caller uses
	/// this to assert that what it passed to NativeDevice_CreateWithBackend round-trips
	/// correctly (defense in depth against ABI mismatches between the prebuilt .so and the
	/// C# wrapper).
	EXPORTED_SYMBOL uint64_t NativeDevice_GetSegmentSize(INativeDevice* device) {
		return device->segment_size_bytes();
	}

	EXPORTED_SYMBOL FASTER::core::Status NativeDevice_ReadAsync(INativeDevice* device, uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
		return device->ReadAsync(source, dest, length, callback, context);
	}

	EXPORTED_SYMBOL FASTER::core::Status NativeDevice_WriteAsync(INativeDevice* device, const void* source, uint64_t dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
		return device->WriteAsync(source, dest, length, callback, context);
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
		return device->CreateDir(std::string(dir), delete_existing != 0);
	}

	EXPORTED_SYMBOL bool NativeDevice_TryComplete(INativeDevice* device) {
		return device->TryComplete();
	}

	EXPORTED_SYMBOL uint64_t NativeDevice_GetFileSize(INativeDevice* device, uint64_t segment) {
		return device->GetFileSize(segment);
	}

	EXPORTED_SYMBOL int NativeDevice_QueueRun(INativeDevice* device, int timeout_secs) {
		return device->QueueRun(timeout_secs);
	}

	/// Per-context drain. ctx_idx must be in [0, NativeDevice_NumIoContexts(device)). Used
	/// by completion threads bound 1:1 to a shard. Returns -1 if ctx_idx is out of range.
	EXPORTED_SYMBOL int NativeDevice_QueueRunFor(INativeDevice* device, int ctx_idx, int timeout_secs) {
		if (device == nullptr) return -1;
		return device->QueueRunFor(ctx_idx, timeout_secs);
	}

	/// Number of submission/completion shards for `device`. >= 1.
	EXPORTED_SYMBOL int NativeDevice_NumIoContexts(INativeDevice* device) {
		if (device == nullptr) return 0;
		return device->num_io_contexts();
	}

	EXPORTED_SYMBOL void NativeDevice_RemoveSegment(INativeDevice* device, uint64_t segment) {
		device->RemoveSegment(segment);
	}

	/// Returns the kernel's required direct-I/O alignment for `filename` (or the closest
	/// existing ancestor) on Linux 6.1+ kernels that populate statx(STATX_DIOALIGN); returns
	/// 512 otherwise. Always returns a power of two >= 512. Never throws or sets last_error.
	/// The C# wrapper uses this to size SectorSize before any I/O is issued; mismatches with
	/// the per-file alignment discovered at open() time are caught by the wrapper's
	/// Initialize cross-check.
	EXPORTED_SYMBOL uint32_t NativeDevice_ProbeAlignment(const char* filename) {
		if (filename == nullptr || *filename == '\0') return 512u;
		uint32_t result = 512u;
#if !defined(_WIN32) && !defined(_WIN64)
		namespace fs = std::experimental::filesystem;
		std::error_code ec;
		fs::path target{ filename };

		// Walk from the requested path up to an existing ancestor. statx requires an existing
		// path; in normal startup the file may not exist yet, but its parent dir was created
		// by the managed ctor moments before this probe runs.
		auto resolve_existing = [&](fs::path p) -> std::string {
			std::error_code e;
			for (;;) {
				if (fs::exists(p, e)) return p.string();
				auto parent = p.parent_path();
				if (parent.empty() || parent == p) return std::string{};
				p = parent;
			}
		};
		std::string probe_path = resolve_existing(target);
		if (probe_path.empty()) return 512u;

#if defined(STATX_DIOALIGN)
		// Try statx on the resolved path. Opening read-only (without O_DIRECT) is enough —
		// statx STATX_DIOALIGN reads the kernel's authoritative alignment from the FS, not
		// from open() flags.
		int fd = ::open(probe_path.c_str(), O_RDONLY | O_CLOEXEC);
		if (fd >= 0) {
			struct statx stx{};
			int rc = ::statx(fd, "", AT_EMPTY_PATH, STATX_DIOALIGN, &stx);
			::close(fd);
			if (rc == 0) {
				uint32_t required = std::max(stx.stx_dio_offset_align, stx.stx_dio_mem_align);
				if (required != 0) {
					// Round up to a power of two.
					uint32_t pow2 = 512u;
					while (pow2 < required) pow2 <<= 1;
					return std::max(result, pow2);
				}
			}
		}
#endif
#endif
		return result;
	}
}
