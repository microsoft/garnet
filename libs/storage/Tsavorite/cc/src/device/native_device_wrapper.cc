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
inline INativeDevice* TryConstructDevice(const char* file, uint64_t segment_size, bool omit_segment_id,
                                         bool enablePrivileges, bool unbuffered, bool delete_on_close) {
    try {
        return FinalizeOrSurfaceError(
            new DeviceT(std::string(file), segment_size, omit_segment_id, enablePrivileges, unbuffered, delete_on_close));
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
	/// descriptive error message (segment-size validation failures, OOM, kernel rejection of
	/// io_setup / io_uring_queue_init, missing FASTER_URING support, etc.).
	///
	/// segment_size_bytes MUST be a positive power of two and >= the device sector size; the
	/// native side enforces the power-of-two check (returns nullptr + last_error on violation).
	///
	/// When `omit_segment_id` is true, segment files are named just `<file>` (no `.<idx>`
	/// suffix). Only meaningful in unbounded single-segment mode (caller passes a
	/// segment_size_bytes large enough that only segment 0 is ever addressed, e.g.
	/// 1<<63 from the managed wrapper's `segmentSize = -1` translation). Multiple segments
	/// under omit mode would all resolve to the same path and clobber each other.
	EXPORTED_SYMBOL INativeDevice* NativeDevice_CreateWithBackend(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close, int32_t backend, uint64_t segment_size_bytes, bool omit_segment_id) {
		native_device::clear_last_error();
		if (file == nullptr) {
			native_device::set_last_error("NativeDevice_CreateWithBackend: 'file' argument is null.");
			return nullptr;
		}
		switch (backend) {
			case NativeDeviceBackend_Default:
				return TryConstructDevice<NativeDeviceDefault>(file, segment_size_bytes, omit_segment_id, enablePrivileges, unbuffered, delete_on_close);
#if !defined(_WIN32) && !defined(_WIN64)
			case NativeDeviceBackend_Libaio:
				return TryConstructDevice<NativeDeviceLibaio>(file, segment_size_bytes, omit_segment_id, enablePrivileges, unbuffered, delete_on_close);
#ifdef FASTER_URING
			case NativeDeviceBackend_Uring:
				return TryConstructDevice<NativeDeviceUring>(file, segment_size_bytes, omit_segment_id, enablePrivileges, unbuffered, delete_on_close);
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

	EXPORTED_SYMBOL void NativeDevice_RemoveSegment(INativeDevice* device, uint64_t segment) {
		device->RemoveSegment(segment);
	}

	/// Probes the kernel's required direct-I/O alignment for `filename` WITHOUT creating a
	/// device. The managed wrapper calls this in NativeStorageDevice's constructor so it can
	/// pass the correct SectorSize to StorageDeviceBase before the upper Tsavorite layers
	/// start aligning buffers.
	///
	/// Probe order:
	///   (1) Linux 6.1+ : statx(STATX_DIOALIGN) on the file. If the file doesn't exist, try the
	///       parent directory (then any existing ancestor). Returns max(offset_align, mem_align)
	///       rounded up to a power of two.
	///   (2) Default : 512 — historical Garnet assumption, and the floor that every other
	///       Linux device assumes.
	///
	/// On pre-6.1 kernels (and on filesystems where stx_dio_*_align is 0), the probe returns
	/// 512 and the actual filesystem alignment is rediscovered by File::GetDeviceAlignment
	/// when the device opens the file. If the discovered value differs, the C# wrapper's
	/// cross-check in Initialize throws — fail fast at startup, never silently corrupt I/O.
	/// We deliberately do NOT use statvfs.f_frsize as a "hint" here: f_frsize is the
	/// filesystem's allocation unit (often 4096 on ext4) which has no required relationship
	/// to the DIO alignment requirement (still 512 on a 512n-backed ext4), so it routinely
	/// over-reports and would cause the cross-check to fail on perfectly good hardware.
	///
	/// Returns the alignment in bytes. Always returns a power of two >= 512. Never throws.
	/// Never modifies last_error (no failure modes — worst case is "use 512 and let the
	/// kernel reject the I/O").
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
