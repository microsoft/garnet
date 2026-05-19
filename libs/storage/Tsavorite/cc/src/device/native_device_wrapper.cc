// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "native_device.h"
#include <string>

#if defined(_WIN32) || defined(_WIN64)  
#define EXPORTED_SYMBOL __declspec(dllexport)  
#else  
#define EXPORTED_SYMBOL __attribute__((visibility("default")))  
#endif 

extern "C" {
	/// Legacy entrypoint: creates a device using the platform default backend
	/// (libaio on Linux, ThreadPool on Windows). Preserved for ABI compatibility.
	EXPORTED_SYMBOL INativeDevice* NativeDevice_Create(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close) {
		return new NativeDeviceDefault(std::string(file), enablePrivileges, unbuffered, delete_on_close);
	}

	/// New entrypoint: creates a device using the specified backend. Returns nullptr
	/// if the requested backend is not available (e.g., io_uring on a build without
	/// FASTER_URING, or on Windows).
	EXPORTED_SYMBOL INativeDevice* NativeDevice_CreateWithBackend(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close, int32_t backend) {
		switch (backend) {
			case NativeDeviceBackend_Default:
				return new NativeDeviceDefault(std::string(file), enablePrivileges, unbuffered, delete_on_close);
#if !defined(_WIN32) && !defined(_WIN64)
			case NativeDeviceBackend_Libaio:
				return new NativeDeviceLibaio(std::string(file), enablePrivileges, unbuffered, delete_on_close);
#ifdef FASTER_URING
			case NativeDeviceBackend_Uring:
				return new NativeDeviceUring(std::string(file), enablePrivileges, unbuffered, delete_on_close);
#endif
#endif
			default:
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

	EXPORTED_SYMBOL FASTER::core::Status NativeDevice_ReadAsync(INativeDevice* device, uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
		return device->ReadAsync(source, dest, length, callback, context);
	}

	EXPORTED_SYMBOL FASTER::core::Status NativeDevice_WriteAsync(INativeDevice* device, const void* source, uint64_t dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
		return device->WriteAsync(source, dest, length, callback, context);
	}

	EXPORTED_SYMBOL void NativeDevice_CreateDir(INativeDevice* device, const char* dir) {
		device->CreateDir(std::string(dir));
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
}
