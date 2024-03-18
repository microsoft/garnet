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
	EXPORTED_SYMBOL NativeDevice* NativeDevice_Create(const char* file, bool enablePrivileges, bool unbuffered, bool delete_on_close) {
		return new NativeDevice(std::string(file), enablePrivileges, unbuffered, delete_on_close);
	}

	EXPORTED_SYMBOL void NativeDevice_Destroy(NativeDevice* device) {
		delete device;
	}

	EXPORTED_SYMBOL void NativeDevice_Reset(NativeDevice* device) {
		device->Reset();
	}

	EXPORTED_SYMBOL uint32_t NativeDevice_sector_size(NativeDevice* device) {
		return device->sector_size();
	}

	EXPORTED_SYMBOL FASTER::core::Status NativeDevice_ReadAsync(NativeDevice* device, uint64_t source, void* dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
		return device->ReadAsync(source, dest, length, callback, context);
	}

	EXPORTED_SYMBOL FASTER::core::Status NativeDevice_WriteAsync(NativeDevice* device, const void* source, uint64_t dest, uint32_t length, FASTER::core::AsyncIOCallback callback, void* context) {
		return device->WriteAsync(source, dest, length, callback, context);
	}

	EXPORTED_SYMBOL void NativeDevice_CreateDir(NativeDevice* device, const char* dir) {
		device->CreateDir(std::string(dir));
	}

	EXPORTED_SYMBOL bool NativeDevice_TryComplete(NativeDevice* device) {
		return device->TryComplete();
	}

	EXPORTED_SYMBOL uint64_t NativeDevice_GetFileSize(NativeDevice* device, uint64_t segment) {
		return device->GetFileSize(segment);
	}

	EXPORTED_SYMBOL int NativeDevice_QueueRun(NativeDevice* device, int timeout_secs) {
		return device->QueueRun(timeout_secs);
	}

	EXPORTED_SYMBOL void NativeDevice_RemoveSegment(NativeDevice* device, uint64_t segment) {
		device->RemoveSegment(segment);
	}
}