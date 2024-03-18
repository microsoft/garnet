# Build the native_device library.

set (NATIVE_DEVICE_HEADERS
  device/alloc.h
  device/async.h
  device/auto_ptr.h
  device/constants.h
  device/file.h
  device/file_common.h
  device/file_system_disk.h
  device/gc_state.h
  device/guid.h
  device/light_epoch.h
  device/lss_allocator.h
  device/native_device.h
  device/phase.h
  device/status.h
  device/thread.h
  device/utility.h
)

if (MSVC)
set (NATIVE_DEVICE_HEADERS ${NATIVE_DEVICE_HEADERS}
  device/file_windows.h
)
else()
set (FNATIVE_DEVICE_HEADERS ${NATIVE_DEVICE_HEADERS}
  device/file_linux.h
)
endif() 

set (NATIVE_DEVICE_SOURCES
  device/lss_allocator.cc
  device/native_device_wrapper.cc
  device/thread_manual.cc
)

if (MSVC)
set (NATIVE_DEVICE_SOURCES ${NATIVE_DEVICE_SOURCES}
  device/file_windows.cc
)
else()
set (NATIVE_DEVICE_SOURCES ${NATIVE_DEVICE_SOURCES}
  device/file_linux.cc
)
endif()

add_library(native_device SHARED ${NATIVE_DEVICE_SOURCES} ${NATIVE_DEVICE_HEADERS})
if (UNIX)
   target_link_libraries(native_device PRIVATE stdc++fs aio)
endif()
