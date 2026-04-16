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
  device/libaio_compat.h
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
   # Search the library's own directory for DT_NEEDED dependencies. This lets the C# loader drop
   # a compat symlink (e.g., libaio.so.1 -> libaio.so.1t64 for the Debian/Ubuntu t64 ABI
   # transition) next to libnative_device.so and have the dynamic linker pick it up without
   # requiring LD_LIBRARY_PATH to be set by the caller. Use RPATH (DT_RPATH) instead of RUNPATH
   # because RPATH is searched before LD_LIBRARY_PATH and applies transitively to all dependency
   # lookups, whereas RUNPATH only applies to the direct library and is overridden by
   # LD_LIBRARY_PATH.
   set_target_properties(native_device PROPERTIES
     INSTALL_RPATH "$ORIGIN"
     BUILD_WITH_INSTALL_RPATH TRUE)
   target_link_options(native_device PRIVATE "LINKER:--disable-new-dtags")
endif()
