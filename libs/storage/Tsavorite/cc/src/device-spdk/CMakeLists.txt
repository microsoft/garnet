if (UNIX)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(spdk_nvme REQUIRED)
    pkg_check_modules(spdk_env_dpdk REQUIRED)
    pkg_check_modules(spdk_syslibs REQUIRED)

    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -g -D_DEBUG")
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -g")

    set (SPDK_DEVICE_HEADERS
      spdk_device.h
    )

    set (SPDK_DEVICE_SOURCES
      spdk_device.c
    )

    add_library(spdk_device SHARED ${SPDK_DEVICE_SOURCES} ${SPDK_DEVICE_HEADERS})

    set_property(TARGET spdk_device PROPERTY C_STANDARD 11)
    target_compile_options(spdk_device PRIVATE "-Wl,--Bstatic")
    target_compile_options(spdk_device PRIVATE "-Wl,--whole-archive")
    target_link_libraries(spdk_device ${spdk_nvme_LIBRARIES} ${spdk_env_dpdk_LIBRARIES})
    target_compile_options(spdk_device PRIVATE "-Wl,-Bdynamic")
    target_compile_options(spdk_device PRIVATE "-Wl,--no-whole-archive")
    target_link_libraries(spdk_device ${spdk_syslibs_STATIC_LIBRARIES})
endif()