if (UNIX)
    include(CMakePrintHelpers)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(spdk_lib spdk_nvme spdk_env_dpdk REQUIRED)
    pkg_check_modules(spdk_syslibs spdk_syslibs REQUIRED)

    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -g -D_DEBUG")
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -O3 -g")

    set (SPDK_DEVICE_HEADERS
      spdk_device.h
    )

    set (SPDK_DEVICE_SOURCES
      spdk_device.c
    )

    cmake_print_variables(spdk_lib_LDFLAGS)
    cmake_print_variables(spdk_syslibs_STATIC_LDFLAGS)
    add_library(spdk_device SHARED ${SPDK_DEVICE_SOURCES} ${SPDK_DEVICE_HEADERS})
    set_property(TARGET spdk_device PROPERTY C_STANDARD 11)
    set_target_properties(spdk_device PROPERTIES LINKER_LANGUAGE C)
    target_link_libraries(spdk_device PRIVATE "-Wl,-Bstatic" "-Wl,--whole-archive" ${spdk_lib_LDFLAGS})
    target_link_libraries(spdk_device PRIVATE "-Wl,-Bdynamic" "-Wl,--no-whole-archive" ${spdk_syslibs_STATIC_LDFLAGS})

    cmake_print_variables(CMAKE_CURRENT_SOURCE_DIR)
    add_custom_command(
      TARGET spdk_device POST_BUILD
      COMMAND cp "$<TARGET_FILE:spdk_device>" "${CMAKE_CURRENT_SOURCE_DIR}/../../../cs/src/core/Device/runtimes/linux-x64/native/"
    )

    find_package(Threads REQUIRED)
    add_executable(spdk_device_test_main test_main.c)
    add_dependencies(spdk_device_test_main spdk_device)
    set_target_properties(spdk_device_test_main PROPERTIES LINKER_LANGUAGE C)
    set_property(TARGET spdk_device_test_main PROPERTY C_STANDARD 11)
    target_link_libraries(spdk_device_test_main spdk_device Threads::Threads)
endif()