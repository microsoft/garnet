cmake_minimum_required (VERSION 3.2.2)

set(CMAKE_CXX_STANDARD 14)

project(Tsavorite)

OPTION(USE_URING "Enable io_uring based IO handler" OFF)

if (MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /Gm- /W3 /WX /EHsc /GS /fp:precise /permissive- /Zc:wchar_t /Zc:forScope /Zc:inline /Gd /TP")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Qspectre /guard:cf /sdl")

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1 /MDd")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /Oi /Gy- /MD")

    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG /OPT:NOICF /CETCOMPAT")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /DEBUG /OPT:NOICF /CETCOMPAT")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -D_DEBUG")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g")

    if (USE_URING)
      set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DUSE_URING")
      set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DUSE_URING")
    endif()
endif()

# Always set _DEBUG compiler directive when compiling bits regardless of target OS
set_directory_properties(PROPERTIES COMPILE_DEFINITIONS_DEBUG "_DEBUG")

include_directories(${CMAKE_SOURCE_DIR}/src)

# Set the directory targets when build in libs and binaries
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

# Build each subdirectory
add_subdirectory(src)
