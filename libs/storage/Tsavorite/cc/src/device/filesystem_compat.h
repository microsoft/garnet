// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma once

// Portable filesystem include. Prefer C++17 <filesystem>; fall back to the pre-C++17
// <experimental/filesystem> only on toolchains that still provide it. Newer MSVC
// (Visual Studio 2022 17.10+ / Visual Studio 2026) has removed <experimental/filesystem>
// entirely, so std::filesystem (C++17) is required there; the build is compiled as C++17.
// A single alias, tsv_fs, is exposed so callers do not hard-code either namespace.
#if defined(__cpp_lib_filesystem) \
    || (defined(_MSVC_LANG) && _MSVC_LANG >= 201703L) \
    || (__cplusplus >= 201703L)
#include <filesystem>
namespace tsv_fs = std::filesystem;
#elif defined(__has_include) && __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace tsv_fs = std::experimental::filesystem;
#else
#error "Neither <filesystem> nor <experimental/filesystem> is available for this toolchain"
#endif
