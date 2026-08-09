#!/usr/bin/env sh
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
#
# Reproducibly (cross-)builds the prebuilt mimalloc binaries shipped under
#   runtimes/<rid>/native/
# (next to this script) and consumed by MimallocPooledAllocator for the
# --native-allocator buffer-pool / full modes.
#
# The mimalloc version is pinned in `mimalloc.version` (next to this script) - the
# single source of truth, and the CI trigger path. It MUST stay ABI-compatible with
# the P/Invoke bindings in Mimalloc.cs (mi_version(): e.g. 219 == v2.1.9). The .csproj
# ships whatever runtimes/<rid>/native/ folders exist via a recursive glob, so adding a
# platform is just producing its binary and committing it.
#
# POSIX sh (no bash): the Build Native mimalloc workflow runs this inside minimal glibc
# and musl (Alpine, busybox sh - no bash) containers, matching cc/build-native.sh.
#
# This script builds the LINUX runtime identifiers (run it inside the matching
# glibc/musl, x64/arm64 container - see the Build Native mimalloc workflow). The Windows
# binaries are built by that workflow with MSVC (toolchain parity + CFG/Spectre with the
# native device DLL); for a quick LOCAL Windows bootstrap you can cross-compile with zig
# via `RID=win-x64` (set ZIG=/path/to/zig) - CI regenerates it with MSVC.
#
# Usage:
#   ./build-mimalloc.sh linux-x64        # -> libmimalloc.so   (needs: gcc)
#   ./build-mimalloc.sh linux-musl-x64   # -> libmimalloc.so   (run in an Alpine container)
#   ZIG=/path/to/zig ./build-mimalloc.sh win-x64   # local bootstrap only -> mimalloc.dll
set -eu

RID="${1:?usage: build-mimalloc.sh <rid>  (linux-x64 | linux-arm64 | linux-musl-x64 | linux-musl-arm64 | win-x64)}"
ZIG="${ZIG:-zig}"

HERE="$(cd "$(dirname "$0")" && pwd)"
# Pin comes from mimalloc.version (single source of truth); env override is for testing only.
MIMALLOC_VERSION="${MIMALLOC_VERSION:-$(tr -d '[:space:]' < "$HERE/mimalloc.version")}"
OUT_DIR="$HERE/runtimes/$RID/native"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "Cloning mimalloc $MIMALLOC_VERSION ..."
git clone --depth 1 --branch "$MIMALLOC_VERSION" https://github.com/microsoft/mimalloc.git "$WORK/mimalloc" >/dev/null 2>&1
cd "$WORK/mimalloc"

# src/static.c is mimalloc's single-translation-unit build (it #includes every source file),
# so one compiler invocation with no CMake yields exactly libmimalloc.so / mimalloc.dll (no
# soname suffix, no import lib) - the flat names the loader (Mimalloc.TryLoad) expects.
# MI_SHARED_LIB + MI_SHARED_LIB_EXPORT make mi_decl_export = dllexport / visibility(default).
# COMMON_DEFS stays unquoted on use so sh word-splits it into flags (no spaces within a flag).
COMMON_DEFS="-O2 -DNDEBUG -DMI_SHARED_LIB -DMI_SHARED_LIB_EXPORT -Iinclude -shared"

mkdir -p "$OUT_DIR"
case "$RID" in
  linux-x64 | linux-arm64 | linux-musl-x64 | linux-musl-arm64)
    # The container provides the native (glibc/musl, x64/arm64) gcc, so a plain build targets
    # the current RID. Run this script inside the matching container (see the CI workflow).
    gcc $COMMON_DEFS -fPIC -o "$OUT_DIR/libmimalloc.so" src/static.c
    ;;
  win-x64)
    # LOCAL bootstrap only (CI builds the canonical MSVC binary). MI_WIN_NOREDIRECT: we call
    # mi_* directly and do NOT use the Windows malloc-redirection feature (which would need the
    # separate mimalloc-redirect.dll); this selects mimalloc's built-in _mi_allocator_init/_done
    # fallback. The zig DLL imports only KERNEL32/ADVAPI32 + the Universal CRT (api-ms-win-crt-*),
    # i.e. no mingw runtime DLLs - self-contained on Windows 10 / Server 2016+.
    "$ZIG" cc -target x86_64-windows-gnu $COMMON_DEFS -DMI_WIN_NOREDIRECT \
        -o "$OUT_DIR/mimalloc.dll" src/static.c
    rm -f "$OUT_DIR/mimalloc.lib" "$OUT_DIR/static.lib" "$OUT_DIR/mimalloc.pdb"
    ;;
  *)
    echo "Unsupported RID '$RID' (add a case here)." >&2; exit 2;;
esac

echo "Built $RID ($MIMALLOC_VERSION) -> $OUT_DIR"
