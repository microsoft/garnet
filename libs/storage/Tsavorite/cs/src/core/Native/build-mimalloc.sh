#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
#
# Reproducibly (cross-)builds the prebuilt mimalloc binaries shipped under
#   runtimes/<rid>/native/
# (next to this script) and consumed by MimallocPooledAllocator for the
# --native-allocator buffer-pool / full modes.
#
# mimalloc is pinned to the version whose ABI the P/Invoke bindings in Mimalloc.cs
# target. Keep MIMALLOC_VERSION in sync with MI_MALLOC_VERSION (mi_version(): e.g.
# 219 == v2.1.9). The .csproj ships whatever runtimes/<rid>/native/ folders exist
# via a recursive glob, so adding a platform is just running this and committing.
#
# Usage:
#   ./build-mimalloc.sh linux-x64     # native gcc build  -> libmimalloc.so
#   ./build-mimalloc.sh win-x64       # zig cross-compile -> mimalloc.dll
#
# linux-x64 needs: gcc.  win-x64 needs: zig (https://ziglang.org; self-contained,
# bundles clang + the Windows cross toolchain, no root needed). Set ZIG=/path/to/zig
# if zig is not on PATH.
set -euo pipefail

MIMALLOC_VERSION="${MIMALLOC_VERSION:-v2.1.9}"
RID="${1:?usage: build-mimalloc.sh <rid>  (linux-x64 | win-x64)}"
ZIG="${ZIG:-zig}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$HERE/runtimes/$RID/native"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "Cloning mimalloc $MIMALLOC_VERSION ..."
git clone --depth 1 --branch "$MIMALLOC_VERSION" https://github.com/microsoft/mimalloc.git "$WORK/mimalloc" >/dev/null 2>&1
cd "$WORK/mimalloc"

# src/static.c is the single-translation-unit build (it #includes every source file).
# MI_SHARED_LIB + MI_SHARED_LIB_EXPORT make mi_decl_export = dllexport / visibility(default).
COMMON_DEFS=(-O2 -DNDEBUG -DMI_SHARED_LIB -DMI_SHARED_LIB_EXPORT -Iinclude -shared)

mkdir -p "$OUT_DIR"
case "$RID" in
  linux-x64)
    gcc "${COMMON_DEFS[@]}" -fPIC -o "$OUT_DIR/libmimalloc.so" src/static.c
    ;;
  win-x64)
    # MI_WIN_NOREDIRECT: we call mi_* directly and do NOT use the Windows malloc-redirection
    # feature (which would require the separate mimalloc-redirect.dll). This selects mimalloc's
    # built-in _mi_allocator_init/_done fallback instead of importing them from the redirector.
    # The resulting DLL imports only KERNEL32/ADVAPI32 + the Universal CRT (api-ms-win-crt-*),
    # i.e. no mingw runtime DLLs — self-contained on Windows 10 / Server 2016+.
    "$ZIG" cc -target x86_64-windows-gnu "${COMMON_DEFS[@]}" -DMI_WIN_NOREDIRECT \
        -o "$OUT_DIR/mimalloc.dll" src/static.c
    # zig also emits an import library (.lib) and debug symbols (.pdb); ship only the runtime DLL.
    rm -f "$OUT_DIR/mimalloc.lib" "$OUT_DIR/static.lib" "$OUT_DIR/mimalloc.pdb"
    ;;
  *)
    echo "Unsupported RID '$RID' (add a case here)." >&2; exit 2;;
esac

echo "Built $RID -> $OUT_DIR"
