#!/usr/bin/env sh
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
#
# Builds the Tsavorite native device shared libraries for the current Linux
# platform and stages them into an output directory as the canonical prebuilts.
#
# Two flavours are produced, matching what the C# NativeStorageDevice loader
# expects side-by-side under runtimes/<rid>/native/:
#   * libnative_device.so        (USE_URING=ON)  - links libaio + liburing
#   * libnative_device_libaio.so (USE_URING=OFF) - links libaio only
#
# The build dependencies must already be installed (this script does NOT install
# them, so it stays package-manager agnostic):
#   Debian/Ubuntu (glibc): build-essential cmake libaio-dev liburing-dev uuid-dev patchelf
#   Alpine (musl):         build-base cmake linux-headers libaio-dev liburing-dev util-linux-dev
#
# Usage: build-native.sh <output-dir>
set -eu

OUT="${1:?output directory required (usage: build-native.sh <output-dir>)}"
CC_DIR="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$OUT"

jobs="$( (nproc 2>/dev/null) || echo 2 )"

build_one() {
    variant="$1"      # subdir name for the build tree
    use_uring="$2"    # ON | OFF
    libname="$3"      # output file name
    bdir="$CC_DIR/build/$variant"

    rm -rf "$bdir"
    cmake -S "$CC_DIR" -B "$bdir" -DCMAKE_BUILD_TYPE=Release -DUSE_URING="$use_uring"
    cmake --build "$bdir" --config Release -j "$jobs"

    so="$(find "$bdir" -name libnative_device.so | head -1)"
    if [ -z "$so" ]; then
        echo "ERROR: build produced no libnative_device.so for variant '$variant'" >&2
        exit 1
    fi

    # Normalize the Ubuntu/Debian 24.04+ t64 SONAME (libaio.so.1t64) back to the portable
    # libaio.so.1 so the checked-in binary loads on any glibc distro. On musl the SONAME is
    # already libaio.so.1, and patchelf is not installed there, so this is a no-op guarded on
    # patchelf's presence.
    if command -v patchelf >/dev/null 2>&1; then
        patchelf --replace-needed libaio.so.1t64 libaio.so.1 "$so" 2>/dev/null || true
    fi

    cp "$so" "$OUT/$libname"

    # Canary: the C ABI entrypoints must be exported, otherwise the C# P/Invoke surface is broken.
    n="$(nm -D --defined-only "$OUT/$libname" 2>/dev/null | grep -c 'NativeDevice_' || true)"
    needed="$(readelf -d "$OUT/$libname" 2>/dev/null | awk '/NEEDED/ {gsub(/[][]/,"",$NF); print $NF}' | paste -sd, - || true)"
    echo "  $libname: ${n} NativeDevice_ symbols; NEEDED=[${needed}]"
    if [ "${n:-0}" -lt 15 ]; then
        echo "ERROR: $libname exports only ${n} NativeDevice_ symbols (expected >= 15)" >&2
        exit 1
    fi
}

echo "Building native device (uring + libaio-only) into: $OUT"
build_one uring  ON  libnative_device.so
build_one libaio OFF libnative_device_libaio.so
echo "Done."
