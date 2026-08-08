// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Process-wide opt-in to the native allocator via the <c>GARNET_NATIVE_ALLOCATOR</c> environment variable
    /// (<c>off</c> | <c>buffer-pool</c> | <c>full</c>). Runs as a module initializer so the surfaces are installed
    /// before any store, pool, or device is constructed — mirroring the host's <c>--native-allocator</c> switch,
    /// but usable in any process (tests, benchmarks) without a command line. Default (unset) is a no-op.
    /// </summary>
    internal static class NativeAllocatorEnvironmentInitializer
    {
        // CA2255: a module initializer in a library is normally discouraged, but this one is inert unless the
        // GARNET_NATIVE_ALLOCATOR env var is set (default: no-op), and it must run before any store/pool is
        // constructed — which is exactly the module-initializer contract. Consumers who don't set the env var
        // are unaffected.
#pragma warning disable CA2255
        [ModuleInitializer]
#pragma warning restore CA2255
        internal static void Initialize()
        {
            var mode = Environment.GetEnvironmentVariable("GARNET_NATIVE_ALLOCATOR");
            if (string.IsNullOrWhiteSpace(mode))
                return;

            var surfaces = mode.Trim().ToLowerInvariant() switch
            {
                "buffer-pool" or "bufferpool" or "pool" => NativeAllocatorSurfaces.BufferPool,
                "full" or "all" => NativeAllocatorSurfaces.Full,
                _ => NativeAllocatorSurfaces.None,
            };

            if (surfaces != NativeAllocatorSurfaces.None)
                _ = NativeAllocatorInitializer.Initialize(surfaces);
        }
    }
}
