// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Explicit binding to the mimalloc native allocator. The library is loaded on demand from the shipped
    /// <c>runtimes/&lt;rid&gt;/native/</c> prebuilt using <see cref="NativeLibrary.Load(string)"/> +
    /// <see cref="NativeLibrary.GetExport(nint, string)"/> (function pointers), <b>not</b> <c>[DllImport]</c>:
    /// <see cref="Tsavorite.core"/> already registers a single <see cref="NativeLibrary.SetDllImportResolver"/>
    /// (for the native storage device) and .NET permits only one resolver per assembly.
    /// </summary>
    internal static unsafe class Mimalloc
    {
        static readonly object InitLock = new();
        static bool initialized;
        static bool available;

        static delegate* unmanaged[Cdecl]<nuint, nuint, nint> p_malloc_aligned;
        static delegate* unmanaged[Cdecl]<nuint, nuint, nint> p_zalloc_aligned;
        static delegate* unmanaged[Cdecl]<nint, void> p_free;
        static delegate* unmanaged[Cdecl]<nint, nuint> p_usable_size;
        static delegate* unmanaged[Cdecl]<int, void> p_collect;

        /// <summary>True if the mimalloc library loaded and all required exports resolved.</summary>
        internal static bool Available => available;

        /// <summary>
        /// Attempt to load and bind mimalloc. Idempotent and thread-safe; the result is cached. Never throws —
        /// on any failure it sets <see cref="Available"/> to false and logs a warning, so callers can fall back
        /// to the managed allocator.
        /// </summary>
        internal static bool TryInitialize(ILogger logger = null)
        {
            if (initialized)
                return available;
            lock (InitLock)
            {
                if (initialized)
                    return available;
                try
                {
                    if (TryLoad(out var handle))
                    {
                        p_malloc_aligned = (delegate* unmanaged[Cdecl]<nuint, nuint, nint>)NativeLibrary.GetExport(handle, "mi_malloc_aligned");
                        p_zalloc_aligned = (delegate* unmanaged[Cdecl]<nuint, nuint, nint>)NativeLibrary.GetExport(handle, "mi_zalloc_aligned");
                        p_free = (delegate* unmanaged[Cdecl]<nint, void>)NativeLibrary.GetExport(handle, "mi_free");
                        p_usable_size = (delegate* unmanaged[Cdecl]<nint, nuint>)NativeLibrary.GetExport(handle, "mi_usable_size");
                        p_collect = (delegate* unmanaged[Cdecl]<int, void>)NativeLibrary.GetExport(handle, "mi_collect");
                        available = true;
                    }
                    else
                    {
                        logger?.LogWarning("mimalloc native library could not be loaded for RID '{rid}'; native allocator surfaces backed by mimalloc will fall back to managed.", GetRuntimeIdentifier());
                    }
                }
                catch (Exception ex)
                {
                    available = false;
                    logger?.LogWarning(ex, "Failed to bind mimalloc exports; native allocator surfaces backed by mimalloc will fall back to managed.");
                }
                initialized = true;
            }
            return available;
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        internal static nint MallocAligned(nuint size, nuint alignment) => p_malloc_aligned(size, alignment);

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        internal static nint ZallocAligned(nuint size, nuint alignment) => p_zalloc_aligned(size, alignment);

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        internal static void Free(nint ptr) => p_free(ptr);

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        internal static nuint UsableSize(nint ptr) => p_usable_size(ptr);

        /// <summary>Force mimalloc to return as much unused memory to the OS as possible. Used at shutdown.</summary>
        internal static void Collect(bool force)
        {
            if (available)
                p_collect(force ? 1 : 0);
        }

        static bool TryLoad(out nint handle)
        {
            var fileName = OperatingSystem.IsWindows() ? "mimalloc.dll"
                : OperatingSystem.IsMacOS() ? "libmimalloc.dylib"
                : "libmimalloc.so";
            var relativePath = $"runtimes/{GetRuntimeIdentifier()}/native/{fileName}";

            // Probe the shipped prebuilt first (assembly dir, app base dir), then fall back to the OS
            // default search (system-installed mimalloc) by bare name.
            foreach (var root in new[] { Path.GetDirectoryName(typeof(Mimalloc).Assembly.Location), AppContext.BaseDirectory })
            {
                if (string.IsNullOrEmpty(root))
                    continue;
                var candidate = Path.Combine(root, relativePath);
                if (File.Exists(candidate) && NativeLibrary.TryLoad(Path.GetFullPath(candidate), out handle))
                    return true;
            }

            return NativeLibrary.TryLoad(fileName, typeof(Mimalloc).Assembly, DllImportSearchPath.SafeDirectories, out handle);
        }

        static string GetRuntimeIdentifier()
        {
            var arch = RuntimeInformation.ProcessArchitecture switch
            {
                Architecture.X64 => "x64",
                Architecture.Arm64 => "arm64",
                Architecture.X86 => "x86",
                Architecture.Arm => "arm",
                var other => other.ToString().ToLowerInvariant()
            };

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return $"win-{arch}";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                return $"osx-{arch}";
            return IsMuslRuntime() ? $"linux-musl-{arch}" : $"linux-{arch}";
        }

        static bool IsMuslRuntime()
        {
            try
            {
                foreach (var f in Directory.EnumerateFiles("/lib", "ld-musl-*"))
                    return true;
            }
            catch
            {
                // /lib not enumerable; assume glibc.
            }
            return false;
        }
    }
}
