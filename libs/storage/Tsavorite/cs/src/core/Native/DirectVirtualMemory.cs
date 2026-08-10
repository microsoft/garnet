// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    /// <summary>
    /// A direct OS virtual-memory reservation: the region to free (<see cref="BasePtr"/>), the aligned usable
    /// pointer (<see cref="AlignedPtr"/>), and the reserved byte length. Default value is the empty block.
    /// </summary>
    internal readonly struct DirectVmBlock
    {
        public readonly nint BasePtr;
        public readonly nint AlignedPtr;
        public readonly long ReservedLength;

        public DirectVmBlock(nint basePtr, nint alignedPtr, long reservedLength)
        {
            BasePtr = basePtr;
            AlignedPtr = alignedPtr;
            ReservedLength = reservedLength;
        }

        public bool IsEmpty => BasePtr == 0;
    }

    /// <summary>
    /// Direct OS virtual-memory allocator for large, long-lived, NUMA-sensitive singletons (hash index, log
    /// pages, recovery frames) in the native-allocator "full" mode. Uses <c>mmap(MAP_ANONYMOUS)</c> on Linux
    /// and <c>VirtualAlloc(MEM_RESERVE|MEM_COMMIT)</c> on Windows: both return <b>demand-zero</b> pages whose
    /// physical mapping happens on <b>first access</b> (NUMA first-touch), matching today's
    /// <see cref="System.GC.AllocateArray{T}(int, bool)"/> behavior — the accepted requirement for these
    /// surfaces. mimalloc is deliberately not used here: for a handful of multi-GB mappings its segment/arena
    /// reuse can hand back dirty memory and it offers no benefit over a plain OS mapping.
    /// </summary>
    internal static unsafe class DirectVirtualMemory
    {
        [DllImport("libc", SetLastError = true)]
        static extern nint mmap(nint addr, nuint length, int prot, int flags, int fd, nint offset);

        [DllImport("libc", SetLastError = true)]
        static extern int munmap(nint addr, nuint length);

        [DllImport("libc", SetLastError = true)]
        static extern int madvise(nint addr, nuint length, int advice);

        [DllImport("kernel32", SetLastError = true)]
        static extern nint VirtualAlloc(nint lpAddress, nuint dwSize, uint flAllocationType, uint flProtect);

        [DllImport("kernel32", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        static extern bool VirtualFree(nint lpAddress, nuint dwSize, uint dwFreeType);

        const int PROT_READ_WRITE = 0x1 | 0x2;          // PROT_READ | PROT_WRITE
        const int MAP_PRIVATE = 0x02;                   // MAP_PRIVATE (all Unix)
        // MAP_ANONYMOUS differs by OS: 0x20 on Linux, 0x1000 on macOS/BSD. Passing the Linux value on macOS with
        // fd=-1 fails with EBADF, so select it at runtime to keep direct-VM working on osx-x64/osx-arm64.
        static readonly int MAP_PRIVATE_ANONYMOUS = MAP_PRIVATE | (OperatingSystem.IsMacOS() ? 0x1000 : 0x20);
        static readonly nint MAP_FAILED = -1;

        const int MADV_HUGEPAGE = 14;                   // Linux: hint transparent huge pages for the range
        const long HugePageSize = 2L << 20;             // 2 MB (x86-64 / arm64 THP granularity)

        const uint MEM_RESERVE_COMMIT = 0x1000 | 0x2000; // MEM_COMMIT | MEM_RESERVE
        const uint MEM_RELEASE = 0x8000;
        const uint PAGE_READWRITE = 0x04;

        static readonly long PageSize = Environment.SystemPageSize;

        /// <summary>
        /// Reserve+commit a demand-zero region of at least <paramref name="size"/> bytes whose usable pointer is
        /// aligned to <paramref name="alignment"/> (a nonzero power of two). Physical pages are mapped on first
        /// touch. Throws <see cref="OutOfMemoryException"/> on failure.
        /// <para>
        /// On Linux, regions at least <see cref="HugePageSize"/> are aligned to 2&#160;MB and hinted with
        /// <c>madvise(MADV_HUGEPAGE)</c> so the kernel backs them with transparent huge pages. These regions (hash
        /// index, log pages, recovery frames) are large, long-lived, and densely accessed at random — exactly the
        /// TLB-bound profile where 2&#160;MB pages cut dTLB misses (measured ~25% lower random-access latency vs
        /// 4&#160;KB pages). The managed POH backend gets 4&#160;KB pages under the common <c>madvise</c> THP mode,
        /// so this is a direct-VM-only win. Best-effort: if THP is disabled the hint is a no-op and mapping proceeds.
        /// </para>
        /// </summary>
        public static DirectVmBlock Allocate(long size, int alignment)
        {
            if (size <= 0)
                throw new ArgumentOutOfRangeException(nameof(size));
            if (alignment <= 0 || (alignment & (alignment - 1)) != 0)
                throw new ArgumentException("alignment must be a power of two", nameof(alignment));

            // For a large region on Linux, align the usable pointer to the 2 MB huge-page boundary so THP can back
            // the whole region from its first byte (a 4 KB-aligned base would leave the leading partial 2 MB on
            // small pages). A 2 MB alignment still satisfies any sector alignment (which is <= 2 MB).
            var useHugePages = !OperatingSystem.IsWindows() && size >= HugePageSize;
            long effectiveAlignment = useHugePages && alignment < HugePageSize ? HugePageSize : alignment;

            // Over-reserve by the alignment so the returned base (page-aligned) can be rounded up to alignment.
            var reserve = RoundUpToPage(size + effectiveAlignment);

            nint basePtr;
            if (OperatingSystem.IsWindows())
            {
                basePtr = VirtualAlloc(0, (nuint)reserve, MEM_RESERVE_COMMIT, PAGE_READWRITE);
                if (basePtr == 0)
                    throw new OutOfMemoryException($"VirtualAlloc of {reserve} bytes failed (error {Marshal.GetLastWin32Error()})");
            }
            else
            {
                basePtr = mmap(0, (nuint)reserve, PROT_READ_WRITE, MAP_PRIVATE_ANONYMOUS, -1, 0);
                if (basePtr == MAP_FAILED || basePtr == 0)
                    throw new OutOfMemoryException($"mmap of {reserve} bytes failed (errno {Marshal.GetLastWin32Error()})");

                // Best-effort THP hint (before first touch, so faults bring in 2 MB pages directly). Ignore failure.
                if (useHugePages)
                    _ = madvise(basePtr, (nuint)reserve, MADV_HUGEPAGE);
            }

            var aligned = (nint)(((long)basePtr + (effectiveAlignment - 1)) & ~((long)effectiveAlignment - 1));
            NativeMemoryTracker.Add(reserve);
            return new DirectVmBlock(basePtr, aligned, reserve);
        }

        /// <summary>Release a region returned by <see cref="Allocate"/>. No-op for the empty block.</summary>
        public static void Free(in DirectVmBlock block)
        {
            if (block.BasePtr == 0)
                return;
            // Release the mapping FIRST, then decrement the tracked native bytes only on success. Subtracting
            // before the OS call (or ignoring its result) would under-report resident native memory if the free
            // failed, corrupting the heap-size tracker; a failed free leaves the region mapped and still counted.
            bool freed = OperatingSystem.IsWindows()
                ? VirtualFree(block.BasePtr, 0, MEM_RELEASE)
                : munmap(block.BasePtr, (nuint)block.ReservedLength) == 0;
            if (freed)
                NativeMemoryTracker.Subtract(block.ReservedLength);
        }

        /// <summary>Zero <paramref name="length"/> bytes at <paramref name="ptr"/>.</summary>
        public static void Clear(nint ptr, long length) => NativeMemory.Clear((void*)ptr, (nuint)length);

        static long RoundUpToPage(long n) => (n + PageSize - 1) & ~(PageSize - 1);
    }
}