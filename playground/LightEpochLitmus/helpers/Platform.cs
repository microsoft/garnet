// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.ComponentModel;
using System.Runtime.InteropServices;

namespace Tsavorite.epoch.litmus
{
    /// <summary>Page allocation and thread-to-core pinning.</summary>
    internal static unsafe class Platform
    {
        const uint MEM_COMMIT = 0x1000, MEM_RESERVE = 0x2000, MEM_RELEASE = 0x8000, PAGE_RW = 0x04;
        [DllImport("kernel32", SetLastError = true)] static extern IntPtr VirtualAlloc(IntPtr a, nuint s, uint t, uint p);
        [DllImport("kernel32", SetLastError = true)] static extern bool VirtualFree(IntPtr a, nuint s, uint t);
        [DllImport("kernel32")] static extern IntPtr GetCurrentThread();
        [DllImport("kernel32", SetLastError = true)] static extern UIntPtr SetThreadAffinityMask(IntPtr h, UIntPtr m);

        const int PROT_READ = 0x1, PROT_WRITE = 0x2;
        const int MAP_PRIVATE = 0x02, MAP_ANONYMOUS = 0x20;
        [DllImport("libc", SetLastError = true, EntryPoint = "mmap")] static extern IntPtr LinuxMmap(IntPtr addr, nuint length, int prot, int flags, int fd, long offset);
        [DllImport("libc", SetLastError = true, EntryPoint = "munmap")] static extern int LinuxMunmap(IntPtr addr, nuint length);
        [DllImport("libc", SetLastError = true, EntryPoint = "sched_setaffinity")] static extern int LinuxSchedSetAffinity(int pid, nuint cpuSetSize, ulong* mask);

        /// <summary>Bytes in the kernel cpu_set_t passed to sched_setaffinity (1024 CPUs).</summary>
        const int CpuSetBytes = 128;

        /// <summary>Whether page unmapping and core pinning are available on this platform.</summary>
        internal static bool IsSupported => OperatingSystem.IsWindows() || OperatingSystem.IsLinux();

        /// <summary>Allocate a page-aligned region, committed and readable/writable.</summary>
        internal static byte* MapPage(nuint bytes)
        {
            if (OperatingSystem.IsWindows())
            {
                var pointer = VirtualAlloc(IntPtr.Zero, bytes, MEM_COMMIT | MEM_RESERVE, PAGE_RW);
                if (pointer == IntPtr.Zero)
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "VirtualAlloc failed.");
                return (byte*)pointer;
            }

            var mapped = LinuxMmap(IntPtr.Zero, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (mapped == new IntPtr(-1))
                throw new Win32Exception(Marshal.GetLastWin32Error(), "mmap failed.");

            return (byte*)mapped;
        }

        /// <summary>Release the region.</summary>
        internal static void Unmap(byte* p, nuint bytes)
        {
            if (OperatingSystem.IsWindows())
            {
                // MEM_RELEASE requires dwSize 0 and drops the whole reservation.
                if (!VirtualFree((IntPtr)p, 0, MEM_RELEASE))
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "VirtualFree failed.");

                return;
            }

            if (LinuxMunmap((IntPtr)p, bytes) != 0)
                throw new Win32Exception(Marshal.GetLastWin32Error(), "munmap failed.");
        }

        /// <summary>
        /// Pin the calling thread to <paramref name="core"/>. An unpinned thread drifts off the
        /// core it was laid out on and stops sampling the race window, so a failure invalidates
        /// the run.
        /// </summary>
        internal static bool TryPin(int core)
        {
            try
            {
                if (OperatingSystem.IsWindows())
                {
                    if ((uint)core >= UIntPtr.Size * 8)
                        return PinFailed(core, $"core is outside the {UIntPtr.Size * 8}-bit affinity mask");

                    if (SetThreadAffinityMask(GetCurrentThread(), (UIntPtr)(1UL << core)) == UIntPtr.Zero)
                        return PinFailed(core, $"SetThreadAffinityMask failed with error {Marshal.GetLastWin32Error()}");

                    return true;
                }

                if (!OperatingSystem.IsLinux())
                    return PinFailed(core, "pinning is only implemented for Windows and Linux");

                if ((uint)core >= CpuSetBytes * 8)
                    return PinFailed(core, $"core is outside the {CpuSetBytes * 8}-CPU set");

                var cpuMask = stackalloc ulong[CpuSetBytes / sizeof(ulong)];
                for (var i = 0; i < CpuSetBytes / sizeof(ulong); i++)
                    cpuMask[i] = 0;

                cpuMask[core / 64] = 1UL << (core % 64);

                // pid 0 means the calling thread: every Linux thread is a task with its own mask.
                if (LinuxSchedSetAffinity(0, CpuSetBytes, cpuMask) != 0)
                    return PinFailed(core, $"sched_setaffinity failed with errno {Marshal.GetLastWin32Error()}");

                return true;
            }
            catch (DllNotFoundException e)
            {
                return PinFailed(core, e.Message);
            }
            catch (EntryPointNotFoundException e)
            {
                return PinFailed(core, e.Message);
            }
        }

        static bool PinFailed(int core, string reason)
        {
            Console.Error.WriteLine($"error: could not pin thread to core {core}: {reason}");
            return false;
        }
    }
}