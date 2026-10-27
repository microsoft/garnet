// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

namespace Tsavorite.epoch.litmus
{
    /// <summary>Thread-to-core pinning.</summary>
    internal static unsafe class Platform
    {
        [DllImport("kernel32")] static extern IntPtr GetCurrentThread();
        [DllImport("kernel32", SetLastError = true)] static extern UIntPtr SetThreadAffinityMask(IntPtr h, UIntPtr m);

        [DllImport("libc", SetLastError = true, EntryPoint = "sched_setaffinity")] static extern int LinuxSchedSetAffinity(int pid, nuint cpuSetSize, ulong* mask);

        /// <summary>Bytes in the kernel cpu_set_t passed to sched_setaffinity (1024 CPUs).</summary>
        const int CpuSetBytes = 128;

        /// <summary>Whether core pinning is available on this platform.</summary>
        internal static bool IsSupported => OperatingSystem.IsWindows() || OperatingSystem.IsLinux();

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