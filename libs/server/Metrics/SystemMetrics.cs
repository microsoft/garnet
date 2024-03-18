// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    internal class SystemMetrics
    {
        [DllImport("psapi.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool GetPerformanceInfo([Out] out PerformanceInformation PerformanceInformation, [In] int Size);

        [StructLayout(LayoutKind.Sequential)]
        public struct PerformanceInformation
        {
            public int Size;
            public IntPtr CommitTotal;
            public IntPtr CommitLimit;
            public IntPtr CommitPeak;
            public IntPtr PhysicalTotal;
            public IntPtr PhysicalAvailable;
            public IntPtr SystemCache;
            public IntPtr KernelTotal;
            public IntPtr KernelPaged;
            public IntPtr KernelNonPaged;
            public IntPtr PageSize;
            public int HandlesCount;
            public int ProcessCount;
            public int ThreadCount;
        }

        public static Int64 GetTotalMemory(long units = 1)
        {
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                PerformanceInformation pi = new PerformanceInformation();
                if (GetPerformanceInfo(out pi, Marshal.SizeOf(pi)))
                {
                    return Convert.ToInt64((pi.PhysicalTotal.ToInt64() * pi.PageSize.ToInt64() / units));
                }
                else
                {
                    return -1;
                }
            }
            else
            {
                var gcMemoryInfo = GC.GetGCMemoryInfo();
                return gcMemoryInfo.TotalAvailableMemoryBytes / units;
            }
        }

        public static Int64 GetPhysicalAvailableMemory(long units = 1)
        {
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                PerformanceInformation pi = new PerformanceInformation();
                if (GetPerformanceInfo(out pi, Marshal.SizeOf(pi)))
                {
                    return Convert.ToInt64((pi.PhysicalAvailable.ToInt64() * pi.PageSize.ToInt64() / units));
                }
                else
                {
                    return -1;
                }
            }
            else
            {
                var cproc = Process.GetCurrentProcess();
                return -1;
            }
        }

        public static Int64 GetPagedMemorySize(long units = 1)
        {
            var cproc = Process.GetCurrentProcess();
            return cproc.PagedMemorySize64 / units;
        }

        public static Int64 GetPagedSystemMemorySize(long units = 1)
        {
            var cproc = Process.GetCurrentProcess();
            return cproc.PagedSystemMemorySize64 / units;
        }

        public static Int64 GetPeakPagedMemorySize(long units = 1)
        {
            var cproc = Process.GetCurrentProcess();
            return cproc.PeakPagedMemorySize64 / units;
        }

        public static Int64 GetVirtualMemorySize64(long units = 1)
        {
            var cproc = Process.GetCurrentProcess();
            return cproc.VirtualMemorySize64 / units;
        }

        public static Int64 GetPrivateMemorySize64(long units = 1)
        {
            var cproc = Process.GetCurrentProcess();
            return cproc.PrivateMemorySize64 / units;
        }

        public static Int64 GetPeakVirtualMemorySize64(long units = 1)
        {
            var cproc = Process.GetCurrentProcess();
            return cproc.PeakVirtualMemorySize64 / units;
        }

        public static Int64 GetPhysicalMemoryUsage(long units = 1)
        {
            var cproc = Process.GetCurrentProcess();
            return cproc.WorkingSet64 / units;
        }

        public static Int64 GetPeakPhysicalMemoryUsage(long units = 1)
        {
            var cproc = Process.GetCurrentProcess();
            return cproc.PeakWorkingSet64 / units;
        }
    }
}