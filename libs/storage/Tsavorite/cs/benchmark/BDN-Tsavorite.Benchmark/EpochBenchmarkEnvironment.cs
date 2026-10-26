// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
#if EPOCH_RELEASE_WORKLOAD_TESTS
using System;
using System.Collections.Generic;
using System.IO;
#endif
#if GARNET_CLIENT_EPOCH_BENCHMARK
using Garnet.client;

namespace BDN.benchmark.Client
#else
using Tsavorite.core;

namespace BenchmarkDotNetTests
#endif
{
    static class EpochBenchmarkEnvironment
    {
        const string CpuVariable = "GARNET_EPOCH_BENCHMARK_CPUS";

        internal static void Report()
        {
            var assembly = typeof(LightEpoch).Assembly;
            var hash = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(assembly.Location)));
            Console.WriteLine($"Epoch: {typeof(LightEpoch).FullName}, SHA256={hash}, processors={Environment.ProcessorCount}");
            Console.WriteLine($"Epoch CPUs: {Environment.GetEnvironmentVariable(CpuVariable) ?? "scheduler default"}");
        }

        internal static Affinity Pin(int index, int workers)
        {
            var value = Environment.GetEnvironmentVariable(CpuVariable);
            if (value == null)
                return null;
            if (!OperatingSystem.IsWindows())
                throw new PlatformNotSupportedException($"{CpuVariable} requires Windows processor-group affinity.");
            var cpus = value.Split(',', StringSplitOptions.TrimEntries);
            if (cpus.Length < workers)
                throw new ArgumentException($"{CpuVariable} must specify at least {workers} distinct group:cpu entries.");
            var affinity = default(GroupAffinity);
            var seen = new HashSet<(ushort Group, int Cpu)>();
            for (var i = 0; i < cpus.Length; i++)
            {
                var parts = cpus[i].Split(':');
                if (parts.Length != 2 || !ushort.TryParse(parts[0], out var group)
                    || !int.TryParse(parts[1], out var cpu) || cpu < 0 || cpu >= 64
                    || group >= GetActiveProcessorGroupCount() || cpu >= GetActiveProcessorCount(group))
                    throw new ArgumentException($"Invalid {CpuVariable} entry: '{cpus[i]}'. Expected an active group:cpu.");
                if (!seen.Add((group, cpu)))
                    throw new ArgumentException($"Duplicate {CpuVariable} entry: '{cpus[i]}'.");
                if (i == index)
                    affinity = new GroupAffinity { Mask = (nuint)(1UL << cpu), Group = group };
            }
            return new Affinity(affinity);
        }

        internal sealed class Affinity : IDisposable
        {
            readonly GroupAffinity previous;
            readonly int ownerThreadId = Environment.CurrentManagedThreadId;

            internal Affinity(GroupAffinity affinity)
            {
                if (!SetThreadGroupAffinity(GetCurrentThread(), ref affinity, out previous))
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "Cannot pin epoch benchmark thread.");
            }

            public void Dispose()
            {
                if (Environment.CurrentManagedThreadId != ownerThreadId)
                    throw new InvalidOperationException("Epoch benchmark affinity must be restored on its owning thread.");
                var restore = previous;
                if (!SetThreadGroupAffinity(GetCurrentThread(), ref restore, out _))
                    throw new Win32Exception(Marshal.GetLastWin32Error(), "Cannot restore epoch benchmark thread affinity.");
            }
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct GroupAffinity
        {
            internal nuint Mask;
            internal ushort Group;
            internal ushort Reserved0;
            internal ushort Reserved1;
            internal ushort Reserved2;
        }

        [DllImport("kernel32.dll")]
        static extern nint GetCurrentThread();

        [DllImport("kernel32.dll")]
        static extern ushort GetActiveProcessorGroupCount();

        [DllImport("kernel32.dll")]
        static extern uint GetActiveProcessorCount(ushort group);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        static extern bool SetThreadGroupAffinity(nint thread, ref GroupAffinity affinity, out GroupAffinity previous);
    }
}