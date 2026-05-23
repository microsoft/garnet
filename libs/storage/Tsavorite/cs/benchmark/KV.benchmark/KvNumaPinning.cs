// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// NUMA + thread affinity pinning helper.
    /// Linux: reads /sys/devices/system/node/* and uses sched_setaffinity via P/Invoke,
    /// intersecting with the process's allowed CPU mask (cgroup-friendly).
    /// Windows: uses SetThreadGroupAffinity (delegated through <see cref="Native32"/>);
    /// best-effort on multi-processor-group hosts.
    /// </summary>
    internal sealed class KvNumaPinning
    {
        public readonly bool Enabled;
        public readonly int NumaNode;
        public readonly int[] NodeCpus;         // physical-first ordering
        public readonly bool IsLinux;
        public readonly bool IsWindows;
        public readonly string DiagnosticMessage;

        public int FirstUnpinnedCpu => NodeCpus.Length > 0 ? NodeCpus[Math.Min(WorkerCount, NodeCpus.Length - 1)] : -1;
        public int WorkerCount;

        public KvNumaPinning(Options opts, int workerCount)
        {
            WorkerCount = workerCount;
            NumaNode = opts.NumaNode;
            Enabled = !opts.NoNumaPin;
            IsLinux = OperatingSystem.IsLinux();
            IsWindows = OperatingSystem.IsWindows();
            NodeCpus = Array.Empty<int>();
            if (!Enabled)
            {
                DiagnosticMessage = "NUMA pinning disabled (--no-numa-pin)";
                return;
            }
            if (IsLinux)
            {
                try { NodeCpus = DiscoverLinuxNodeCpus(NumaNode); }
                catch (Exception ex) { DiagnosticMessage = $"Linux NUMA discovery failed: {ex.Message}"; Enabled = false; return; }
                if (NodeCpus.Length == 0)
                {
                    DiagnosticMessage = $"NUMA node {NumaNode} has no CPUs after cpuset intersection";
                    Enabled = false;
                    return;
                }
                DiagnosticMessage = $"Linux NUMA node {NumaNode}: {NodeCpus.Length} CPUs after cpuset intersection";
            }
            else if (IsWindows)
            {
                try
                {
                    var (_, procsPerGroup) = Native32.GetNumGroupsProcsPerGroup();
                    NodeCpus = Enumerable.Range(0, (int)procsPerGroup).ToArray();
                    DiagnosticMessage = $"Windows: {NodeCpus.Length} CPUs in current processor group (best-effort NUMA)";
                }
                catch (Exception ex) { DiagnosticMessage = $"Windows NUMA discovery failed: {ex.Message}"; Enabled = false; }
            }
            else
            {
                DiagnosticMessage = "NUMA pinning not supported on this OS";
                Enabled = false;
            }
        }

        /// <summary>Pin the calling thread to the worker CPU for <paramref name="workerIndex"/>.</summary>
        public void PinWorker(int workerIndex)
        {
            if (!Enabled || NodeCpus.Length == 0) return;
            var cpu = NodeCpus[workerIndex % NodeCpus.Length];
            PinToCpu(cpu);
        }

        /// <summary>Pin the calling thread to the first un-pinned node CPU (setup / reporter).</summary>
        public void PinSetupOrReporter()
        {
            if (!Enabled || NodeCpus.Length == 0) return;
            var cpu = FirstUnpinnedCpu;
            if (cpu >= 0) PinToCpu(cpu);
        }

        private void PinToCpu(int cpu)
        {
            try
            {
                if (IsLinux) LinuxSchedSetAffinity(cpu);
                else if (IsWindows) Native32.AffinitizeThreadRoundRobin((uint)cpu, skipHyperthreads: false);
            }
            catch { /* best-effort; report via diagnostic if needed */ }
        }

        // ====== Linux discovery ======

        private static int[] DiscoverLinuxNodeCpus(int nodeId)
        {
            var nodeFile = $"/sys/devices/system/node/node{nodeId}/cpulist";
            if (!File.Exists(nodeFile))
                throw new InvalidOperationException($"NUMA node{nodeId}/cpulist not found");
            var nodeCpus = ParseCpuList(File.ReadAllText(nodeFile).Trim());

            // Intersect with the process's allowed mask (sched_getaffinity).
            var allowed = LinuxSchedGetAffinity();
            var intersected = nodeCpus.Where(c => allowed.Contains(c)).ToList();
            if (intersected.Count == 0) return Array.Empty<int>();

            // Group by physical core (one CPU per core first, then add siblings).
            var visited = new HashSet<int>();
            var result = new List<int>();
            foreach (var cpu in intersected)
            {
                if (visited.Contains(cpu)) continue;
                result.Add(cpu);
                visited.Add(cpu);
                // Add siblings AFTER all physical cores: collect siblings into a tail list.
                var sibFile = $"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list";
                if (!File.Exists(sibFile)) continue;
                foreach (var sib in ParseCpuList(File.ReadAllText(sibFile).Trim()))
                {
                    if (sib == cpu) continue;
                    visited.Add(sib);
                }
            }
            // Append siblings (round 2): visited contains physical+all siblings. The set difference
            // of (visited - result) is the sibling-only list.
            var siblings = visited.Where(c => !result.Contains(c) && intersected.Contains(c)).OrderBy(c => c).ToList();
            result.AddRange(siblings);
            return result.ToArray();
        }

        private static IEnumerable<int> ParseCpuList(string s)
        {
            // Format: "0-5,8-11,16" or "0,2,4"
            foreach (var part in s.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                var dash = part.IndexOf('-');
                if (dash >= 0)
                {
                    if (int.TryParse(part.AsSpan(0, dash), out var lo) &&
                        int.TryParse(part.AsSpan(dash + 1), out var hi))
                    {
                        for (int i = lo; i <= hi; i++) yield return i;
                    }
                }
                else if (int.TryParse(part, out var v))
                {
                    yield return v;
                }
            }
        }

        // ====== Linux libc P/Invoke for sched_get/setaffinity ======

        private const int CPU_SETSIZE_BITS = 1024;
        private const int CPU_SETSIZE_LONGS = CPU_SETSIZE_BITS / 64;

        [DllImport("libc", SetLastError = true)]
        private static extern int sched_setaffinity(int pid, IntPtr cpusetsize, ulong[] mask);

        [DllImport("libc", SetLastError = true)]
        private static extern int sched_getaffinity(int pid, IntPtr cpusetsize, ulong[] mask);

        private static HashSet<int> LinuxSchedGetAffinity()
        {
            var mask = new ulong[CPU_SETSIZE_LONGS];
            var rc = sched_getaffinity(0, (IntPtr)(CPU_SETSIZE_LONGS * 8), mask);
            var result = new HashSet<int>();
            if (rc != 0) return result;
            for (int i = 0; i < CPU_SETSIZE_BITS; i++)
                if ((mask[i / 64] & (1UL << (i % 64))) != 0) result.Add(i);
            return result;
        }

        private static void LinuxSchedSetAffinity(int cpu)
        {
            var mask = new ulong[CPU_SETSIZE_LONGS];
            mask[cpu / 64] = 1UL << (cpu % 64);
            _ = sched_setaffinity(0, (IntPtr)(CPU_SETSIZE_LONGS * 8), mask);
        }

        /// <summary>Format the applied CPU mask for emission in the metadata block.</summary>
        public string DescribeWorkerCpus()
        {
            if (!Enabled || NodeCpus.Length == 0) return "<none>";
            var n = Math.Min(WorkerCount, NodeCpus.Length);
            return string.Join(",", NodeCpus.Take(n));
        }
    }
}
