// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Validators;

namespace BDN.benchmark.Diagnostics
{
    /// <summary>
    /// BenchmarkDotNet diagnoser that reports the average CPU time the benchmark process consumed per
    /// operation, across <em>all</em> of its threads, split into three columns: "KernelMode CPU"
    /// (kernel-mode time), "UserMode CPU" (user-mode time), and "Total CPU" (their sum), alongside the
    /// usual time and allocation columns.
    ///
    /// Unlike the wall-clock Mean column, this attributes background CPU work that never runs on the
    /// measured thread. A method that parks and waits shows near-zero CPU even if it blocks for a
    /// while, whereas a busy-spin that keeps a core hot shows CPU at or above its wall-clock time.
    /// That difference is the whole point here: it quantifies the CPU burn a spin-wait inflicts versus a
    /// wake-based wait that an allocation/latency diagnoser cannot see.
    ///
    /// KernelMode CPU is <see cref="System.Diagnostics.Process.PrivilegedProcessorTime"/>: kernel-mode
    /// CPU time, i.e. syscalls, scheduling, and other in-kernel work charged to the process. A spin-wait
    /// that churns the scheduler (e.g. repeated <c>Task.Yield</c>) shows up disproportionately here.
    ///
    /// Measurement reads the child benchmark process's <c>UserProcessorTime</c> and
    /// <c>PrivilegedProcessorTime</c> at the actual-run boundaries
    /// (<see cref="HostSignal.BeforeActualRun"/> / <see cref="HostSignal.AfterActualRun"/>),
    /// so warmup, pilot, and JIT phases are excluded. Each delta is divided by
    /// <see cref="DiagnoserResults.TotalOperations"/> to yield per-op CPU time. Reading the child's
    /// aggregate processor time is cross-platform and framework-agnostic (no <c>Environment.CpuUsage</c>
    /// dependency), so it works identically on every target framework BDN.benchmark builds for.
    /// </summary>
    public sealed class CpuDiagnoser : IDiagnoser
    {
        sealed class CpuMetricDescriptor(string id, string displayName, int priority, string legend) : IMetricDescriptor
        {
            public string Id => id;
            public string DisplayName => displayName;
            public string Legend => legend;
            public string NumberFormat => "N1";
            public UnitType UnitType => UnitType.Time;
            public string Unit => "ns";
            public bool TheGreaterTheBetter => false;
            public int PriorityInCategory => priority;
            public bool GetIsAvailable(Metric metric) => true;
        }

        /// <summary>User-mode and kernel-mode CPU time captured at one actual-run boundary.</summary>
        readonly record struct CpuSample(TimeSpan User, TimeSpan Kernel);

        /// <summary>Shared instance; the diagnoser holds only per-run transient state keyed by benchmark case.</summary>
        public static readonly CpuDiagnoser Default = new();

        static readonly IMetricDescriptor KernelCpuDescriptor = new CpuMetricDescriptor(
            "KernelModeCpuTimePerOp", "KernelMode CPU", 0,
            "Average kernel-mode CPU time per operation: syscalls, scheduling, and other in-kernel work charged to the process.");

        static readonly IMetricDescriptor UserCpuDescriptor = new CpuMetricDescriptor(
            "UserModeCpuTimePerOp", "UserMode CPU", 1,
            "Average user-mode CPU time consumed by the benchmark process per operation, across all threads");

        static readonly IMetricDescriptor TotalCpuDescriptor = new CpuMetricDescriptor(
            "TotalCpuTimePerOp", "Total CPU", 2,
            "Average total CPU time (user + kernel) consumed by the benchmark process per operation, across all threads");

        // Per-case transient state. Benchmark cases run sequentially, but keying by case keeps the
        // start/delta bookkeeping robust against any interleaving of signals across cases.
        readonly ConcurrentDictionary<BenchmarkCase, CpuSample> beforeActualRun = new();
        readonly ConcurrentDictionary<BenchmarkCase, CpuSample> afterActualRun = new();

        // Counts result-processing lookups that found no recorded CPU sample, surfaced by DisplayResults.
        int unmatchedResults;

        CpuDiagnoser() { }

        /// <inheritdoc/>
        public IEnumerable<string> Ids => [nameof(CpuDiagnoser)];

        /// <inheritdoc/>
        public IEnumerable<IExporter> Exporters => [];

        /// <inheritdoc/>
        public IEnumerable<IAnalyser> Analysers => [];

        /// <inheritdoc/>
        public RunMode GetRunMode(BenchmarkCase benchmarkCase) => RunMode.NoOverhead;

        /// <inheritdoc/>
        public void Handle(HostSignal signal, DiagnoserActionParameters parameters)
        {
            switch (signal)
            {
                case HostSignal.BeforeActualRun:
                    if (TryReadProcessCpu(parameters, out var start))
                        beforeActualRun[parameters.BenchmarkCase] = start;
                    break;

                case HostSignal.AfterActualRun:
                    if (beforeActualRun.TryRemove(parameters.BenchmarkCase, out var begin) &&
                        TryReadProcessCpu(parameters, out var end))
                    {
                        // Record even a zero delta: a wake-based wait legitimately burns ~0 CPU, and
                        // dropping that would hide the very measurement this diagnoser exists to show.
                        afterActualRun[parameters.BenchmarkCase] = new CpuSample(
                            Clamp(end.User - begin.User),
                            Clamp(end.Kernel - begin.Kernel));
                    }
                    break;
            }
        }

        /// <inheritdoc/>
        public IEnumerable<Metric> ProcessResults(DiagnoserResults results)
        {
            // BenchmarkCase has no value equality, so the dictionary matches by reference identity.
            // BDN threads the same BenchmarkCase instance through the signal and result pipeline, so a
            // miss here means the actual-run boundary never recorded a reading for this case; count it
            // so DisplayResults surfaces it instead of silently dropping the CPU column for that row.
            if (!afterActualRun.TryRemove(results.BenchmarkCase, out var sample))
            {
                Interlocked.Increment(ref unmatchedResults);
                yield break;
            }

            if (results.TotalOperations > 0)
            {
                // 1 tick == 100 ns; normalize the per-op CPU burn to nanoseconds.
                var ops = results.TotalOperations;
                yield return new Metric(KernelCpuDescriptor, sample.Kernel.Ticks * 100.0 / ops);
                yield return new Metric(UserCpuDescriptor, sample.User.Ticks * 100.0 / ops);
                yield return new Metric(TotalCpuDescriptor, (sample.User + sample.Kernel).Ticks * 100.0 / ops);
            }
        }

        /// <inheritdoc/>
        public void DisplayResults(ILogger logger)
        {
            var missed = Interlocked.Exchange(ref unmatchedResults, 0);
            if (missed > 0)
                logger.WriteLine(LogKind.Warning,
                    $"{nameof(CpuDiagnoser)}: {missed} benchmark case(s) produced no CPU reading (missing actual-run CPU sample).");
        }

        /// <inheritdoc/>
        public IEnumerable<ValidationError> Validate(ValidationParameters validationParameters) => [];

        static bool TryReadProcessCpu(DiagnoserActionParameters parameters, out CpuSample sample)
        {
            sample = default;
            var process = parameters.Process;
            if (process is null)
                return false;

            try
            {
                process.Refresh();
                sample = new CpuSample(process.UserProcessorTime, process.PrivilegedProcessorTime);
                return true;
            }
            catch (InvalidOperationException)
            {
                // Process already exited; no CPU reading is available for this boundary.
                return false;
            }
        }

        static TimeSpan Clamp(TimeSpan value) => value > TimeSpan.Zero ? value : TimeSpan.Zero;
    }

    /// <summary>
    /// Applies <see cref="CpuDiagnoser"/> to a benchmark, adding the "KernelMode CPU", "UserMode CPU",
    /// and "Total CPU" columns. Mirrors the usage of <see cref="MemoryDiagnoserAttribute"/>: annotate a
    /// benchmark class with <c>[CpuDiagnoser]</c>.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public sealed class CpuDiagnoserAttribute : Attribute, IConfigSource
    {
        /// <inheritdoc/>
        public IConfig Config { get; }

        /// <summary>Creates the attribute, registering the shared <see cref="CpuDiagnoser"/>.</summary>
        public CpuDiagnoserAttribute()
        {
            Config = ManualConfig.CreateEmpty().AddDiagnoser(CpuDiagnoser.Default);
        }
    }
}