// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
#if GARNET_CLIENT_EPOCH_BENCHMARK
namespace BDN.benchmark.Client
#else
namespace BenchmarkDotNetTests
#endif
{
    /// <summary>
    /// Concurrent acquire/release throughput with controlled pending-drain and slot-waiter pressure.
    /// </summary>
    public class EpochReleaseTests
    {
        /// <summary>Number of active workers.</summary>
        [Params(2, 8)]
        public int Threads { get; set; }

        /// <summary>Maximum spin iterations before and after each suspend.</summary>
        [Params(0, 8)]
        public int Jitter { get; set; }

        EpochReleaseWorkload workload;

        [GlobalSetup(Target = nameof(Stable))]
        public void SetupStable() => Setup(EpochReleasePressure.Stable);

        [GlobalSetup(Target = nameof(DrainPressure))]
        public void SetupDrain() => Setup(EpochReleasePressure.Drain);

        [GlobalSetup(Target = nameof(WaiterPressure))]
        public void SetupWaiter() => Setup(EpochReleasePressure.Waiter);

        void Setup(EpochReleasePressure pressure)
        {
            EpochBenchmarkEnvironment.Report();
            workload = new EpochReleaseWorkload(Threads, Jitter, pressure);
            try
            {
                workload.Initialize();
            }
            catch
            {
                workload.Dispose();
                throw;
            }
        }

        [GlobalCleanup]
        public void Cleanup() => workload?.Dispose();

        [Benchmark(OperationsPerInvoke = EpochReleaseWorkload.Operations)]
        public void Stable() => workload.Run();

        [Benchmark(OperationsPerInvoke = EpochReleaseWorkload.Operations)]
        public void DrainPressure() => workload.Run();

        [Benchmark(OperationsPerInvoke = EpochReleaseWorkload.Operations)]
        public void WaiterPressure() => workload.Run();
    }
}