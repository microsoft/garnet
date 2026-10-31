// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
#if GARNET_CLIENT_EPOCH_BENCHMARK
using Garnet.client;

namespace BDN.benchmark.Client
#else
using Tsavorite.core;

namespace BenchmarkDotNetTests
#endif
{
    /// <summary>
    /// Direct epoch costs without worker dispatch, barriers, or concurrent drain activity.
    /// The same source is compiled against the Tsavorite and client implementations.
    /// </summary>
    public class EpochTests
    {
        const int Operations = 100_000;

        LightEpoch epoch;
        EpochBenchmarkEnvironment.Affinity affinity;

        [GlobalSetup]
        public void Setup()
        {
            EpochBenchmarkEnvironment.Report();
            affinity = EpochBenchmarkEnvironment.Pin(0, 1);
            epoch = new LightEpoch();
            EpochWorkloads.ResumeSuspend(epoch, 1_000);
        }

        [GlobalCleanup]
        public void TearDown()
        {
            try
            {
                epoch?.Dispose();
            }
            finally
            {
                affinity?.Dispose();
            }
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public void ResumeSuspend() => EpochWorkloads.ResumeSuspend(epoch, Operations);

        [Benchmark(OperationsPerInvoke = Operations)]
        public void ProtectAndDrain() => EpochWorkloads.ProtectAndDrain(epoch, Operations);

        [Benchmark(OperationsPerInvoke = Operations)]
        public void BumpAndSuspend() => EpochWorkloads.BumpAndSuspend(epoch, Operations);
    }

    /// <summary>
    /// Aggregate epoch throughput under contention, measured separately from direct operation costs.
    /// Thread creation and metadata warmup happen before measurement.
    /// </summary>
    public class EpochContentionTests
    {
        const int ResumeSuspendOps = 1_000_000;
        const int BumpOps = 100_000;

        [Params(1, 2, 4, 8)]
        public int Threads { get; set; }

        enum Workload
        {
            ResumeSuspend,
            BumpAndSuspend,
            BumpWhileProtected
        }

        LightEpoch epoch;
        Thread[] workers;
        Exception[] workerErrors;
        Barrier start;
        Barrier done;
        CountdownEvent ready;
        Workload workload;
        int opsPerThread;
        volatile bool stopping;

        [GlobalSetup]
        public void Setup()
        {
            EpochBenchmarkEnvironment.Report();
            epoch = new LightEpoch();
            stopping = false;
            start = new Barrier(Threads + 1);
            done = new Barrier(Threads + 1);
            ready = new CountdownEvent(Threads);
            workers = new Thread[Threads];
            workerErrors = new Exception[Threads];
            for (var t = 0; t < Threads; t++)
            {
                var index = t;
                workers[t] = new Thread(() => WorkerLoop(index)) { IsBackground = true, Name = $"EpochContentionTests-{t}" };
                workers[t].Start();
            }
            ready.Wait();
            try
            {
                ThrowWorkerErrors();
            }
            catch
            {
                TearDown();
                throw;
            }
        }

        [GlobalCleanup]
        public void TearDown()
        {
            if (stopping)
                return;
            stopping = true;
            start.SignalAndWait();
            foreach (var worker in workers)
                worker.Join();
            start.Dispose();
            done.Dispose();
            ready.Dispose();
            epoch.Dispose();
        }

        void WorkerLoop(int index)
        {
            EpochBenchmarkEnvironment.Affinity affinity = null;
            try
            {
                try
                {
                    affinity = EpochBenchmarkEnvironment.Pin(index, Threads);
                    EpochWorkloads.ResumeSuspend(epoch, 1_000);
                }
                catch (Exception exception)
                {
                    workerErrors[index] = exception;
                }
                finally
                {
                    ready.Signal();
                }
                while (true)
                {
                    start.SignalAndWait();
                    if (stopping)
                        return;
                    try
                    {
                        if (workerErrors[index] != null)
                            continue;
                        switch (workload)
                        {
                            case Workload.ResumeSuspend:
                                EpochWorkloads.ResumeSuspend(epoch, opsPerThread);
                                break;
                            case Workload.BumpAndSuspend:
                                EpochWorkloads.BumpAndSuspend(epoch, opsPerThread);
                                break;
                            case Workload.BumpWhileProtected:
                                EpochWorkloads.BumpWhileProtected(epoch, opsPerThread);
                                break;
                        }
                    }
                    catch (Exception exception)
                    {
                        workerErrors[index] = exception;
                    }
                    finally
                    {
                        done.SignalAndWait();
                    }
                }
            }
            finally
            {
                affinity?.Dispose();
            }
        }

        void Run(Workload workload, int totalOps)
        {
            this.workload = workload;
            opsPerThread = totalOps / Threads;
            start.SignalAndWait();
            done.SignalAndWait();
            ThrowWorkerErrors();
        }

        void ThrowWorkerErrors()
        {
            foreach (var error in workerErrors)
            {
                if (error != null)
                    throw new AggregateException("Epoch benchmark worker failed", workerErrors.Where(error => error != null));
            }
        }

        [Benchmark(OperationsPerInvoke = ResumeSuspendOps)]
        public void ResumeSuspend() => Run(Workload.ResumeSuspend, ResumeSuspendOps);

        [Benchmark(OperationsPerInvoke = BumpOps)]
        public void BumpAndSuspend() => Run(Workload.BumpAndSuspend, BumpOps);

        [Benchmark(OperationsPerInvoke = BumpOps)]
        public void BumpWhileProtected() => Run(Workload.BumpWhileProtected, BumpOps);
    }

    static class EpochWorkloads
    {
        internal static void ResumeSuspend(LightEpoch epoch, int ops)
        {
            for (var i = 0; i < ops; i++)
            {
                epoch.Resume();
                epoch.Suspend();
            }
        }

        internal static void ProtectAndDrain(LightEpoch epoch, int ops)
        {
            epoch.Resume();
            try
            {
                for (var i = 0; i < ops; i++)
                    epoch.ProtectAndDrain();
            }
            finally
            {
                epoch.Suspend();
            }
        }

        static void MyAction() { }

        internal static void BumpAndSuspend(LightEpoch epoch, int ops)
        {
            for (var i = 0; i < ops; i++)
            {
                epoch.Resume();
                try
                {
                    epoch.BumpCurrentEpoch(MyAction);
                }
                finally
                {
                    epoch.Suspend();
                }
            }
        }

        internal static void BumpWhileProtected(LightEpoch epoch, int ops)
        {
            epoch.Resume();
            try
            {
                for (var i = 0; i < ops; i++)
                    epoch.BumpCurrentEpoch(MyAction);
            }
            finally
            {
                epoch.Suspend();
            }
        }
    }
}