// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Tsavorite.core;

namespace BenchmarkDotNetTests
{
    /// <summary>
    /// <see cref="LightEpoch"/> operations run concurrently by <see cref="Threads"/> threads sharing one instance.
    /// Each invocation performs a fixed total number of operations split evenly across the threads, so
    /// <c>Threads = 1</c> measures a single uncontended thread and larger counts show how throughput scales as
    /// threads contend on the epoch table, <c>CurrentEpoch</c>, and the drain list. Worker threads are created
    /// once in <see cref="Setup"/> and released per invocation by a barrier, so thread startup is not measured.
    /// </summary>
    public class EpochTests
    {
        const int ResumeSuspendOps = 1_000_000;
        const int ProtectAndDrainOps = 1_000_000;
        const int BumpOps = 100_000;

        [Params(1, 2, 4, 8)]
        public int Threads { get; set; }

        LightEpoch epoch;
        Thread[] workers;
        Barrier start;
        Barrier done;
        Action<int> work;
        int opsPerThread;
        volatile bool stopping;

        [GlobalSetup]
        public void Setup()
        {
            epoch = new LightEpoch();
            stopping = false;

            // The invoking thread is one participant in each barrier, so it waits for every worker to finish.
            start = new Barrier(Threads + 1);
            done = new Barrier(Threads + 1);

            workers = new Thread[Threads];
            for (var t = 0; t < Threads; t++)
            {
                workers[t] = new Thread(WorkerLoop) { IsBackground = true, Name = $"EpochTests-{t}" };
                workers[t].Start();
            }
        }

        [GlobalCleanup]
        public void TearDown()
        {
            stopping = true;
            start.SignalAndWait();
            foreach (var worker in workers)
                worker.Join();

            start.Dispose();
            done.Dispose();
            epoch.Dispose();
        }

        void WorkerLoop()
        {
            while (true)
            {
                start.SignalAndWait();
                if (stopping)
                    return;

                work(opsPerThread);
                done.SignalAndWait();
            }
        }

        void Run(Action<int> work, int totalOps)
        {
            this.work = work;
            opsPerThread = totalOps / Threads;
            start.SignalAndWait();
            done.SignalAndWait();
        }

        [Benchmark]
        public void ResumeSuspend() => Run(RunResumeSuspend, ResumeSuspendOps);

        [Benchmark]
        public void ProtectAndDrain() => Run(RunProtectAndDrain, ProtectAndDrainOps);

        [Benchmark]
        public void BumpCurrentEpoch() => Run(RunBumpCurrentEpoch, BumpOps);

        void RunResumeSuspend(int ops)
        {
            for (var i = 0; i < ops; i++)
            {
                epoch.Resume();
                epoch.Suspend();
            }
        }

        void RunProtectAndDrain(int ops)
        {
            epoch.Resume();
            for (var i = 0; i < ops; i++)
            {
                epoch.ProtectAndDrain();
            }
            epoch.Suspend();
        }

        static void MyAction() { }

        void RunBumpCurrentEpoch(int ops)
        {
            epoch.Resume();
            for (var i = 0; i < ops; i++)
            {
                epoch.BumpCurrentEpoch(MyAction);
            }
            epoch.Suspend();
        }
    }
}