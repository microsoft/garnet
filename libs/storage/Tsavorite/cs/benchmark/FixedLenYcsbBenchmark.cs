// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// Define below to enable continuous performance report for dashboard
// #define DASHBOARD

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    internal class Tsavorite_YcsbBenchmark
    {
        // Ensure sizes are aligned to chunk sizes
        static long InitCount;
        static long TxnCount;

        readonly TestLoader testLoader;
        readonly ManualResetEventSlim waiter = new();
        readonly int numaStyle;
        readonly int readPercent, upsertPercent, rmwPercent;
        readonly Functions functions;
        readonly Input[] input_;

        readonly Key[] init_keys_;
        readonly Key[] txn_keys_;

        readonly IDevice device;
        readonly TsavoriteKV<Key, Value> store;

        long idx_ = 0;
        long total_ops_done = 0;
        volatile bool done = false;

        internal Tsavorite_YcsbBenchmark(Key[] i_keys_, Key[] t_keys_, TestLoader testLoader)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // Affinize main thread to last core on first socket if not used by experiment
                var (numGrps, numProcs) = Native32.GetNumGroupsProcsPerGroup();
                if ((testLoader.Options.NumaStyle == 0 && testLoader.Options.ThreadCount <= (numProcs - 1)) ||
                    (testLoader.Options.NumaStyle == 1 && testLoader.Options.ThreadCount <= numGrps * (numProcs - 1)))
                    Native32.AffinitizeThreadRoundRobin(numProcs - 1);
            }
            this.testLoader = testLoader;
            init_keys_ = i_keys_;
            txn_keys_ = t_keys_;
            numaStyle = testLoader.Options.NumaStyle;
            readPercent = testLoader.ReadPercent;
            upsertPercent = testLoader.UpsertPercent;
            rmwPercent = testLoader.RmwPercent;
            functions = new Functions();

#if DASHBOARD
            statsWritten = new AutoResetEvent[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                statsWritten[i] = new AutoResetEvent(false);
            }
            threadThroughput = new double[threadCount];
            threadAverageLatency = new double[threadCount];
            threadMaximumLatency = new double[threadCount];
            threadProgress = new long[threadCount];
            writeStats = new bool[threadCount];
            freq = Stopwatch.Frequency;
#endif

            input_ = new Input[8];
            for (int i = 0; i < 8; i++)
                input_[i].value = i;

            var revivificationSettings = testLoader.Options.RevivificationLevel switch
            {
                RevivificationLevel.None => default,
                RevivificationLevel.Chain => new RevivificationSettings(),
                RevivificationLevel.Full => RevivificationSettings.DefaultFixedLength.Clone(),
                _ => throw new ApplicationException("Invalid RevivificationLevel")
            };

            if (testLoader.Options.RevivificationLevel == RevivificationLevel.Full)
                revivificationSettings.FreeRecordBins[0].NumberOfRecords = testLoader.Options.RevivBinRecordCount;
            if (revivificationSettings is not null)
            {
                revivificationSettings.RevivifiableFraction = testLoader.Options.RevivifiableFraction;
                revivificationSettings.RestoreDeletedRecordsIfBinIsFull = true;
            }

            device = Devices.CreateLogDevice(TestLoader.DevicePath, preallocateFile: true, deleteOnClose: !testLoader.RecoverMode, useIoCompletionPort: true);

            if (testLoader.Options.ThreadCount >= 16)
                device.ThrottleLimit = testLoader.Options.ThreadCount * 12;

            if (testLoader.Options.UseSmallMemoryLog)
                store = new TsavoriteKV<Key, Value>
                    (testLoader.GetHashTableSize(), new LogSettings { LogDevice = device, PreallocateLog = true, PageSizeBits = 25, SegmentSizeBits = 30, MemorySizeBits = 28 },
                    new CheckpointSettings { CheckpointDir = testLoader.BackupPath }, concurrencyControlMode: testLoader.Options.ConcurrencyControlMode, revivificationSettings: revivificationSettings);
            else
                store = new TsavoriteKV<Key, Value>
                    (testLoader.GetHashTableSize(), new LogSettings { LogDevice = device, PreallocateLog = true },
                    new CheckpointSettings { CheckpointDir = testLoader.BackupPath }, concurrencyControlMode: testLoader.Options.ConcurrencyControlMode, revivificationSettings: revivificationSettings);
        }

        internal void Dispose()
        {
            store.Dispose();
            device.Dispose();
        }

        private void RunYcsbUnsafeContext(int thread_idx)
        {
            RandomGenerator rng = new((uint)(1 + thread_idx));

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
                else
                    Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets
            }
            waiter.Wait();

            var sw = Stopwatch.StartNew();

            Value value = default;
            Input input = default;
            Output output = default;

            long reads_done = 0;
            long writes_done = 0;
            long deletes_done = 0;

#if DASHBOARD
            var tstart = Stopwatch.GetTimestamp();
            var tstop1 = tstart;
            var lastWrittenValue = 0;
            int count = 0;
#endif

            var session = store.NewSession<Input, Output, Empty, Functions>(functions);
            var uContext = session.UnsafeContext;
            uContext.BeginUnsafe();

            try
            {
                while (!done)
                {
                    long chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                    while (chunk_idx >= TxnCount)
                    {
                        if (chunk_idx == TxnCount)
                            idx_ = 0;
                        chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                    }

                    for (long idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize && !done; ++idx)
                    {
                        if (idx % 512 == 0)
                        {
                            uContext.Refresh();
                            uContext.CompletePending(false);
                        }

                        int r = (int)rng.Generate(100);     // rng.Next() is not inclusive of the upper bound so this will be <= 99
                        if (r < readPercent)
                        {
                            uContext.Read(ref txn_keys_[idx], ref input, ref output, Empty.Default, 1);
                            ++reads_done;
                            continue;
                        }
                        if (r < upsertPercent)
                        {
                            uContext.Upsert(ref txn_keys_[idx], ref value, Empty.Default, 1);
                            ++writes_done;
                            continue;
                        }
                        if (r < rmwPercent)
                        {
                            uContext.RMW(ref txn_keys_[idx], ref input_[idx & 0x7], Empty.Default, 1);
                            ++writes_done;
                            continue;
                        }
                        uContext.Delete(ref txn_keys_[idx], Empty.Default, 1);
                        ++deletes_done;
                    }

#if DASHBOARD
                    count += (int)kChunkSize;

                    //Check if stats collector is requesting for statistics
                    if (writeStats[thread_idx])
                    {
                        var tstart1 = tstop1;
                        tstop1 = Stopwatch.GetTimestamp();
                        threadProgress[thread_idx] = count;
                        threadThroughput[thread_idx] = (count - lastWrittenValue) / ((tstop1 - tstart1) / freq);
                        lastWrittenValue = count;
                        writeStats[thread_idx] = false;
                        statsWritten[thread_idx].Set();
                    }
#endif
                }

                uContext.CompletePending(true);
            }
            finally
            {
                uContext.EndUnsafe();
            }

            session.Dispose();

            sw.Stop();

#if DASHBOARD
            statsWritten[thread_idx].Set();
#endif

            Console.WriteLine($"Thread {thread_idx} done; {reads_done} reads, {writes_done} writes, {deletes_done} deletes in {sw.ElapsedMilliseconds} ms.");
            Interlocked.Add(ref total_ops_done, reads_done + writes_done + deletes_done);
        }

        private void RunYcsbSafeContext(int thread_idx)
        {
            RandomGenerator rng = new((uint)(1 + thread_idx));

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
                else
                    Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets
            }
            waiter.Wait();

            var sw = Stopwatch.StartNew();

            Value value = default;
            Input input = default;
            Output output = default;

            long reads_done = 0;
            long writes_done = 0;
            long deletes_done = 0;

            var session = store.NewSession<Input, Output, Empty, Functions>(functions);

            while (!done)
            {
                long chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                while (chunk_idx >= TxnCount)
                {
                    if (chunk_idx == TxnCount)
                        idx_ = 0;
                    chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                }

                for (long idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize && !done; ++idx)
                {
                    if (idx % 512 == 0)
                        session.CompletePending(false);

                    int r = (int)rng.Generate(100);     // rng.Next() is not inclusive of the upper bound so this will be <= 99
                    if (r < readPercent)
                    {
                        session.Read(ref txn_keys_[idx], ref input, ref output, Empty.Default, 1);
                        ++reads_done;
                        continue;
                    }
                    if (r < upsertPercent)
                    {
                        session.Upsert(ref txn_keys_[idx], ref value, Empty.Default, 1);
                        ++writes_done;
                        continue;
                    }
                    if (r < rmwPercent)
                    {
                        session.RMW(ref txn_keys_[idx], ref input_[idx & 0x7], Empty.Default, 1);
                        ++writes_done;
                        continue;
                    }
                    session.Delete(ref txn_keys_[idx], Empty.Default, 1);
                    ++deletes_done;
                }
            }

            session.CompletePending(true);
            session.Dispose();

            sw.Stop();

            Console.WriteLine($"Thread {thread_idx} done; {reads_done} reads, {writes_done} writes, {deletes_done} deletes in {sw.ElapsedMilliseconds} ms.");
            Interlocked.Add(ref total_ops_done, reads_done + writes_done);
        }

        internal unsafe (double insPerSec, double opsPerSec, long tailAddress) Run(TestLoader testLoader)
        {
#if DASHBOARD
            var dash = new Thread(() => DoContinuousMeasurements());
            dash.Start();
#endif

            Thread[] workers = new Thread[testLoader.Options.ThreadCount];

            Console.WriteLine("Executing setup.");

            var storeWasRecovered = testLoader.MaybeRecoverStore(store);
            long elapsedMs = 0;
            if (!storeWasRecovered)
            {
                // Setup the store for the YCSB benchmark.
                Console.WriteLine("Loading TsavoriteKV from data");
                for (int idx = 0; idx < testLoader.Options.ThreadCount; ++idx)
                {
                    int x = idx;
                    if (testLoader.Options.UseSafeContext)
                        workers[idx] = new Thread(() => SetupYcsbSafeContext(x));
                    else
                        workers[idx] = new Thread(() => SetupYcsbUnsafeContext(x));
                }

                foreach (Thread worker in workers)
                {
                    worker.Start();
                }

                waiter.Set();
                var sw = Stopwatch.StartNew();
                foreach (Thread worker in workers)
                {
                    worker.Join();
                }
                sw.Stop();
                elapsedMs = sw.ElapsedMilliseconds;
                waiter.Reset();
            }
            double insertsPerSecond = elapsedMs == 0 ? 0 : ((double)InitCount / elapsedMs) * 1000;
            Console.WriteLine(TestStats.GetLoadingTimeLine(insertsPerSecond, elapsedMs));
            Console.WriteLine(TestStats.GetAddressesLine(AddressLineNum.Before, store.Log.BeginAddress, store.Log.HeadAddress, store.Log.ReadOnlyAddress, store.Log.TailAddress));

            if (!storeWasRecovered)
                testLoader.MaybeCheckpointStore(store);

            // Uncomment below to dispose log from memory, use for 100% read workloads only
            // store.Log.DisposeFromMemory();

            idx_ = 0;

            if (testLoader.Options.DumpDistribution)
                Console.WriteLine(store.DumpDistribution());

            // Ensure first fold-over checkpoint is fast
            if (testLoader.Options.PeriodicCheckpointMilliseconds > 0 && testLoader.Options.PeriodicCheckpointType == CheckpointType.FoldOver)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, true);

            Console.WriteLine("Executing experiment.");

            // Run the experiment.
            for (int idx = 0; idx < testLoader.Options.ThreadCount; ++idx)
            {
                int x = idx;
                if (testLoader.Options.UseSafeContext)
                    workers[idx] = new Thread(() => RunYcsbSafeContext(x));
                else
                    workers[idx] = new Thread(() => RunYcsbUnsafeContext(x));
            }
            // Start threads.
            foreach (Thread worker in workers)
            {
                worker.Start();
            }

            waiter.Set();
            var swatch = Stopwatch.StartNew();

            if (testLoader.Options.PeriodicCheckpointMilliseconds <= 0)
            {
                Thread.Sleep(TimeSpan.FromSeconds(testLoader.Options.RunSeconds));
            }
            else
            {
                var checkpointTaken = 0;
                while (swatch.ElapsedMilliseconds < 1000 * testLoader.Options.RunSeconds)
                {
                    if (checkpointTaken < swatch.ElapsedMilliseconds / testLoader.Options.PeriodicCheckpointMilliseconds)
                    {
                        long start = swatch.ElapsedTicks;
                        if (store.TryInitiateHybridLogCheckpoint(out _, testLoader.Options.PeriodicCheckpointType, testLoader.Options.PeriodicCheckpointTryIncremental))
                        {
                            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
                            var timeTaken = (swatch.ElapsedTicks - start) / TimeSpan.TicksPerMillisecond;
                            Console.WriteLine("Checkpoint time: {0}ms", timeTaken);
                            checkpointTaken++;
                        }
                    }
                }
                Console.WriteLine($"Checkpoint taken {checkpointTaken}");
            }

            swatch.Stop();

            done = true;

            foreach (Thread worker in workers)
            {
                worker.Join();
            }

            waiter.Reset();

#if DASHBOARD
            dash.Join();
#endif

            double seconds = swatch.ElapsedMilliseconds / 1000.0;
            Console.WriteLine(TestStats.GetAddressesLine(AddressLineNum.After, store.Log.BeginAddress, store.Log.HeadAddress, store.Log.ReadOnlyAddress, store.Log.TailAddress));

            double opsPerSecond = total_ops_done / seconds;
            Console.WriteLine(TestStats.GetTotalOpsString(total_ops_done, seconds));
            Console.WriteLine(TestStats.GetStatsLine(StatsLineNum.Iteration, YcsbConstants.OpsPerSec, opsPerSecond));
            return (insertsPerSecond, opsPerSecond, store.Log.TailAddress);
        }

        private void SetupYcsbUnsafeContext(int thread_idx)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
                else
                    Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets
            }
            waiter.Wait();

            var session = store.NewSession<Input, Output, Empty, Functions>(functions);
            var uContext = session.UnsafeContext;
            uContext.BeginUnsafe();

#if DASHBOARD
            var tstart = Stopwatch.GetTimestamp();
            var tstop1 = tstart;
            var lastWrittenValue = 0;
            int count = 0;
#endif

            Value value = default;

            try
            {
                for (long chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                    chunk_idx < InitCount;
                    chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize)
                {
                    for (long idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize; ++idx)
                    {
                        if (idx % 256 == 0)
                        {
                            uContext.Refresh();

                            if (idx % 65536 == 0)
                            {
                                uContext.CompletePending(false);
                            }
                        }

                        uContext.Upsert(ref init_keys_[idx], ref value, Empty.Default, 1);
                    }
#if DASHBOARD
                count += (int)kChunkSize;

                //Check if stats collector is requesting for statistics
                if (writeStats[thread_idx])
                {
                    var tstart1 = tstop1;
                    tstop1 = Stopwatch.GetTimestamp();
                    threadThroughput[thread_idx] = (count - lastWrittenValue) / ((tstop1 - tstart1) / freq);
                    lastWrittenValue = count;
                    writeStats[thread_idx] = false;
                    statsWritten[thread_idx].Set();
                }
#endif
                }
                uContext.CompletePending(true);
            }
            finally
            {
                uContext.EndUnsafe();
            }
            session.Dispose();
        }

        private void SetupYcsbSafeContext(int thread_idx)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
                else
                    Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets
            }
            waiter.Wait();

            var session = store.NewSession<Input, Output, Empty, Functions>(functions);

            Value value = default;

            for (long chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                chunk_idx < InitCount;
                chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize)
            {
                for (long idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize; ++idx)
                {
                    if (idx % 256 == 0)
                    {
                        session.Refresh();

                        if (idx % 65536 == 0)
                        {
                            session.CompletePending(false);
                        }
                    }

                    session.Upsert(ref init_keys_[idx], ref value, Empty.Default, 1);
                }
            }

            session.CompletePending(true);
            session.Dispose();
        }

#if DASHBOARD
        int measurementInterval = 2000;
        bool measureLatency;
        bool[] writeStats;
        private EventWaitHandle[] statsWritten;
        double[] threadThroughput;
        double[] threadAverageLatency;
        double[] threadMaximumLatency;
        long[] threadProgress;
        double freq;

        void DoContinuousMeasurements()
        {
            double totalThroughput, totalLatency, maximumLatency;
            double totalProgress;
            int ver = 0;

            using (var client = new WebClient())
            {
                while (!done)
                {
                    ver++;

                    Thread.Sleep(measurementInterval);

                    totalProgress = 0;
                    totalThroughput = 0;
                    totalLatency = 0;
                    maximumLatency = 0;

                    for (int i = 0; i < threadCount; i++)
                    {
                        writeStats[i] = true;
                    }


                    for (int i = 0; i < threadCount; i++)
                    {
                        statsWritten[i].WaitOne();
                        totalThroughput += threadThroughput[i];
                        totalProgress += threadProgress[i];
                        if (measureLatency)
                        {
                            totalLatency += threadAverageLatency[i];
                            if (threadMaximumLatency[i] > maximumLatency)
                            {
                                maximumLatency = threadMaximumLatency[i];
                            }
                        }
                    }

                    if (measureLatency)
                    {
                        Console.WriteLine("{0} \t {1:0.000} \t {2} \t {3} \t {4} \t {5}", ver, totalThroughput / (double)1000000, totalLatency / threadCount, maximumLatency, store.Log.TailAddress, totalProgress);
                    }
                    else
                    {
                        Console.WriteLine("{0} \t {1:0.000} \t {2} \t {3}", ver, totalThroughput / (double)1000000, store.Log.TailAddress, totalProgress);
                    }
                }
            }
        }
#endif

        #region Load Data

        internal static void CreateKeyVectors(TestLoader testLoader, out Key[] i_keys, out Key[] t_keys)
        {
            InitCount = YcsbConstants.kChunkSize * (testLoader.InitCount / YcsbConstants.kChunkSize);
            TxnCount = YcsbConstants.kChunkSize * (testLoader.TxnCount / YcsbConstants.kChunkSize);

            i_keys = new Key[InitCount];
            t_keys = new Key[TxnCount];
        }

        internal class KeySetter : IKeySetter<Key>
        {
            public void Set(Key[] vector, long idx, long value) => vector[idx].value = value;
        }

        #endregion
    }
}