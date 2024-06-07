﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    internal class SpanByteYcsbBenchmark
    {
        // Ensure sizes are aligned to chunk sizes
        static long InitCount;
        static long TxnCount;

        readonly TestLoader testLoader;
        readonly ManualResetEventSlim waiter = new();
        readonly int numaStyle;
        readonly int readPercent, upsertPercent, rmwPercent;
        readonly SessionSpanByteFunctions functions;
        readonly Input[] input_;

        readonly KeySpanByte[] init_keys_;
        readonly KeySpanByte[] txn_keys_;

        readonly IDevice device;
        readonly TsavoriteKV<SpanByte, SpanByte> store;

        long idx_ = 0;
        long total_ops_done = 0;
        volatile bool done = false;

        internal const int kKeySize = 16;
        internal const int kValueSize = 100;

        internal SpanByteYcsbBenchmark(KeySpanByte[] i_keys_, KeySpanByte[] t_keys_, TestLoader testLoader)
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
            functions = new SessionSpanByteFunctions();

            input_ = new Input[8];
            for (int i = 0; i < 8; i++)
                input_[i].value = i;

            var revivificationSettings = testLoader.Options.RevivificationLevel switch
            {
                RevivificationLevel.None => default,
                RevivificationLevel.Chain => new RevivificationSettings(),
                RevivificationLevel.Full => new RevivificationSettings()
                {
                    FreeRecordBins = new[]
                        {
                            new RevivificationBin()
                            {
                                RecordSize = RecordInfo.GetLength() + kKeySize + kValueSize + 8,    // extra to ensure rounding up of value
                                NumberOfRecords = testLoader.Options.RevivBinRecordCount,
                                BestFitScanLimit = RevivificationBin.UseFirstFit
                            }
                        },
                },
                _ => throw new ApplicationException("Invalid RevivificationLevel")
            };

            if (revivificationSettings is not null)
            {
                revivificationSettings.RevivifiableFraction = testLoader.Options.RevivifiableFraction;
                revivificationSettings.RestoreDeletedRecordsIfBinIsFull = true;
            }

            device = Devices.CreateLogDevice(TestLoader.DevicePath, preallocateFile: true, deleteOnClose: !testLoader.RecoverMode, useIoCompletionPort: true);

            if (testLoader.Options.UseSmallMemoryLog)
                store = new TsavoriteKV<SpanByte, SpanByte>
                    (testLoader.GetHashTableSize(), new LogSettings { LogDevice = device, PreallocateLog = true, PageSizeBits = 22, SegmentSizeBits = 26, MemorySizeBits = 26 },
                    new CheckpointSettings { CheckpointDir = testLoader.BackupPath }, revivificationSettings: revivificationSettings);
            else
                store = new TsavoriteKV<SpanByte, SpanByte>
                    (testLoader.GetHashTableSize(), new LogSettings { LogDevice = device, PreallocateLog = true, MemorySizeBits = 35 },
                    new CheckpointSettings { CheckpointDir = testLoader.BackupPath }, revivificationSettings: revivificationSettings);
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

            Span<byte> value = stackalloc byte[kValueSize];
            Span<byte> input = stackalloc byte[kValueSize];
            Span<byte> output = stackalloc byte[kValueSize];

            SpanByte _value = SpanByte.FromPinnedSpan(value);
            SpanByte _input = SpanByte.FromPinnedSpan(input);
            SpanByteAndMemory _output = SpanByteAndMemory.FromPinnedSpan(output);

            long reads_done = 0;
            long writes_done = 0;
            long deletes_done = 0;

            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SessionSpanByteFunctions>(functions);
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
                            uContext.Read(ref SpanByte.Reinterpret(ref txn_keys_[idx]), ref _input, ref _output, Empty.Default);
                            ++reads_done;
                            continue;
                        }
                        if (r < upsertPercent)
                        {
                            uContext.Upsert(ref SpanByte.Reinterpret(ref txn_keys_[idx]), ref _value, Empty.Default);
                            ++writes_done;
                            continue;
                        }
                        if (r < rmwPercent)
                        {
                            uContext.RMW(ref SpanByte.Reinterpret(ref txn_keys_[idx]), ref _input, Empty.Default);
                            ++writes_done;
                            continue;
                        }
                        uContext.Delete(ref SpanByte.Reinterpret(ref txn_keys_[idx]), Empty.Default);
                        ++deletes_done;
                    }
                }

                uContext.CompletePending(true);
            }
            finally
            {
                uContext.EndUnsafe();
            }

            sw.Stop();

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

            Span<byte> value = stackalloc byte[kValueSize];
            Span<byte> input = stackalloc byte[kValueSize];
            Span<byte> output = stackalloc byte[kValueSize];

            SpanByte _value = SpanByte.FromPinnedSpan(value);
            SpanByte _input = SpanByte.FromPinnedSpan(input);
            SpanByteAndMemory _output = SpanByteAndMemory.FromPinnedSpan(output);

            long reads_done = 0;
            long writes_done = 0;
            long deletes_done = 0;

            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SessionSpanByteFunctions>(functions);
            var bContext = session.BasicContext;

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
                        if (!testLoader.Options.UseSafeContext)
                            bContext.Refresh();
                        bContext.CompletePending(false);
                    }

                    int r = (int)rng.Generate(100);     // rng.Next() is not inclusive of the upper bound so this will be <= 99
                    if (r < readPercent)
                    {
                        bContext.Read(ref SpanByte.Reinterpret(ref txn_keys_[idx]), ref _input, ref _output, Empty.Default);
                        ++reads_done;
                        continue;
                    }
                    if (r < upsertPercent)
                    {
                        bContext.Upsert(ref SpanByte.Reinterpret(ref txn_keys_[idx]), ref _value, Empty.Default);
                        ++writes_done;
                        continue;
                    }
                    if (r < rmwPercent)
                    {
                        bContext.RMW(ref SpanByte.Reinterpret(ref txn_keys_[idx]), ref _input, Empty.Default);
                        ++writes_done;
                        continue;
                    }
                    bContext.Delete(ref SpanByte.Reinterpret(ref txn_keys_[idx]), Empty.Default);
                    ++deletes_done;
                }
            }

            bContext.CompletePending(true);

            sw.Stop();

            Console.WriteLine($"Thread {thread_idx} done; {reads_done} reads, {writes_done} writes, {deletes_done} deletes in {sw.ElapsedMilliseconds} ms.");
            Interlocked.Add(ref total_ops_done, reads_done + writes_done + deletes_done);
        }

        internal unsafe (double insPerSec, double opsPerSec, long tailAddress) Run(TestLoader testLoader)
        {
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
                    worker.Start();

                waiter.Set();
                var sw = Stopwatch.StartNew();
                foreach (Thread worker in workers)
                    worker.Join();

                sw.Stop();
                waiter.Reset();

                elapsedMs = sw.ElapsedMilliseconds;
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
                worker.Start();

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
                worker.Join();

            waiter.Reset();

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

            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SessionSpanByteFunctions>(functions);
            var uContext = session.UnsafeContext;
            uContext.BeginUnsafe();

            Span<byte> value = stackalloc byte[kValueSize];
            ref SpanByte _value = ref SpanByte.Reinterpret(value);

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

                        uContext.Upsert(ref SpanByte.Reinterpret(ref init_keys_[idx]), ref _value, Empty.Default);
                    }
                }
                uContext.CompletePending(true);
            }
            finally
            {
                uContext.EndUnsafe();
            }
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

            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SessionSpanByteFunctions>(functions);
            var bContext = session.BasicContext;

            Span<byte> value = stackalloc byte[kValueSize];
            ref SpanByte _value = ref SpanByte.Reinterpret(value);

            for (long chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                chunk_idx < InitCount;
                chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize)
            {
                for (long idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize; ++idx)
                {
                    if (idx % 256 == 0)
                    {
                        bContext.Refresh();

                        if (idx % 65536 == 0)
                        {
                            bContext.CompletePending(false);
                        }
                    }

                    bContext.Upsert(ref SpanByte.Reinterpret(ref init_keys_[idx]), ref _value, Empty.Default);
                }
            }

            bContext.CompletePending(true);
        }

        #region Load Data

        internal static void CreateKeyVectors(TestLoader testLoader, out KeySpanByte[] i_keys, out KeySpanByte[] t_keys)
        {
            InitCount = YcsbConstants.kChunkSize * (testLoader.InitCount / YcsbConstants.kChunkSize);
            TxnCount = YcsbConstants.kChunkSize * (testLoader.TxnCount / YcsbConstants.kChunkSize);

            i_keys = new KeySpanByte[InitCount];
            t_keys = new KeySpanByte[TxnCount];
        }

        internal class KeySetter : IKeySetter<KeySpanByte>
        {
            public unsafe void Set(KeySpanByte[] vector, long idx, long value)
            {
                vector[idx].length = kKeySize - 4;
                vector[idx].value = value;
            }
        }

        #endregion
    }
}