// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using Tsavorite.core;

namespace Tsavorite.benchmark
{
    internal class TsavoriteLogFixedLenYcsbBenchmark
    {
        // Ensure sizes are aligned to chunk sizes
        static long InitCount;
        static long TxnCount;

        readonly TsavoriteLog log;
        readonly IDevice device;
        readonly ManualResetEventSlim waiter = new();
        readonly TestLoader testLoader;
        readonly int numaStyle;

        readonly KeySpanByte[] init_keys_;
        readonly KeySpanByte[] txn_keys_;
        readonly LogInput[] logInput;

        long total_ops_done = 0;
        long total_bytes_written = 0;
        volatile bool done = false;

        internal const int kKeySize = 16;
        internal const int kValueSize = 100;

        internal TsavoriteLogFixedLenYcsbBenchmark(KeySpanByte[] i_keys_, KeySpanByte[] t_keys_, TestLoader testLoader)
        {
            this.testLoader = testLoader;
            init_keys_ = i_keys_;
            txn_keys_ = t_keys_;
            logInput = new LogInput[init_keys_.Length];

            for (var i = 0; i < logInput.Length; i++)
            {
                logInput[i].header = SpanByte.Reinterpret(ref init_keys_[i % txn_keys_.Length]);
                logInput[i].state = SpanByte.Reinterpret(ref init_keys_[i % txn_keys_.Length]);
            }

            var tsavoriteLogSettings = new TsavoriteLogSettings
            {
                MemorySizeBits = testLoader.Options.AofMemorySizeBits(),
                PageSizeBits = testLoader.Options.AofPageSizeBits(),
                LogDevice = testLoader.Options.GetAofDevice(),
                TryRecoverLatest = false,
                SafeTailRefreshFrequencyMs = -1,
                FastCommitMode = true,
                AutoCommit = testLoader.Options.CommitFrequencyMs == 0,
                MutableFraction = 0.9,
            };

            numaStyle = testLoader.Options.NumaStyle;
            device = tsavoriteLogSettings.LogDevice;
            log = new TsavoriteLog(tsavoriteLogSettings);
        }

        internal void Dispose()
        {
            log.Dispose();
            device.Dispose();
        }

        internal (double insPerSec, double opsPerSec, long tailAddress) Run()
        {
            var workers = new Thread[testLoader.Options.ThreadCount];
            for (var idx = 0; idx < testLoader.Options.ThreadCount; ++idx)
            {
                var x = idx;
                workers[idx] = new Thread(() => RunWorker(x));
            }

            // Start threads.
            foreach (var worker in workers)
                worker.Start();

            waiter.Set();
            var swatch = Stopwatch.StartNew();

            // Wait for workers to run for specified period of time
            Thread.Sleep(TimeSpan.FromSeconds(testLoader.Options.RunSeconds));

            done = true;
            foreach (var worker in workers)
                worker.Join();

            waiter.Reset();
            swatch.Stop();

            var seconds = swatch.ElapsedMilliseconds / 1000.0;
            var opsPerSecond = total_ops_done / seconds;
            var MiBperSecond = ((double)total_bytes_written / (double)(swatch.ElapsedMilliseconds / 1000)) / (1000 * 1000);
            Console.WriteLine(TestStats.GetTotalOpsString(total_ops_done, seconds));
            Console.WriteLine(TestStats.GetBandwidthString(MiBperSecond));
            Console.WriteLine(TestStats.GetStatsLine(StatsLineNum.Iteration, YcsbConstants.OpsPerSec, opsPerSecond));
            return (opsPerSecond, opsPerSecond, log.TailAddress);
        }

        private void RunWorker(int threadIdx)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint)threadIdx);
                else
                    Native32.AffinitizeThreadShardedNuma((uint)threadIdx, 2); // assuming two NUMA sockets
            }
            waiter.Wait();

            var sw = Stopwatch.StartNew();
            var offset = (uint)threadIdx;
            var bytesWritten = 0L;
            var opsDone = 0;
            while (!done)
            {
                var chunk_idx = offset * YcsbConstants.kChunkSize;
                for (var idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize && !done; ++idx)
                {
                    var logHeader = new LogHeader { type = 0, storeVersion = threadIdx, sessionID = threadIdx };
                    var key = SpanByte.Reinterpret(ref txn_keys_[idx % txn_keys_.Length]);
                    var value = SpanByte.Reinterpret(ref init_keys_[idx % init_keys_.Length]);
                    var input = logInput[idx % logInput.Length];
                    log.Enqueue(
                        logHeader,
                        ref key,
                        ref value,
                        ref input, out _);

                    bytesWritten += (long)(16 + key.TotalSize + value.TotalSize + input.SerializedLength);
                    opsDone++;
                }

                offset += (uint)testLoader.Options.ThreadCount;
            }

            sw.Stop();

            var MiBperSecond = ((double)bytesWritten / (double)(sw.ElapsedMilliseconds / 1000)) / (1000 * 1000);
            Console.WriteLine($"Thread {threadIdx} done; {bytesWritten} bytes in {sw.ElapsedMilliseconds} ms. {MiBperSecond} MiB/s");
            _ = Interlocked.Add(ref total_ops_done, opsDone);
            _ = Interlocked.Add(ref total_bytes_written, bytesWritten);
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