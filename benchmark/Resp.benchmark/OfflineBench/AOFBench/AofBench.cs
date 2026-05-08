// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Text;
using Garnet.server;
using Tsavorite.core;

namespace Resp.benchmark
{
    public class AofBench
    {
        public static GarnetServerOptions GetServerOptions(Options options)
        {
            var serverOptions = new GarnetServerOptions
            {
                ClusterAnnounceEndpoint = new IPEndPoint(IPAddress.Loopback, 6379),
                QuietMode = true,
                IndexMemorySize = options.IndexMemorySize,
                EnableAOF = options.EnableAOF || options.AofBench,
                EnableCluster = options.EnableCluster,
                ClusterConfigFlushFrequencyMs = -1,
                FastAofTruncate = options.EnableCluster && options.UseAofNullDevice,
                UseAofNullDevice = options.UseAofNullDevice,
                AofMemorySize = options.AofMemorySize,
                AofPageSize = options.AofPageSize,
                CommitFrequencyMs = options.CommitFrequencyMs,
                AofPhysicalSublogCount = options.AofPhysicalSublogCount,
                AofReplayTaskCount = options.AofReplayTaskCount,
                ReplicationOffsetMaxLag = 0,
                CheckpointDir = OperatingSystem.IsLinux() ? "/tmp" : null,
            };
            return serverOptions;
        }

        readonly ManualResetEventSlim waiter = new();
        readonly Options options;
        readonly AofGen aofGen;
        readonly AofReplayStream[] aofReplayStream;
        StringBuilder stats = new();
        long total_bytes_processed = 0;
        long total_pages_processed = 0;
        long total_records_replayed = 0;
        long total_records_enqueued = 0;

        volatile bool done = false;

        AofAddress aofTailAddress;
        readonly LightEpoch epoch;

        public AofBench(Options options)
        {
            this.options = options;

            var replayEnabled = options.AofBenchType is AofBenchType.Replay or AofBenchType.ReplayNoResp;
            if (!options.EnableCluster && options.AofBenchType == AofBenchType.Replay)
                throw new Exception("InProc/AofBench with AofBenchType.Replay requires --cluster!");

            var serverOptions = GetServerOptions(options);
            aofGen = new AofGen(options);

            if (options.IsReplayEnabled)
            {
                options.EnableCluster = true;
                var instance = new GarnetServerInstance(options);
                aofGen.primaryId = instance.primaryId;
                aofReplayStream = [.. Enumerable.Range(0, options.AofPhysicalSublogCount).Select(
                    x => new AofReplayStream(instance, threadId: x, startAddress: 64, options))];
            }
            else
            {
                epoch = new LightEpoch();
            }
        }

        public void GenerateData() => aofGen.GenerateData();

        public void Run(int threads)
        {
            var workers = new Thread[threads];

            Console.WriteLine($"Epoch instance count:{LightEpoch.ActiveInstanceCount()}");

            try
            {
                var msg = options.AofBenchType switch
                {
                    AofBenchType.Replay or AofBenchType.ReplayNoResp or AofBenchType.ReplayDirect => $">>> Running {options.AofBenchType} using {threads}x{options.AofReplayTaskCount} worker(s) >>>",
                    AofBenchType.EnqueueSharded or AofBenchType.EnqueueRandom => $">>> Running {options.AofBenchType} using {threads} worker(s) >>>",
                    _ => throw new Exception($"AofBenchType {options.AofBenchType} not supported"),
                };
                Console.WriteLine(msg);

                if (options.IsReplayEnabled)
                    aofTailAddress = aofGen.appendOnlyFile.Log.TailAddress;

                // Run the experiment.
                for (var idx = 0; idx < threads; ++idx)
                {
                    var x = idx;
                    workers[idx] = options.AofBenchType switch
                    {
                        AofBenchType.Replay => new Thread(() => RunAofReplayBench(x)),
                        AofBenchType.ReplayNoResp => new Thread(() => RunAofReplayBenchNoResp(x)),
                        AofBenchType.ReplayDirect => new Thread(() => RunAofReplayBenchDirect(x)),
                        AofBenchType.EnqueueSharded or AofBenchType.EnqueueRandom => new Thread(() => RunAofEnqueueBench(x)),
                        _ => throw new Exception($"AofBenchType {options.AofBenchType} not supported"),
                    };
                }

                // Start threads.
                foreach (var worker in workers)
                    worker.Start();

                waiter.Set();

                Stopwatch swatch = new();
                swatch.Start();
                // Let workers operate for a specific RunTime
                Thread.Sleep(TimeSpan.FromSeconds(options.RunTime));
                done = true;

                // Wait for AOF load to complete
                foreach (var worker in workers)
                    worker.Join();

                swatch.Stop();

                var seconds = swatch.ElapsedMilliseconds / 1000.0;
                if (options.IsReplayEnabled)
                {
                    var bytesPerSecond = (total_bytes_processed / seconds) / (double)1_000_000_000;
                    var recordsReplayedPerSecond = total_records_replayed / seconds;
                    Console.WriteLine($"[Total time]: {swatch.ElapsedMilliseconds:N2} ms for {total_bytes_processed:N0} AOF bytes");
                    Console.WriteLine($"[Bandwidth]: {bytesPerSecond:N2} GiB/sec");
                    Console.WriteLine($"[Total pages send]: {total_pages_processed:N0}");
                    Console.WriteLine($"[Total records replayed]: {total_records_replayed:N0}");
                    Console.WriteLine($"[Throughput]: {recordsReplayedPerSecond:N2} records/sec");
                }
                else
                {
                    var bytesPerSecond = (total_bytes_processed / seconds) / (double)1_000_000_000;
                    var recordsEnqueuedPerSecond = total_records_enqueued / seconds;
                    Console.WriteLine($"[Total time]: {swatch.ElapsedMilliseconds:N2} ms for {total_bytes_processed:N0} AOF bytes");
                    Console.WriteLine($"[Bandwidth]: {bytesPerSecond:N2} GiB/sec");
                    Console.WriteLine($"[Total records enqueued]: {total_records_enqueued:N0}");
                    Console.WriteLine($"[Throughput]: {recordsEnqueuedPerSecond:N2} records/sec");
                }
            }
            finally
            {
                done = false;
                total_records_replayed = 0;
                total_records_enqueued = 0;
                total_bytes_processed = 0;
                waiter.Reset();
                Console.WriteLine("------------------------------");
            }
        }

        unsafe void RunAofEnqueueBench(int threadId)
        {
            waiter.Wait();
            var kvPairs = aofGen.GetKVPairBuffer(threadId);
            var recordsEnqueued = 0L;
            var bytesEnqueued = 0L;
            while (!done)
            {
                for (var i = 0; i < kvPairs.Count; i++)
                {
                    if (done) break;
                    var kvPair = kvPairs[i];
                    var kb = kvPair.Item1;
                    var vb = kvPair.Item2;
                    fixed (byte* keyPtr = kb)
                    fixed (byte* valPtr = vb)
                    {
                        var key = SpanByte.FromPinnedPointer(keyPtr, kb.Length);
                        var value = SpanByte.FromPinnedPointer(valPtr, vb.Length);
                        StringInput input = default;
                        aofGen.appendOnlyFile.Log.Enqueue(
                            AofEntryType.StoreUpsert,
                            1,
                            threadId,
                            key,
                            value,
                            ref input,
                            epoch,
                            out _);
                        bytesEnqueued += sizeof(AofShardedHeader) + key.TotalSize() + value.TotalSize() + input.SerializedLength;
                    }
                    recordsEnqueued++;
                }

                if (done) break;
            }
            //Console.WriteLine($"[{threadId}] - Enqueued: {recordsEnqueued:N0} records");
            _ = Interlocked.Add(ref total_records_enqueued, recordsEnqueued);
            _ = Interlocked.Add(ref total_bytes_processed, bytesEnqueued);
        }

        unsafe void RunAofReplayBench(int threadId)
        {
            var messages = aofGen.GetRespReplayMessages(threadId);
            var offset = 0;
            var pagesSend = 0L;
            var totalBytes = 0L;
            var recordsReplayedCount = 0L;
            var pageSize = 1 << options.AofPageSizeBits();

            // Track monotonically increasing addresses for circular replay
            var previousAddress = 64L;
            var currentAddress = 64L;

            waiter.Wait();

            // Initialize stream for replay
            aofReplayStream[threadId].InitializeReplayStream();

            while (!done)
            {
                var pos = offset++ % messages.Length;
                var msg = messages[pos];
                var nextAddress = currentAddress + msg.payloadLength;

                fixed (byte* ptr = msg.buffer)
                {
                    // Update fixed-width address fields in-place for the current replay position.
                    // Addresses are zero-padded to max digits during generation (see WriterClusterAppendLogFixedWidth)
                    // so overwriting here does not change message length.
                    AofGen.PatchAddress(ptr, msg.previousAddressDigitOffset, previousAddress);
                    AofGen.PatchAddress(ptr, msg.currentAddressDigitOffset, currentAddress);
                    AofGen.PatchAddress(ptr, msg.nextAddressDigitOffset, nextAddress);

                    aofReplayStream[threadId].ConsumeResp(ptr + msg.messageOffset, msg.messageLength);

                    pagesSend++;
                    totalBytes += msg.payloadLength;
                    recordsReplayedCount += msg.recordCount;
                }

                previousAddress = nextAddress;
                currentAddress = currentAddress == 64 ? pageSize : currentAddress + pageSize;
            }

            //Console.WriteLine($"[{threadId}] - Pages send: {pagesSend:N0}, Total AOF bytes send: {totalBytes:N0}, Total records replayed:{recordsReplayedCount:N0}");
            _ = Interlocked.Add(ref total_pages_processed, pagesSend);
            _ = Interlocked.Add(ref total_bytes_processed, totalBytes);
            _ = Interlocked.Add(ref total_records_replayed, recordsReplayedCount);
        }

        unsafe void RunAofReplayBenchNoResp(int threadId)
        {
            var buffers = aofGen.GetPageBuffers(threadId);
            var offset = 0;
            var currentAddress = 64L;
            var nextAddress = 64L;
            var pagesSend = 0L;
            var totalBytes = 0L;
            var recordsReplayedCount = 0L;

            waiter.Wait();

            // Initialize stream for replay
            aofReplayStream[threadId].InitializeReplayStream();

            while (!done)
            {
                var pos = offset++ % buffers.Length;
                var currPage = buffers[pos];
                fixed (byte* payloadPtr = currPage.payload)
                {
                    nextAddress = currentAddress + currPage.payloadLength;
                    aofReplayStream[threadId].ConsumeNoResp(payloadPtr, currPage.payloadLength, currentAddress, nextAddress, isProtected: false);

                    // First page has a valid address from 64.
                    // After that currentAddress starts from beginning of bage (i.e. multiple of page size)
                    currentAddress = currentAddress == 64 ? currPage.Length : currentAddress + currPage.Length;
                    pagesSend++;
                    totalBytes += currPage.payloadLength;
                    recordsReplayedCount += currPage.recordCount;
                }
            }

            //Console.WriteLine($"[{threadId}] - Pages send: {pagesSend:N0}, Total AOF bytes send: {totalBytes:N0}, Total records replayed:{recordsReplayedCount:N0}");
            _ = Interlocked.Add(ref total_pages_processed, pagesSend);
            _ = Interlocked.Add(ref total_bytes_processed, totalBytes);
            _ = Interlocked.Add(ref total_records_replayed, recordsReplayedCount);
        }

        unsafe void RunAofReplayBenchDirect(int threadId)
        {
            var buffers = aofGen.GetPageBuffers(threadId);
            var offset = 0;
            var currentAddress = 64L;
            var nextAddress = 64L;
            var pagesSend = 0L;
            var totalBytes = 0L;
            var recordsReplayedCount = 0L;

            waiter.Wait();

            // Initialize stream for replay
            aofReplayStream[threadId].InitializeReplayStream();

            while (!done)
            {
                var pos = offset++ % buffers.Length;
                var currPage = buffers[pos];
                fixed (byte* payloadPtr = currPage.payload)
                {
                    nextAddress = currentAddress + currPage.payloadLength;
                    aofReplayStream[threadId].ConsumeDirect(payloadPtr, currPage.payloadLength, currentAddress, nextAddress, isProtected: false);

                    // First page has a valid address from 64.
                    // After that currentAddress starts from beginning of bage (i.e. multiple of page size)
                    currentAddress = currentAddress == 64 ? currPage.Length : currentAddress + currPage.Length;
                    pagesSend++;
                    totalBytes += currPage.payloadLength;
                    recordsReplayedCount += currPage.recordCount;
                }
            }

            //Console.WriteLine($"[{threadId}] - Pages send: {pagesSend:N0}, Total AOF bytes send: {totalBytes:N0}, Total records replayed:{recordsReplayedCount:N0}");
            _ = Interlocked.Add(ref total_pages_processed, pagesSend);
            _ = Interlocked.Add(ref total_bytes_processed, totalBytes);
            _ = Interlocked.Add(ref total_records_replayed, recordsReplayedCount);
        }
    }
}