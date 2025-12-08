// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Net;
using System.Text;
using Embedded.server;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Resp.benchmark
{
    public enum AofBenchType
    {
        EnqueueRandom,
        EnqueueSharded,
        Replay
    }

    public class AofBench
    {
        public static GarnetServerOptions GetServerOptions(Options options)
        {
            var serverOptions = new GarnetServerOptions
            {
                ClusterAnnounceEndpoint = new IPEndPoint(IPAddress.Loopback, 6379),
                QuietMode = true,
                EnableAOF = options.EnableAOF,
                EnableCluster = options.EnableCluster,
                IndexSize = options.IndexSize,
                ClusterConfigFlushFrequencyMs = -1,
                FastAofTruncate = options.EnableCluster && options.UseAofNullDevice,
                UseAofNullDevice = options.UseAofNullDevice,
                AofMemorySize = options.AofMemorySize,
                AofPageSize = options.AofPageSize,
                CommitFrequencyMs = options.CommitFrequencyMs,
                AofSublogCount = options.AofSublogCount,
                ReplicationOffsetMaxLag = 0,
                CheckpointDir = OperatingSystem.IsLinux() ? "/tmp" : null
            };
            return serverOptions;
        }

        readonly ManualResetEventSlim waiter = new();
        readonly Options options;
        readonly AofGen aofGen;
        readonly AofSync[] aofSync;
        StringBuilder stats = new();
        long total_bytes_processed = 0;
        long total_pages_processed = 0;
        long total_records_replayed = 0;
        long total_records_enqueued = 0;

        internal EmbeddedRespServer server;
        internal RespServerSession[] sessions;

        volatile bool done = false;

        internal readonly string primaryId;

        AofAddress aofTailAddress;

        public AofBench(Options options)
        {
            this.options = options;

            if (options.Client == ClientType.InProc && !options.EnableCluster && options.AofBenchType == AofBenchType.Replay)
                throw new Exception("InProc AofBench requires --cluster!");

            var serverOptions = GetServerOptions(options);

            if (options.Client == ClientType.InProc)
            {
                primaryId = Generator.CreateHexId();
            }

            server = new EmbeddedRespServer(serverOptions, Program.loggerFactory, new GarnetServerEmbedded());
            sessions = server.GetRespSessions(options.AofSublogCount);
            aofGen = new AofGen(options);
            aofSync = [.. Enumerable.Range(0, options.AofSublogCount).Select(x => new AofSync(this, threadId: x, startAddress: 64, options, aofGen))];
        }

        public void GenerateData() => aofGen.GenerateData();

        public void Run(int threads)
        {
            var workers = new Thread[threads];

            try
            {
                Console.WriteLine($">>> Running {options.AofBenchType} using {threads} thread(s) >>>");

                if (options.AofBenchType is AofBenchType.EnqueueRandom or AofBenchType.EnqueueSharded)
                    aofTailAddress = aofGen.appendOnlyFile.Log.TailAddress;

                // Run the experiment.
                for (var idx = 0; idx < threads; ++idx)
                {
                    var x = idx;
                    workers[idx] = options.AofBenchType switch
                    {
                        AofBenchType.Replay => new Thread(() => RunAofReplayBench(x)),
                        AofBenchType.EnqueueSharded or AofBenchType.EnqueueRandom => new Thread(() => RunAofEnqueBench(x)),
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
                if (options.AofBenchType == AofBenchType.Replay)
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

            unsafe void RunAofReplayBench(int threadId)
            {
                var buffers = aofGen.GetPageBuffers(threadId);
                var offset = 0;
                var currentAddress = 64L;
                var nextAddress = 64L;
                var pagesSend = 0L;
                var totalBytes = 0L;
                var recordsReplayedCount = 0L;

                waiter.Wait();

                while (!done)
                {
                    var pos = offset++ % buffers.Length;
                    var currPage = buffers[pos];
                    fixed (byte* payloadPtr = currPage.payload)
                    {
                        nextAddress = currentAddress + currPage.payloadLength;
                        aofSync[threadId].Consume(payloadPtr, currPage.payloadLength, currentAddress, nextAddress, isProtected: false);

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

            unsafe void RunAofEnqueBench(int threadId)
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
                            RawStringInput input = default;
                            if (aofGen.appendOnlyFile.Log.Size == 1)
                            {
                                var aofHeader = new AofHeader
                                {
                                    opType = AofEntryType.StoreUpsert,
                                    storeVersion = 1,
                                    sessionID = threadId,
                                };
                                aofGen.appendOnlyFile.Log.SigleLog.Enqueue(
                                    aofHeader,
                                    key,
                                    value,
                                    ref input,
                                    out _);
                                bytesEnqueued += sizeof(AofHeader) + key.TotalSize() + value.TotalSize() + input.SerializedLength;
                            }
                            else
                            {
                                var extendedAofHeader = new AofExtendedHeader(new AofHeader
                                {
                                    opType = AofEntryType.StoreUpsert,
                                    storeVersion = 1,
                                    sessionID = threadId,
                                },
                                aofGen.appendOnlyFile.seqNumGen.GetSequenceNumber(),
                                0);

                                aofGen.appendOnlyFile.Log.GetSubLog(key).Enqueue(
                                    extendedAofHeader,
                                    key,
                                    value,
                                    ref input,
                                    out _);
                                bytesEnqueued += sizeof(AofExtendedHeader) + key.TotalSize() + value.TotalSize() + input.SerializedLength;
                            }
                        }
                        recordsEnqueued++;
                    }

                    if (done) break;
                }
                //Console.WriteLine($"[{threadId}] - Enqueued: {recordsEnqueued:N0} records");
                _ = Interlocked.Add(ref total_records_enqueued, recordsEnqueued);
                _ = Interlocked.Add(ref total_bytes_processed, bytesEnqueued);
            }
        }
    }
}