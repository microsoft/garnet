// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Text;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Resp.benchmark
{
    public class Page(int size)
    {
        public int Length => payload.Length;
        public byte[] payload = GC.AllocateArray<byte>(size, pinned: true);
        public int payloadLength = 0;
        public int recordCount = 0;
    }

    public sealed class AofGen
    {
        readonly GarnetLog garnetLog;

        public readonly GarnetAppendOnlyFile appendOnlyFile;

        readonly Options options;
        readonly GarnetServerOptions aofServerOptions;

        /// <summary>
        /// threads x pageNum
        /// </summary>
        Page[][] pageBuffers;

        /// <summary>
        /// DBSize kv pairs
        /// </summary>
        List<(byte[], byte[])>[] kvPairBuffers;

        long total_number_of_aof_records = 0L;
        long total_number_of_aof_bytes = 0L;

        public Page[] GetPageBuffers(int threadIdx) => pageBuffers[threadIdx];
        public List<(byte[], byte[])> GetKVPairBuffer(int threadIdx) => kvPairBuffers[threadIdx];

        public AofGen(Options options)
        {
            this.options = options;

            this.aofServerOptions = new GarnetServerOptions()
            {
                EnableAOF = true,
                AofMemorySize = options.AofMemorySize,
                AofPageSize = options.AofPageSize,
                UseAofNullDevice = true,
                EnableFastCommit = true,
                CommitFrequencyMs = -1,
                FastAofTruncate = true,
                AofReplicationRefreshFrequencyMs = 10,
                EnableCluster = true,
                ReplicationOffsetMaxLag = 0,
                AofSublogCount = options.AofSublogCount
            };
            aofServerOptions.GetAofSettings(0, out var logSettings);
            garnetLog = new GarnetLog(aofServerOptions, logSettings);

            appendOnlyFile = new GarnetAppendOnlyFile(aofServerOptions, logSettings, Program.loggerFactory.CreateLogger("AofGen - AOF instance"));

            if (options.AofBenchType == AofBenchType.Replay)
            {
                pageBuffers = new Page[options.AofSublogCount][];
            }
            else
            {
                kvPairBuffers = new List<(byte[], byte[])>[options.NumThreads.Max()];
            }

            if (options.AofSublogCount != options.NumThreads.Max() && options.AofBenchType == AofBenchType.EnqueueSharded)
                throw new Exception("Use --threads(MAX)== --aof-sublog-count to generated perfectly sharded data!");
        }

        public NetworkBufferSettings GetAofSyncNetworkBufferSettings()
        {
            var aofSyncSendBufferSize = 2 << aofServerOptions.AofPageSizeBits();
            var aofSyncInitialReceiveBufferSize = 1 << 17;
            return new(aofSyncSendBufferSize, aofSyncInitialReceiveBufferSize);
        }

        byte[] GetKey() => Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.KeyLength, 8)));

        byte[] GetKey(int threadId)
        {
            while (true)
            {
                var keyData = Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.KeyLength, 8)));
                garnetLog.HashKey(keyData.AsSpan(), out _, out var sublogIdx, out _);
                if (sublogIdx == threadId) return keyData;
            }
        }

        byte[] GetValue() => Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.ValueLength, 8)));

        List<(byte[], byte[])> GenerateKVPairs(int threadId, bool random)
        {
            var kvPairs = new List<(byte[], byte[])>();

            for (var i = 0; i < options.DbSize; i++)
            {
                var key = random ? GetKey() : GetKey(threadId);
                var value = GetValue();
                kvPairs.Add((key, value));
            }
            return kvPairs;
        }

        public unsafe void GenerateData()
        {
            var seqNumGen = new SequenceNumberGenerator(0);
            Console.WriteLine($"Generating AoFBench Data!");
            var threads = options.AofBenchType == AofBenchType.Replay ? options.AofSublogCount : options.NumThreads.Max();
            var workers = new Thread[threads];

            // Run the experiment.
            for (var idx = 0; idx < threads; ++idx)
            {
                var x = idx;
                workers[idx] = options.AofBenchType switch
                {
                    AofBenchType.Replay => new Thread(() => GeneratePages(x)),
                    AofBenchType.EnqueueSharded or AofBenchType.EnqueueRandom => new Thread(() => GenerateKeys(x)),
                    _ => throw new Exception($"AofBenchType {options.AofBenchType} not supported"),
                };
            }

            Stopwatch swatch = new();
            swatch.Start();

            // Start threads.
            foreach (var worker in workers)
                worker.Start();

            // Wait for workers to complete
            foreach (var worker in workers)
                worker.Join();

            swatch.Stop();

            var seconds = swatch.ElapsedMilliseconds / 1000.0;
            if (options.AofBenchType == AofBenchType.Replay)
            {
                Console.WriteLine($"Generated {threads}x{options.DbSize} pages of size {aofServerOptions.AofPageSize} in {seconds:N2} secs");
                Console.WriteLine($"Generated number of AOF records: {total_number_of_aof_records:N0}");
                Console.WriteLine($"Generated number of AOF bytes: {total_number_of_aof_bytes:N0}");
            }
            else
            {
                Console.WriteLine($"Generated {threads}x{options.DbSize} KV pairs in {seconds:N2} secs");
            }

            void GeneratePages(int threadId)
            {
                var number_of_aof_records = 0L;
                var number_of_aof_bytes = 0L;
                var kvPairs = GenerateKVPairs(threadId, options.AofSublogCount == 1);
                //Console.WriteLine($"[{threadId}] {string.Join(',', kvPairs.Select(x => Encoding.ASCII.GetString(x.Item1) + "=" + Encoding.ASCII.GetString(x.Item2)))}");
                var pages = options.DbSize;
                pageBuffers[threadId] = new Page[pages];
                for (var i = 0; i < pages; i++)
                {
                    pageBuffers[threadId][i] = new Page(1 << aofServerOptions.AofPageSizeBits());
                    FillPage(threadId, kvPairs, i, pageBuffers[threadId][i]);
                }

                //Console.WriteLine($"[{threadId}] - Generated {number_of_aof_records:N0} AOF records, {number_of_aof_bytes:N0} AOF bytes");
                _ = Interlocked.Add(ref total_number_of_aof_records, number_of_aof_records);
                _ = Interlocked.Add(ref total_number_of_aof_bytes, number_of_aof_bytes);

                void FillPage(int threadId, List<(byte[], byte[])> kvPairs, int pageCount, Page page)
                {
                    fixed (byte* pagePtr = page.payload)
                    {
                        var pageOffset = pagePtr;
                        // First page starts from 64 address, so the payload space must be smaller
                        var pageEnd = pageOffset + page.Length - (pageCount == 0 ? 64 : 0);
                        var kvOffset = 0;
                        while (true)
                        {
                            var kvPair = kvPairs[kvOffset++ % kvPairs.Count];
                            var keyData = kvPair.Item1;
                            var valueData = kvPair.Item2;
                            RawStringInput input = default;
                            fixed (byte* keyPtr = keyData)
                            fixed (byte* valuePtr = valueData)
                            {
                                var key = SpanByte.FromPinnedPointer(keyPtr, keyData.Length);
                                var value = SpanByte.FromPinnedPointer(valuePtr, valueData.Length);
                                var aofHeader = new AofHeader { opType = AofEntryType.StoreUpsert, storeVersion = 1, sessionID = 0 };
                                if (options.AofSublogCount == 1)
                                {
                                    if (!garnetLog.GetSubLog(threadId).DummyEnqueue(
                                        ref pageOffset,
                                        pageEnd,
                                        aofHeader,
                                        key,
                                        value,
                                        ref input))
                                        break;
                                }
                                else
                                {
                                    var extendedAofHeader = new AofShardedHeader
                                    {
                                        basicHeader = new AofHeader
                                        {
                                            opType = aofHeader.opType,
                                            storeVersion = aofHeader.storeVersion,
                                            sessionID = aofHeader.sessionID
                                        },
                                        sequenceNumber = seqNumGen.GetSequenceNumber()
                                    };

                                    if (!garnetLog.GetSubLog(threadId).DummyEnqueue(
                                        ref pageOffset,
                                        pageEnd,
                                        extendedAofHeader,
                                        key,
                                        value,
                                        ref input))
                                        break;
                                }
                                page.recordCount++;
                            }
                        }

                        var payloadLength = (int)(pageOffset - pagePtr);
                        page.payloadLength = payloadLength;
                        number_of_aof_records += page.recordCount;
                        number_of_aof_bytes += payloadLength;
                    }
                }
            }

            void GenerateKeys(int threadId)
            {
                kvPairBuffers[threadId] = GenerateKVPairs(threadId, options.AofBenchType == AofBenchType.EnqueueRandom);
                //Console.WriteLine($"[{threadId}] - Generated {kvPairBuffers[threadId].Count} KV pairs for {options.AofBenchType}");
            }
        }
    }
}