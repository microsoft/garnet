// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;
using System.Text;
using System.Diagnostics;

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
        readonly Options options;
        readonly GarnetServerOptions aofServerOptions;
        readonly StringBuilder stats;

        // threads x pageNum
        Page[][] pageBuffers;

        long total_number_of_aof_records = 0L;
        long total_number_of_aof_bytes = 0L;

        public Page[] GetPageBuffers(int threadIdx) => pageBuffers[threadIdx];

        public AofGen(Options options, StringBuilder stats)
        {
            this.stats = stats;
            this.options = options;

            this.aofServerOptions = new GarnetServerOptions()
            {
                AofMemorySize = options.CalculateAofMemorySizeForLoad(),
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
            pageBuffers = new Page[options.AofSublogCount][];
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
                garnetLog.Hash(keyData.AsSpan(), out _, out var sublogIdx, out _);
                if (sublogIdx == threadId) return keyData;
            }
        }

        byte[] GetValue() => Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.ValueLength, 8)));

        List<(byte[], byte[])> GenerateKVPairs(int threadId)
        {
            var kvPairs = new List<(byte[], byte[])>();

            for (var i = 0; i < options.DbSize; i++)
            {
                var key = options.AofSublogCount == 1 ? GetKey() : GetKey(threadId);
                var value = GetValue();
                kvPairs.Add((key, value));
            }
            return kvPairs;
        }

        public unsafe void GenerateData()
        {
            var threads = options.AofSublogCount;
            var workers = new Thread[threads];

            Console.WriteLine($"Generating {threads}x{options.DbSize} pages of size {aofServerOptions.AofPageSize}");

            // Run the experiment.
            for (var idx = 0; idx < threads; ++idx)
            {
                var x = idx;
                workers[idx] = new Thread(() => GenerateData(x));
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
            Console.WriteLine($"Generated {threads}x{options.DbSize} pages of size {aofServerOptions.AofPageSize} in {seconds:N2} secs");
            Console.WriteLine($"Total number of AOF records: {total_number_of_aof_records:N0}");
            Console.WriteLine($"Total number of AOF bytes: {total_number_of_aof_bytes:N0}");

            void GenerateData(int threadId)
            {
                var number_of_aof_records = 0L;
                var number_of_aof_bytes = 0L;
                var kvPairs = GenerateKVPairs(threadId);
                var pages = options.DbSize;
                pageBuffers[threadId] = new Page[pages];
                for (var i = 0; i < pages; i++)
                {
                    pageBuffers[threadId][i] = new Page(1 << aofServerOptions.AofPageSizeBits());
                    FillPage(threadId, i, pageBuffers[threadId][i]);
                }

                Console.WriteLine($"[{threadId}] - Generated {number_of_aof_records:N0} AOF records, {number_of_aof_bytes:N0} AOF bytes");
                _ = Interlocked.Add(ref total_number_of_aof_records, number_of_aof_records);
                _ = Interlocked.Add(ref total_number_of_aof_bytes, number_of_aof_bytes);

                void FillPage(int threadId, int pageCount, Page page)
                {
                    fixed (byte* pagePtr = page.payload)
                    {
                        var pageOffset = pagePtr;
                        // First page starts from 64 address, so the payload space must be smaller
                        var pageEnd = pageOffset + page.Length - (pageCount == 0 ? 64 : 0);
                        var kvOffset = 0;
                        while (true)
                        {
                            var kvPair = kvPairs[kvOffset++ % kvPairs.Count()];
                            var keyData = kvPair.Item1;
                            var valueData = kvPair.Item2;
                            RawStringInput input = default;
                            fixed (byte* keyPtr = GetKey())
                            fixed (byte* valuePtr = GetValue())
                            {
                                var key = SpanByte.FromPinnedPointer(keyPtr, keyData.Length);
                                var value = SpanByte.FromPinnedPointer(valuePtr, valueData.Length);
                                if (!garnetLog.GetSubLog(threadId).DummyEnqueue(
                                    ref pageOffset,
                                    pageEnd,
                                    new AofHeader { opType = AofEntryType.StoreUpsert, storeVersion = 1, sessionID = 0 },
                                    ref key,
                                    ref value,
                                    ref input))
                                    break;
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
        }
    }
}