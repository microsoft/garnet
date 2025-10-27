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
        public int payloadLength;
    }

    public sealed class AofGen
    {
        public readonly TsavoriteLog log;
        readonly Options options;
        readonly GarnetServerOptions aofServerOptions;
        readonly StringBuilder stats;
        readonly int threadId;

        // threads x pageNum
        Page[][] pageBuffers;

        long total_number_of_aof_records = 0L;
        long total_number_of_aof_bytes = 0L;

        public Page[] GetPageBuffers(int threadIdx) => pageBuffers[threadId];

        public AofGen(int threadId, Options options, StringBuilder stats)
        {
            this.stats = stats;
            this.options = options;
            this.threadId = threadId;

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
                ReplicationOffsetMaxLag = 0
            };
            aofServerOptions.GetAofSettings(0, out var logSettings);
            log = new TsavoriteLog(logSettings);

            pageBuffers = new Page[options.NumThreads.First()][];
        }

        public NetworkBufferSettings GetAofSyncNetworkBufferSettings()
        {
            var aofSyncSendBufferSize = 2 << aofServerOptions.AofPageSizeBits();
            var aofSyncInitialReceiveBufferSize = 1 << 17;
            return new(aofSyncSendBufferSize, aofSyncInitialReceiveBufferSize);
        }

        byte[] GetKey() => Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.KeyLength, 8)));

        byte[] GetValue() => Encoding.ASCII.GetBytes(Generator.CreateHexId(size: Math.Max(options.ValueLength, 8)));

        List<(byte[], byte[])> GenerateKVPairs()
        {
            var kvPairs = new List<(byte[], byte[])>();

            for (var i = 0; i < options.DbSize; i++)
            {
                var key = GetKey();
                var value = GetValue();
                kvPairs.Add((key, value));
            }
            return kvPairs;
        }

        public unsafe void GenerateData()
        {
            var threads = 1;
            var workers = new Thread[threads];

            Console.WriteLine($"Start generating {threads}x{options.DbSize} pages of size {aofServerOptions.AofPageSize}");

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
                var kvPairs = GenerateKVPairs();
                Console.WriteLine($"[{threadId}] - Generating AOF data");
                var pages = options.DbSize;
                pageBuffers[threadId] = new Page[pages];
                for (var i = 0; i < pages; i++)
                {
                    pageBuffers[threadId][i] = new Page(1 << aofServerOptions.AofPageSizeBits());
                    FillPage(i, pageBuffers[threadId][i]);
                }

                void FillPage(int pageCount, Page page)
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
                                if (!log.DummyEnqueue(
                                    ref pageOffset,
                                    pageEnd,
                                    new AofHeader { opType = AofEntryType.StoreUpsert, storeVersion = 1, sessionID = 0 },
                                    ref key,
                                    ref value,
                                    ref input))
                                    break;
                            }
                        }

                        var payloadLength = (int)(pageOffset - pagePtr);
                        page.payloadLength = payloadLength;

                        _ = Interlocked.Add(ref total_number_of_aof_records, kvOffset);
                        _ = Interlocked.Add(ref total_number_of_aof_bytes, payloadLength);
                    }
                }
            }
        }
    }
}