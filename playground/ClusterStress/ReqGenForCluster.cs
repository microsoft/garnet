// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using StackExchange.Redis;

namespace Resp.benchmark
{
    public unsafe partial class ReqGen
    {
        readonly List<int>[] databaseKeys;
        readonly int[] interleavedSlots = new int[16384];
        readonly int shard;
        readonly ClusterConfiguration clusterConfig;
        int currSlot = 0;
        int currKeyInSlot = 0;

        public ReqGen(
          int Start,
          int DbSize,
          int NumOps,
          int BatchSize,
          OpType opType,
          ClusterConfiguration clusterConfig,
          int shard = -1,
          bool randomGen = true,
          bool randomServe = true,
          int keyLen = default,
          int valueLen = default,
          bool numericValue = false,
          bool verbose = true,
          bool zipf = false,
          bool flatBufferClient = false,
          int ttl = 0)
        {
            this.shard = shard;
            this.clusterConfig = clusterConfig;

            databaseKeys = new List<int>[16384];

            for (int i = 0; i < databaseKeys.Length; i++)
                databaseKeys[i] = new List<int>();

            this.NumBuffs = NumOps / BatchSize;
            if (NumBuffs > MaxBatches && verbose)
            {
                Console.WriteLine($"Restricting #buffers to {MaxBatches} instead of {NumBuffs}");
                NumBuffs = MaxBatches;
            }
            this.buffers = new byte[NumBuffs][];

            this.flatRequestBuffer = flatBufferClient ? new List<List<string>>() : null;
            this.lens = new int[NumBuffs];
            this.BatchCount = BatchSize;
            this.opType = opType;
            this.seqNo = 0;
            this.randomGen = randomGen;
            this.randomServe = randomServe;
            this.DbSize = DbSize;
            this.Start = Start;
            this.flatBufferClient = flatBufferClient;
            this.ttl = ttl;

            if (zipf)
            {
                this.zipf = zipf;
                zipfg = new ZipfGenerator(new RandomGenerator(), DbSize, 0.99);
            }

            this.keyLen = keyLen == default ? NumUtils.NumDigits(DbSize) : keyLen;
            this.valueLen = valueLen == default ? 8 : valueLen;
            valueBuffer = new byte[this.valueLen];

            this.numericValue = numericValue;
            this.verbose = verbose;

            int _hllDstMergeKeyCount = (int)(((double)(DbSize)) * this.hllDstMergeKeyFraction);
            this.hllDstMergeKeyCount = hllDstMergeKeyCount == 0 ? this.hllDstMergeKeyCount : _hllDstMergeKeyCount;

            this.ttl = ttl;
        }

        public void GenerateForCluster()
        {
            if (verbose)
            {
                Console.WriteLine();
                Console.WriteLine($"Generating {NumBuffs} {opType} request batches of size {BatchCount} each; total {NumBuffs * BatchCount} ops");

                if (opType == OpType.PFMERGE)
                {
                    Console.WriteLine("PFMERGE config > mergeDstKeyCount:{0}, hllDstMergeKeyFraction:{1}", hllDstMergeKeyCount, hllDstMergeKeyFraction);
                }
            }

            // Prepare the cluster sharded keys and slots
            var elapsed = Stopwatch.StartNew();
            GenerateShardedKeys();
            InitInterleaveSlots();
            elapsed.Stop();
            if (verbose)
            {
                Console.WriteLine("Generate keys time: {0} secs", elapsed.ElapsedMilliseconds / 1000.0);
            }

            var sw = Stopwatch.StartNew();
            int maxBytesWritten = 0;
            while (true)
            {
                InitializeRNG();
                for (int i = 0; i < NumBuffs; i++)
                {
                    buffers[i] = new byte[BufferSize];
                    lens[i] = 0;

                    //Reset counters to point to buffer for slot
                    currSlot = i;
                    currKeyInSlot = 0;

                    switch (opType)
                    {
                        case OpType.ZADDREM:
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, 0, BatchCount / 2, OpType.ZADD)) goto resizeBuffer;
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, BatchCount / 2, BatchCount, OpType.ZREM)) goto resizeBuffer;
                            break;
                        case OpType.GEOADDREM:
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, 0, BatchCount / 2, OpType.GEOADD)) goto resizeBuffer;
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, BatchCount / 2, BatchCount, OpType.ZREM)) goto resizeBuffer;
                            break;
                        case OpType.ZADDCARD:
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, 0, BatchCount / 2, OpType.ZADD)) goto resizeBuffer;
                            InitializeRNG(keySeed: i);
                            if (!GenerateBatch(i, BatchCount / 2, BatchCount, OpType.ZCARD)) goto resizeBuffer;
                            break;
                        default:
                            if (!GenerateBatch(i, 0, BatchCount, opType)) goto resizeBuffer;
                            break;
                    }
                    maxBytesWritten = Math.Max(lens[i], maxBytesWritten);
                }
                break;

            resizeBuffer:
                if (verbose)
                {
                    Console.Write("Resizing request buffer from {0}", BufferSize);
                    BufferSize = BufferSize << 1;
                    Console.WriteLine(" to {0}", BufferSize);
                }
            }
            sw.Stop();
            if (verbose)
            {
                Console.WriteLine("Request generation complete");
                Console.WriteLine("maxBytesWritten out of maxBufferSize: {0}/{1}", maxBytesWritten, BufferSize);
                Console.WriteLine("Loading time: {0} secs", sw.ElapsedMilliseconds / 1000.0);
            }

            if (flatBufferClient)
            {
                ConvertToSERedisInput(opType);
                if (flatRequestBuffer.Count > 0)
                {
                    for (int i = 0; i < NumBuffs; i++)
                        buffers[i] = null;
                }
            }

        }

        public byte[] GetRequestInterleaved(ref Random r, out int len, out int slot)
        {
            int offset;
            if (randomServe)
                offset = r.Next(NumBuffs);
            else
                offset = (Interlocked.Increment(ref seqNo) - 1) % NumBuffs;

            slot = interleavedSlots[offset & 16383];
            len = lens[offset];
            return buffers[offset];
        }

        private byte[] GetClusterKeyInterleaved()
        {
            //currSlot is buffer index, interleaved slots array for all slots to assign all
            int slot = interleavedSlots[currSlot & 16383];
            int keyPrefixCount = databaseKeys[slot].Count;
            int key = databaseKeys[slot][currKeyInSlot & (keyPrefixCount - 1)];

            string keyStr = "{" + key.ToString() + "}";
            currKeyInSlot++;
            return Encoding.ASCII.GetBytes(keyStr.PadRight(keyLen, numericValue ? '1' : 'X'));
        }

    }
}