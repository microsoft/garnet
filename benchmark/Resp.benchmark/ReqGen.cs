// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using Garnet.common;

namespace Resp.benchmark
{
    /// <summary>
    /// Buffer of generated RESP requests
    /// </summary>
    public unsafe partial class ReqGen
    {
        const int MaxBatches = 1 << 16;

        static int bitfieldOpCount = 3;

        readonly byte[][] buffers;
        readonly List<List<string>> flatRequestBuffer;
        readonly int[] lens;
        readonly OpType opType;
        readonly bool randomGen, randomServe;
        public readonly int DbSize, NumBuffs;
        readonly bool zipf;
        readonly int Start;
        readonly byte[] valueBuffer;
        readonly int hllDstMergeKeyCount = 1 << 8;
        readonly double hllDstMergeKeyFraction = 0.001;
        readonly ZipfGenerator zipfg;
        readonly byte[] ascii_chars = System.Text.Encoding.ASCII.GetBytes("abcdefghijklmnopqrstvuwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        readonly byte[] ascii_digits = System.Text.Encoding.ASCII.GetBytes("0123456789");
        readonly bool verbose;
        readonly bool numericValue;
        readonly bool flatBufferClient;
        readonly int ttl;
        readonly int keyLen, valueLen;

        Random r;
        Random keyRandomGen;
        Random valueRandomGen;
        int BufferSize = 1 << 16;
        int keyIndex;
        int seqNo;

        public int BatchCount;

        public ReqGen(
            int Start,
            int DbSize,
            int NumOps,
            int BatchSize,
            OpType opType,
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
            NumBuffs = NumOps / BatchSize;
            if (NumBuffs > MaxBatches && verbose)
            {
                Console.WriteLine($"Restricting #buffers to {MaxBatches} instead of {NumBuffs}");
                NumBuffs = MaxBatches;
            }
            buffers = new byte[NumBuffs][];

            flatRequestBuffer = flatBufferClient ? new List<List<string>>() : null;
            lens = new int[NumBuffs];
            BatchCount = BatchSize;
            this.opType = opType;
            seqNo = 0;
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

            int _hllDstMergeKeyCount = (int)(hllDstMergeKeyFraction * DbSize);
            if (_hllDstMergeKeyCount != 0)
                hllDstMergeKeyCount = _hllDstMergeKeyCount;

            this.ttl = ttl;
        }

        public int GetBufferSize()
        {
            return BufferSize;
        }

        /// <summary>
        /// Get batch of requests to serve
        /// </summary>
        /// <param name="len"></param>
        /// <returns></returns>
        public byte[] GetRequest(out int len)
        {
            int offset;

            if (randomServe)
                offset = r.Next(NumBuffs);
            else
                offset = (Interlocked.Increment(ref seqNo) - 1) % NumBuffs;

            len = lens[offset];
            return buffers[offset];
        }

        public List<string> GetRequestArgs()
        {
            int offset;
            if (randomServe)
                offset = r.Next(flatRequestBuffer.Count);
            else
                offset = (Interlocked.Increment(ref seqNo) - 1) % flatRequestBuffer.Count;

            return flatRequestBuffer[offset];
        }

        private void InitializeRNG(int keySeed = -1, int valueSeed = -1)
        {
            keyIndex = Start;
            r = new Random(789110123);
            keyRandomGen = keySeed == -1 ? new Random(789110123) : new Random(keySeed);
            valueRandomGen = valueSeed == -1 ? new Random(789110123) : new Random(valueSeed);
        }

        private void ConvertToSERedisInput(OpType opType)
        {
            for (int i = 0; i < NumBuffs; i++)
            {
                switch (opType)
                {
                    case OpType.GET:
                    case OpType.SET:
                    case OpType.MSET:
                        var buffer = buffers[i];
                        flatRequestBuffer.Add(new List<string>());
                        ProcessArgs(i, buffer);
                        break;
                    default:
                        Console.WriteLine($"op {opType} not supported with SERedis! Skipping conversion to SERedis input!");
                        break;
                }
            }
        }

        private void ProcessArgs(int i, byte[] buffer)
        {
            fixed (byte* buf = buffer)
            {
                byte* ptr = buf;
                RespReadUtils.TryReadUnsignedArrayLength(out int count, ref ptr, buf + buffer.Length);
                RespReadUtils.TryReadStringWithLengthHeader(out var cmd, ref ptr, buf + buffer.Length);

                for (int j = 0; j < count - 1; j++)
                {
                    RespReadUtils.TryReadStringWithLengthHeader(out var arg, ref ptr, buf + buffer.Length);
                    flatRequestBuffer[i].Add(arg);
                }
            }
        }

        /// <summary>
        /// Response handler
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="bytesRead"></param>
        /// <param name="opType"></param>
        /// <returns></returns>
        public static (int, int) OnResponse(byte* buf, int bytesRead, int opType)
        {
            int count = 0;

            switch ((OpType)opType)
            {
                case OpType.MGET:
                case OpType.GET:
                case OpType.MYDICTGET:
                case OpType.SCRIPTGET:
                case OpType.SCRIPTRETKEY:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == '$') count++;
                    break;
                case OpType.DEL:
                case OpType.INCR:
                case OpType.DBSIZE:
                case OpType.PUBLISH:
                case OpType.SPUBLISH:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == ':') count++;
                    break;
                case OpType.MSET:
                case OpType.PING:
                case OpType.PFMERGE:
                case OpType.AUTH:
                case OpType.SET:
                case OpType.SCRIPTSET:
                case OpType.SETEX:
                case OpType.SETIFPM:
                case OpType.MYDICTSET:
                case OpType.READONLY:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == '+') count++;
                    break;
                case OpType.PFADD:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == ':') count++;
                    break;
                case OpType.MPFADD:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == ':') count++;
                    break;
                case OpType.PFCOUNT:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == ':') count++;
                    break;
                case OpType.ZADD:
                case OpType.ZREM:
                case OpType.ZADDREM:
                case OpType.ZCARD:
                case OpType.GEOADD:
                case OpType.GEOADDREM:
                case OpType.ZADDCARD:
                case OpType.SETBIT:
                case OpType.GETBIT:
                case OpType.BITCOUNT:
                case OpType.BITPOS:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == ':') count++;
                    break;
                case OpType.BITOP_AND:
                case OpType.BITOP_OR:
                case OpType.BITOP_XOR:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == ':') count++;
                    break;
                case OpType.BITOP_NOT:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == ':') count += (1 + 1);
                    break;
                case OpType.BITFIELD:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == '*') count += bitfieldOpCount;
                    break;
                case OpType.BITFIELD_GET:
                case OpType.BITFIELD_SET:
                case OpType.BITFIELD_INCR:
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == '*') count++;
                    break;
                case OpType.XADD:
                    // XADD returns a bulk string with the stream ID
                    for (int i = 0; i < bytesRead; i++)
                        if (buf[i] == '$') count++;
                    break;
                default:
                    break;
            }
            return (bytesRead, count);
        }

        private long LongRandom() => ((long)valueRandomGen.Next() << 32) | (long)valueRandomGen.Next();

        private ulong ULongRandom()
        {
            ulong lsb = (ulong)(valueRandomGen.Next());
            ulong msb = (ulong)(valueRandomGen.Next()) << 32;
            return (msb | lsb);
        }

        private long RandomIntBitRange(int bitCount, bool signed)
        {
            if (signed)
            {
                long value = LongRandom();
                value = (valueRandomGen.Next() & 0x1) == 0x1 ? -value : value;
                value >>= (64 - bitCount);
                return value;
            }
            else
            {
                ulong value = ULongRandom();
                value >>= (64 - bitCount);
                return (long)value;
            }
        }

        private void RandomString()
        {
            if (numericValue)
            {
                for (int i = 0; i < valueBuffer.Length; i++)
                {
                    if (i == 0)
                        valueBuffer[i] = ascii_digits[1 + valueRandomGen.Next(ascii_digits.Length - 1)];
                    else
                        valueBuffer[i] = ascii_digits[valueRandomGen.Next(ascii_digits.Length)];
                }
            }
            else
            {
                for (int i = 0; i < valueBuffer.Length; i++)
                    valueBuffer[i] = ascii_chars[valueRandomGen.Next(ascii_chars.Length)];
            }
        }
    }
}