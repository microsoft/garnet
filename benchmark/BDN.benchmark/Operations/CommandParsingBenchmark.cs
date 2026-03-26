// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Garnet.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for RESP command parsing only (no storage operations).
    /// Calls ParseRespCommandBuffer directly to measure pure parsing throughput
    /// across all optimization tiers.
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class CommandParsingBenchmark : OperationsBase
    {
        // Tier 1a: SIMD Vector128 fast path (3-6 char commands with fixed arg counts)
        static ReadOnlySpan<byte> CMD_PING => "*1\r\n$4\r\nPING\r\n"u8;
        static ReadOnlySpan<byte> CMD_GET => "*2\r\n$3\r\nGET\r\n$1\r\na\r\n"u8;
        static ReadOnlySpan<byte> CMD_SET => "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n"u8;
        static ReadOnlySpan<byte> CMD_INCR => "*2\r\n$4\r\nINCR\r\n$1\r\ni\r\n"u8;
        static ReadOnlySpan<byte> CMD_EXISTS => "*2\r\n$6\r\nEXISTS\r\n$1\r\na\r\n"u8;

        // Tier 1b: Scalar ulong switch (variable-arg commands)
        static ReadOnlySpan<byte> CMD_SETEX => "*4\r\n$5\r\nSETEX\r\n$1\r\na\r\n$2\r\n60\r\n$1\r\nb\r\n"u8;
        static ReadOnlySpan<byte> CMD_EXPIRE => "*3\r\n$6\r\nEXPIRE\r\n$1\r\na\r\n$2\r\n60\r\n"u8;

        // Old Tier 2 (FastParseArrayCommand): near top of switch chains (short names, common first chars)
        static ReadOnlySpan<byte> CMD_HSET => "*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\nv\r\n"u8;
        static ReadOnlySpan<byte> CMD_LPUSH => "*3\r\n$5\r\nLPUSH\r\n$1\r\nl\r\n$1\r\nv\r\n"u8;
        static ReadOnlySpan<byte> CMD_ZADD => "*4\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n1\r\n$1\r\nm\r\n"u8;

        // Old Tier 2 (FastParseArrayCommand): deep in switch chains (long names, double-digit $ header)
        static ReadOnlySpan<byte> CMD_ZRANGEBYSCORE => "*4\r\n$13\r\nZRANGEBYSCORE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n"u8;
        static ReadOnlySpan<byte> CMD_ZREMRANGEBYSCORE => "*4\r\n$16\r\nZREMRANGEBYSCORE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n10\r\n"u8;
        static ReadOnlySpan<byte> CMD_HINCRBYFLOAT => "*4\r\n$12\r\nHINCRBYFLOAT\r\n$1\r\nh\r\n$1\r\nf\r\n$3\r\n1.5\r\n"u8;

        // Old Tier 3 (SlowParseCommand): sequential SequenceEqual scan
        static ReadOnlySpan<byte> CMD_SUBSCRIBE => "*2\r\n$9\r\nSUBSCRIBE\r\n$2\r\nch\r\n"u8;
        static ReadOnlySpan<byte> CMD_GEORADIUS => "*6\r\n$9\r\nGEORADIUS\r\n$1\r\ng\r\n$1\r\n0\r\n$1\r\n0\r\n$3\r\n100\r\n$2\r\nkm\r\n"u8;
        static ReadOnlySpan<byte> CMD_SETIFMATCH => "*4\r\n$10\r\nSETIFMATCH\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\n0\r\n"u8;

        // Pre-allocated buffers (pinned for pointer stability)
        byte[] bufPing, bufGet, bufSet, bufIncr, bufExists, bufSetex, bufExpire, bufHset, bufLpush, bufZadd, bufSubscribe;
        byte[] bufZrangebyscore, bufZremrangebyscore, bufHincrbyfloat, bufGeoradius, bufSetifmatch;

        public override void GlobalSetup()
        {
            base.GlobalSetup();

            // Pre-seed a key so GET/EXISTS don't return NOT_FOUND
            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n"u8);

            bufPing = GC.AllocateArray<byte>(CMD_PING.Length, pinned: true);
            CMD_PING.CopyTo(bufPing);
            bufGet = GC.AllocateArray<byte>(CMD_GET.Length, pinned: true);
            CMD_GET.CopyTo(bufGet);
            bufSet = GC.AllocateArray<byte>(CMD_SET.Length, pinned: true);
            CMD_SET.CopyTo(bufSet);
            bufIncr = GC.AllocateArray<byte>(CMD_INCR.Length, pinned: true);
            CMD_INCR.CopyTo(bufIncr);
            bufExists = GC.AllocateArray<byte>(CMD_EXISTS.Length, pinned: true);
            CMD_EXISTS.CopyTo(bufExists);
            bufSetex = GC.AllocateArray<byte>(CMD_SETEX.Length, pinned: true);
            CMD_SETEX.CopyTo(bufSetex);
            bufExpire = GC.AllocateArray<byte>(CMD_EXPIRE.Length, pinned: true);
            CMD_EXPIRE.CopyTo(bufExpire);
            bufHset = GC.AllocateArray<byte>(CMD_HSET.Length, pinned: true);
            CMD_HSET.CopyTo(bufHset);
            bufLpush = GC.AllocateArray<byte>(CMD_LPUSH.Length, pinned: true);
            CMD_LPUSH.CopyTo(bufLpush);
            bufZadd = GC.AllocateArray<byte>(CMD_ZADD.Length, pinned: true);
            CMD_ZADD.CopyTo(bufZadd);
            bufSubscribe = GC.AllocateArray<byte>(CMD_SUBSCRIBE.Length, pinned: true);
            CMD_SUBSCRIBE.CopyTo(bufSubscribe);
            bufZrangebyscore = GC.AllocateArray<byte>(CMD_ZRANGEBYSCORE.Length, pinned: true);
            CMD_ZRANGEBYSCORE.CopyTo(bufZrangebyscore);
            bufZremrangebyscore = GC.AllocateArray<byte>(CMD_ZREMRANGEBYSCORE.Length, pinned: true);
            CMD_ZREMRANGEBYSCORE.CopyTo(bufZremrangebyscore);
            bufHincrbyfloat = GC.AllocateArray<byte>(CMD_HINCRBYFLOAT.Length, pinned: true);
            CMD_HINCRBYFLOAT.CopyTo(bufHincrbyfloat);
            bufGeoradius = GC.AllocateArray<byte>(CMD_GEORADIUS.Length, pinned: true);
            CMD_GEORADIUS.CopyTo(bufGeoradius);
            bufSetifmatch = GC.AllocateArray<byte>(CMD_SETIFMATCH.Length, pinned: true);
            CMD_SETIFMATCH.CopyTo(bufSetifmatch);
        }

        // === Tier 1a: SIMD Vector128 fast path ===

        [Benchmark]
        public RespCommand ParsePING()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufPing);
            return result;
        }

        [Benchmark]
        public RespCommand ParseGET()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufGet);
            return result;
        }

        [Benchmark]
        public RespCommand ParseSET()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufSet);
            return result;
        }

        [Benchmark]
        public RespCommand ParseINCR()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufIncr);
            return result;
        }

        [Benchmark]
        public RespCommand ParseEXISTS()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufExists);
            return result;
        }

        // === Tier 1b: Scalar ulong switch ===

        [Benchmark]
        public RespCommand ParseSETEX()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufSetex);
            return result;
        }

        [Benchmark]
        public RespCommand ParseEXPIRE()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufExpire);
            return result;
        }

        // === Old Tier 2 (FastParseArrayCommand): near top of switch ===

        [Benchmark]
        public RespCommand ParseHSET()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufHset);
            return result;
        }

        [Benchmark]
        public RespCommand ParseLPUSH()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufLpush);
            return result;
        }

        [Benchmark]
        public RespCommand ParseZADD()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufZadd);
            return result;
        }

        // === Old Tier 2 (FastParseArrayCommand): deep in switch (long names) ===

        [Benchmark]
        public RespCommand ParseZRANGEBYSCORE()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufZrangebyscore);
            return result;
        }

        [Benchmark]
        public RespCommand ParseZREMRANGEBYSCORE()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufZremrangebyscore);
            return result;
        }

        [Benchmark]
        public RespCommand ParseHINCRBYFLOAT()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufHincrbyfloat);
            return result;
        }

        // === Old Tier 3 (SlowParseCommand): sequential SequenceEqual scan ===

        [Benchmark]
        public RespCommand ParseSUBSCRIBE()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufSubscribe);
            return result;
        }

        [Benchmark]
        public RespCommand ParseGEORADIUS()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufGeoradius);
            return result;
        }

        [Benchmark]
        public RespCommand ParseSETIFMATCH()
        {
            RespCommand result = default;
            for (int i = 0; i < batchSize; i++)
                result = session.ParseRespCommandBuffer(bufSetifmatch);
            return result;
        }
    }
}
