// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for SortedSetOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class SortedSetOperations : OperationsBase
    {
        static ReadOnlySpan<byte> ZADDREM => "*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nc\r\n*3\r\n$4\r\nZREM\r\n$1\r\nc\r\n$1\r\nc\r\n"u8;
        static ReadOnlySpan<byte> ZCARD => "*2\r\n$5\r\nZCARD\r\n$1\r\nc\r\n"u8;
        static ReadOnlySpan<byte> ZCOUNT => "*4\r\n$6\r\nZCOUNT\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\n2\r\n"u8;
        static ReadOnlySpan<byte> ZDIFF => "*4\r\n$5\r\nZDIFF\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\nh\r\n"u8;
        static ReadOnlySpan<byte> ZDIFFSTORE => "*5\r\n$10\r\nZDIFFSTORE\r\n$4\r\ndest\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\nh\r\n"u8;
        static ReadOnlySpan<byte> ZINCRBY => "*4\r\n$7\r\nZINCRBY\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8;
        static ReadOnlySpan<byte> ZINTER => "*4\r\n$6\r\nZINTER\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\nh\r\n"u8;
        static ReadOnlySpan<byte> ZINTERCARD => "*4\r\n$10\r\nZINTERCARD\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\nh\r\n"u8;
        static ReadOnlySpan<byte> ZINTERSTORE => "*5\r\n$11\r\nZINTERSTORE\r\n$4\r\ndest\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\nh\r\n"u8;
        static ReadOnlySpan<byte> ZLEXCOUNT => "*4\r\n$9\r\nZLEXCOUNT\r\n$1\r\nc\r\n$1\r\n-\r\n$1\r\n+\r\n"u8;
        static ReadOnlySpan<byte> ZMPOP => "*4\r\n$5\r\nZMPOP\r\n$1\r\n1\r\n$1\r\nc\r\n$3\r\nMIN\r\n*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8;
        static ReadOnlySpan<byte> ZMSCORE => "*3\r\n$7\r\nZMSCORE\r\n$1\r\nc\r\n$1\r\nd\r\n"u8;
        static ReadOnlySpan<byte> ZPOPMAX => "*2\r\n$7\r\nZPOPMAX\r\n$1\r\nc\r\n*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8;
        static ReadOnlySpan<byte> ZPOPMIN => "*2\r\n$7\r\nZPOPMIN\r\n$1\r\nc\r\n*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8;
        static ReadOnlySpan<byte> ZRANDMEMBER => "*2\r\n$10\r\nZRANDMEMBER\r\n$1\r\nc\r\n"u8;
        static ReadOnlySpan<byte> ZRANGE => "*4\r\n$6\r\nZRANGE\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\n1\r\n"u8;
        static ReadOnlySpan<byte> ZRANGESTORE => "*5\r\n$11\r\nZRANGESTORE\r\n$4\r\ndest\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\n1\r\n"u8;
        static ReadOnlySpan<byte> ZRANK => "*3\r\n$5\r\nZRANK\r\n$1\r\nc\r\n$1\r\nd\r\n"u8;
        static ReadOnlySpan<byte> ZREMRANGEBYLEX => "*4\r\n$14\r\nZREMRANGEBYLEX\r\n$1\r\nc\r\n$1\r\n-\r\n$1\r\n+\r\n*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8;
        static ReadOnlySpan<byte> ZREMRANGEBYRANK => "*4\r\n$15\r\nZREMRANGEBYRANK\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\n1\r\n*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8;
        static ReadOnlySpan<byte> ZREMRANGEBYSCORE => "*4\r\n$16\r\nZREMRANGEBYSCORE\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\n2\r\n*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8;
        static ReadOnlySpan<byte> ZREVRANK => "*3\r\n$8\r\nZREVRANK\r\n$1\r\nc\r\n$1\r\nd\r\n"u8;
        static ReadOnlySpan<byte> ZSCAN => "*6\r\n$5\r\nZSCAN\r\n$1\r\nc\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n"u8;
        static ReadOnlySpan<byte> ZSCORE => "*3\r\n$6\r\nZSCORE\r\n$1\r\nc\r\n$1\r\nd\r\n"u8;
        static ReadOnlySpan<byte> ZUNION => "*4\r\n$6\r\nZUNION\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\nh\r\n"u8;
        static ReadOnlySpan<byte> ZUNIONSTORE => "*5\r\n$11\r\nZUNIONSTORE\r\n$4\r\ndest\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\nh\r\n"u8;

        Request zAddRem, zCard, zCount, zDiff, zDiffStore, zIncrby, zInter, zInterCard,
                zInterStore, zLexCount, zMPop, zMScore, zPopMax, zPopMin,
                zRandMember, zRange, zRangeStore, zRank, zRemRangeByLex,
                zRemRangeByRank, zRemRangeByScore, zRevRank, zScan,
                zScore, zUnion, zUnionStore;

        public override void GlobalSetup()
        {
            base.GlobalSetup();

            SetupOperation(ref zAddRem, ZADDREM);
            SetupOperation(ref zCard, ZCARD);
            SetupOperation(ref zCount, ZCOUNT);
            SetupOperation(ref zDiff, ZDIFF);
            SetupOperation(ref zDiffStore, ZDIFFSTORE);
            SetupOperation(ref zIncrby, ZINCRBY);
            SetupOperation(ref zInter, ZINTER);
            SetupOperation(ref zInterCard, ZINTERCARD);
            SetupOperation(ref zInterStore, ZINTERSTORE);
            SetupOperation(ref zLexCount, ZLEXCOUNT);
            SetupOperation(ref zMPop, ZMPOP);
            SetupOperation(ref zMScore, ZMSCORE);
            SetupOperation(ref zPopMax, ZPOPMAX);
            SetupOperation(ref zPopMin, ZPOPMIN);
            SetupOperation(ref zRandMember, ZRANDMEMBER);
            SetupOperation(ref zRange, ZRANGE);
            SetupOperation(ref zRangeStore, ZRANGESTORE);
            SetupOperation(ref zRank, ZRANK);
            SetupOperation(ref zRemRangeByLex, ZREMRANGEBYLEX);
            SetupOperation(ref zRemRangeByRank, ZREMRANGEBYRANK);
            SetupOperation(ref zRemRangeByScore, ZREMRANGEBYSCORE);
            SetupOperation(ref zRevRank, ZREVRANK);
            SetupOperation(ref zScan, ZSCAN);
            SetupOperation(ref zScore, ZSCORE);
            SetupOperation(ref zUnion, ZUNION);
            SetupOperation(ref zUnionStore, ZUNIONSTORE);

            // Pre-populate data for two sorted sets
            SlowConsumeMessage("*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8);
            SlowConsumeMessage("*6\r\n$4\r\nZADD\r\n$1\r\nh\r\n$1\r\n1\r\n$1\r\nd\r\n$1\r\n2\r\n$1\r\ne\r\n"u8);
            SlowConsumeMessage("*4\r\n$4\r\nZADD\r\n$4\r\ndest\r\n$1\r\n1\r\n$1\r\nd\r\n"u8);
        }

        [Benchmark]
        public void ZAddRem()
        {
            Send(zAddRem);
        }

        [Benchmark]
        public void ZCard() => Send(zCard);

        [Benchmark]
        public void ZCount() => Send(zCount);

        [Benchmark]
        public void ZDiff() => Send(zDiff);

        [Benchmark]
        public void ZDiffStore() => Send(zDiffStore);

        [Benchmark]
        public void ZIncrby() => Send(zIncrby);

        [Benchmark]
        public void ZInter() => Send(zInter);

        [Benchmark]
        public void ZInterCard() => Send(zInterCard);

        [Benchmark]
        public void ZInterStore() => Send(zInterStore);

        [Benchmark]
        public void ZLexCount() => Send(zLexCount);

        [Benchmark]
        public void ZMPop() => Send(zMPop);

        [Benchmark]
        public void ZMScore() => Send(zMScore);

        [Benchmark]
        public void ZPopMax() => Send(zPopMax);

        [Benchmark]
        public void ZPopMin() => Send(zPopMin);

        [Benchmark]
        public void ZRandMember() => Send(zRandMember);

        [Benchmark]
        public void ZRange() => Send(zRange);

        [Benchmark]
        public void ZRangeStore() => Send(zRangeStore);

        [Benchmark]
        public void ZRank() => Send(zRank);

        [Benchmark]
        public void ZRemRangeByLex() => Send(zRemRangeByLex);

        [Benchmark]
        public void ZRemRangeByRank() => Send(zRemRangeByRank);

        [Benchmark]
        public void ZRemRangeByScore() => Send(zRemRangeByScore);

        [Benchmark]
        public void ZRevRank() => Send(zRevRank);

        [Benchmark]
        public void ZScan() => Send(zScan);

        [Benchmark]
        public void ZScore() => Send(zScore);

        [Benchmark]
        public void ZUnion() => Send(zUnion);

        [Benchmark]
        public void ZUnionStore() => Send(zUnionStore);
    }
}