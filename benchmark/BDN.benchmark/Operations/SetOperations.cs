// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for SetOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class SetOperations : OperationsBase
    {
        static ReadOnlySpan<byte> SADDREM => "*3\r\n$4\r\nSADD\r\n$4\r\nkey1\r\n$6\r\nmember\r\n*3\r\n$4\r\nSREM\r\n$4\r\nkey1\r\n$6\r\nmember\r\n"u8;
        static ReadOnlySpan<byte> SADDPOP_SINGLE => "*3\r\n$4\r\nSADD\r\n$4\r\nkey1\r\n$6\r\nmember\r\n*2\r\n$4\r\nSPOP\r\n$4\r\nkey1\r\n"u8;
        static ReadOnlySpan<byte> SCARD => "*2\r\n$5\r\nSCARD\r\n$4\r\nkey2\r\n"u8;
        static ReadOnlySpan<byte> SMEMBERS => "*2\r\n$8\r\nSMEMBERS\r\n$4\r\nkey2\r\n"u8;
        static ReadOnlySpan<byte> SMOVE_TWICE => "*4\r\n$5\r\nSMOVE\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n$1\r\na\r\n*4\r\n$5\r\nSMOVE\r\n$4\r\nkey3\r\n$4\r\nkey2\r\n$1\r\na\r\n"u8;
        static ReadOnlySpan<byte> SISMEMBER => "*3\r\n$9\r\nSISMEMBER\r\n$4\r\nkey2\r\n$1\r\na\r\n"u8;
        static ReadOnlySpan<byte> SMISMEMBER => "*4\r\n$10\r\nSMISMEMBER\r\n$4\r\nkey2\r\n$1\r\na\r\n$1\r\nb\r\n"u8;
        static ReadOnlySpan<byte> SRANDMEMBER_SINGLE => "*2\r\n$11\r\nSRANDMEMBER\r\n$4\r\nkey3\r\n"u8;
        static ReadOnlySpan<byte> SSCAN => "*3\r\n$5\r\nSSCAN\r\n$4\r\nkey3\r\n$1\r\n0\r\n"u8;
        static ReadOnlySpan<byte> SUNION => "*3\r\n$6\r\nSUNION\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n"u8;
        static ReadOnlySpan<byte> SUNIONSTORE => "*4\r\n$11\r\nSUNIONSTORE\r\n$4\r\ndest\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n"u8;
        static ReadOnlySpan<byte> SINTER => "*3\r\n$6\r\nSINTER\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n"u8;
        static ReadOnlySpan<byte> SINTERSTORE => "*4\r\n$11\r\nSINTERSTORE\r\n$4\r\ndest\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n"u8;
        static ReadOnlySpan<byte> SINTERCARD => "*4\r\n$10\r\nSINTERCARD\r\n$1\r\n2\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n"u8;
        static ReadOnlySpan<byte> SDIFF => "*3\r\n$5\r\nSDIFF\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n"u8;
        static ReadOnlySpan<byte> SDIFFSTORE => "*4\r\n$10\r\nSDIFFSTORE\r\n$4\r\ndest\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n"u8;
        Request sAddRem, sAddPopSingle, sCard, sMembers, sMoveTwice, sIsMember, sMIsMember, sRandMemberSingle, sScan, sUnion, sUnionStore,
            sInter, sInterStore, sInterCard, sDiff, sDiffStore;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            SetupOperation(ref sAddRem, SADDREM);
            SetupOperation(ref sAddPopSingle, SADDPOP_SINGLE);
            SetupOperation(ref sCard, SCARD);
            SetupOperation(ref sMembers, SMEMBERS);
            SetupOperation(ref sMoveTwice, SMOVE_TWICE);
            SetupOperation(ref sIsMember, SISMEMBER);
            SetupOperation(ref sMIsMember, SMISMEMBER);
            SetupOperation(ref sRandMemberSingle, SRANDMEMBER_SINGLE);
            SetupOperation(ref sScan, SSCAN);
            SetupOperation(ref sUnion, SUNION);
            SetupOperation(ref sUnionStore, SUNIONSTORE);
            SetupOperation(ref sInter, SINTER);
            SetupOperation(ref sInterStore, SINTERSTORE);
            SetupOperation(ref sInterCard, SINTERCARD);
            SetupOperation(ref sDiff, SDIFF);
            SetupOperation(ref sDiffStore, SDIFFSTORE);

            // Pre-populate data
            SlowConsumeMessage("*4\r\n$4\r\nSADD\r\n$4\r\nkey2\r\n$1\r\na\r\n$1\r\nb\r\n"u8);
            SlowConsumeMessage("*5\r\n$4\r\nSADD\r\n$4\r\nkey3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n"u8);
        }

        [Benchmark]
        public void SAddRem() => Send(sAddRem);

        [Benchmark]
        public void SAddPopSingle() => Send(sAddPopSingle);

        [Benchmark]
        public void SCard() => Send(sCard);

        [Benchmark]
        public void SMembers() => Send(sMembers);

        [Benchmark]
        public void SMoveTwice() => Send(sMoveTwice);

        [Benchmark]
        public void SIsMember() => Send(sIsMember);

        [Benchmark]
        public void SMIsMember() => Send(sMIsMember);

        [Benchmark]
        public void SRandMemberSingle() => Send(sRandMemberSingle);

        [Benchmark]
        public void SScan() => Send(sScan);

        [Benchmark]
        public void SUnion() => Send(sUnion);

        [Benchmark]
        public void SUnionStore() => Send(sUnionStore);

        [Benchmark]
        public void SInter() => Send(sInter);

        [Benchmark]
        public void SInterStore() => Send(sInterStore);

        [Benchmark]
        public void SInterCard() => Send(sInterCard);

        [Benchmark]
        public void SDiff() => Send(sDiff);

        [Benchmark]
        public void SDiffStore() => Send(sDiffStore);
    }
}