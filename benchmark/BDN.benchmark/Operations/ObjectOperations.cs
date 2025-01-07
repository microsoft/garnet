// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for ObjectOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class ObjectOperations : OperationsBase
    {
        static ReadOnlySpan<byte> ZADDREM => "*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nc\r\n*3\r\n$4\r\nZREM\r\n$1\r\nc\r\n$1\r\nc\r\n"u8;
        Request zAddRem;

        static ReadOnlySpan<byte> LPUSHPOP => "*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\ne\r\n*2\r\n$4\r\nLPOP\r\n$1\r\nd\r\n"u8;
        Request lPushPop;

        static ReadOnlySpan<byte> SADDREM => "*3\r\n$4\r\nSADD\r\n$1\r\ne\r\n$1\r\na\r\n*3\r\n$4\r\nSREM\r\n$1\r\ne\r\n$1\r\na\r\n"u8;
        Request sAddRem;

        public override void GlobalSetup()
        {
            base.GlobalSetup();

            SetupOperation(ref zAddRem, ZADDREM);
            SetupOperation(ref lPushPop, LPUSHPOP);
            SetupOperation(ref sAddRem, SADDREM);

            // Pre-populate data
            SlowConsumeMessage("*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8);
            SlowConsumeMessage("*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\nf\r\n"u8);
            SlowConsumeMessage("*3\r\n$4\r\nSADD\r\n$1\r\ne\r\n$1\r\nb\r\n"u8);
        }

        [Benchmark]
        public void ZAddRem()
        {
            Send(zAddRem);
        }

        [Benchmark]
        public void LPushPop()
        {
            Send(lPushPop);
        }

        [Benchmark]
        public void SAddRem()
        {
            Send(sAddRem);
        }
    }
}