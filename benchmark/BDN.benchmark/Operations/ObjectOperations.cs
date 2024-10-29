// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for ObjectOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class ObjectOperations : OperationsBase
    {
        static ReadOnlySpan<byte> ZADDREM => "*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nc\r\n*3\r\n$4\r\nZREM\r\n$1\r\nc\r\n$1\r\nc\r\n"u8;
        byte[] zAddRemRequestBuffer;
        byte* zAddRemRequestBufferPointer;

        static ReadOnlySpan<byte> LPUSHPOP => "*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\ne\r\n*2\r\n$4\r\nLPOP\r\n$1\r\nd\r\n"u8;
        byte[] lPushPopRequestBuffer;
        byte* lPushPopRequestBufferPointer;

        static ReadOnlySpan<byte> SADDREM => "*3\r\n$4\r\nSADD\r\n$1\r\ne\r\n$1\r\na\r\n*3\r\n$4\r\nSREM\r\n$1\r\ne\r\n$1\r\na\r\n"u8;
        byte[] sAddRemRequestBuffer;
        byte* sAddRemRequestBufferPointer;

        static ReadOnlySpan<byte> HSETDEL => "*4\r\n$4\r\nHSET\r\n$1\r\nf\r\n$1\r\na\r\n$1\r\na\r\n*3\r\n$4\r\nHDEL\r\n$1\r\nf\r\n$1\r\na\r\n"u8;
        byte[] hSetDelRequestBuffer;
        byte* hSetDelRequestBufferPointer;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            SetupOperation(ref zAddRemRequestBuffer, ref zAddRemRequestBufferPointer, ZADDREM);
            SetupOperation(ref lPushPopRequestBuffer, ref lPushPopRequestBufferPointer, LPUSHPOP);
            SetupOperation(ref sAddRemRequestBuffer, ref sAddRemRequestBufferPointer, SADDREM);
            SetupOperation(ref hSetDelRequestBuffer, ref hSetDelRequestBufferPointer, HSETDEL);

            // Pre-populate data
            SlowConsumeMessage("*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8);
            SlowConsumeMessage("*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\nf\r\n"u8);
            SlowConsumeMessage("*3\r\n$4\r\nSADD\r\n$1\r\ne\r\n$1\r\nb\r\n"u8);
            SlowConsumeMessage("*3\r\n$4\r\nHSET\r\n$1\r\nf\r\n$1\r\nb\r\n$1\r\nb\r\n"u8);
        }

        [Benchmark]
        public void ZAddRem()
        {
            _ = session.TryConsumeMessages(zAddRemRequestBufferPointer, zAddRemRequestBuffer.Length);
        }

        [Benchmark]
        public void LPushPop()
        {
            _ = session.TryConsumeMessages(lPushPopRequestBufferPointer, lPushPopRequestBuffer.Length);
        }

        [Benchmark]
        public void SAddRem()
        {
            _ = session.TryConsumeMessages(sAddRemRequestBufferPointer, sAddRemRequestBuffer.Length);
        }

        [Benchmark]
        public void HSetDel()
        {
            _ = session.TryConsumeMessages(hSetDelRequestBufferPointer, hSetDelRequestBuffer.Length);
        }
    }
}