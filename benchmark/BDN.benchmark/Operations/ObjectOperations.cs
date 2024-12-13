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

        static ReadOnlySpan<byte> HEXISTS => "*3\r\n$7\r\nHEXISTS\r\n$1\r\nf\r\n$1\r\na\r\n"u8;
        byte[] hExistsRequestBuffer;
        byte* hExistsRequestBufferPointer;

        static ReadOnlySpan<byte> HGET => "*3\r\n$4\r\nHGET\r\n$1\r\nf\r\n$1\r\na\r\n"u8;
        byte[] hGetRequestBuffer;
        byte* hGetRequestBufferPointer;

        static ReadOnlySpan<byte> HGETALL => "*2\r\n$7\r\nHGETALL\r\n$1\r\nf\r\n"u8;
        byte[] hGetAllRequestBuffer;
        byte* hGetAllRequestBufferPointer;

        static ReadOnlySpan<byte> HINCRBY => "*4\r\n$7\r\nHINCRBY\r\n$1\r\nf\r\n$1\r\nc\r\n$1\r\n1\r\n"u8;
        byte[] hIncrbyRequestBuffer;
        byte* hIncrbyRequestBufferPointer;

        static ReadOnlySpan<byte> HINCRBYFLOAT => "*4\r\n$12\r\nHINCRBYFLOAT\r\n$1\r\nf\r\n$1\r\nd\r\n$3\r\n1.5\r\n"u8;
        byte[] hIncrbyFloatRequestBuffer;
        byte* hIncrbyFloatRequestBufferPointer;

        static ReadOnlySpan<byte> HKEYS => "*2\r\n$5\r\nHKEYS\r\n$1\r\nf\r\n"u8;
        byte[] hKeysRequestBuffer;
        byte* hKeysRequestBufferPointer;

        static ReadOnlySpan<byte> HLEN => "*2\r\n$4\r\nHLEN\r\n$1\r\nf\r\n"u8;
        byte[] hLenRequestBuffer;
        byte* hLenRequestBufferPointer;

        static ReadOnlySpan<byte> HMGET => "*4\r\n$5\r\nHMGET\r\n$1\r\nf\r\n$1\r\na\r\n$1\r\nb\r\n"u8;
        byte[] hMGetRequestBuffer;
        byte* hMGetRequestBufferPointer;

        static ReadOnlySpan<byte> HMSET => "*6\r\n$5\r\nHMSET\r\n$1\r\nf\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"u8;
        byte[] hMSetRequestBuffer;
        byte* hMSetRequestBufferPointer;

        static ReadOnlySpan<byte> HRANDFIELD => "*2\r\n$10\r\nHRANDFIELD\r\n$1\r\nf\r\n"u8;
        byte[] hRandFieldRequestBuffer;
        byte* hRandFieldRequestBufferPointer;

        static ReadOnlySpan<byte> HSCAN => "*6\r\n$5\r\nHSCAN\r\n$1\r\nf\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n"u8;
        byte[] hScanRequestBuffer;
        byte* hScanRequestBufferPointer;

        static ReadOnlySpan<byte> HSETNX => "*4\r\n$6\r\nHSETNX\r\n$1\r\nf\r\n$1\r\nx\r\n$1\r\n1\r\n"u8;
        byte[] hSetNxRequestBuffer;
        byte* hSetNxRequestBufferPointer;

        static ReadOnlySpan<byte> HSTRLEN => "*3\r\n$7\r\nHSTRLEN\r\n$1\r\nf\r\n$1\r\na\r\n"u8;
        byte[] hStrLenRequestBuffer;
        byte* hStrLenRequestBufferPointer;

        static ReadOnlySpan<byte> HVALS => "*2\r\n$5\r\nHVALS\r\n$1\r\nf\r\n"u8;
        byte[] hValsRequestBuffer;
        byte* hValsRequestBufferPointer;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            SetupOperation(ref zAddRemRequestBuffer, ref zAddRemRequestBufferPointer, ZADDREM);
            SetupOperation(ref lPushPopRequestBuffer, ref lPushPopRequestBufferPointer, LPUSHPOP);
            SetupOperation(ref sAddRemRequestBuffer, ref sAddRemRequestBufferPointer, SADDREM);
            SetupOperation(ref hSetDelRequestBuffer, ref hSetDelRequestBufferPointer, HSETDEL);
            SetupOperation(ref hExistsRequestBuffer, ref hExistsRequestBufferPointer, HEXISTS);
            SetupOperation(ref hGetRequestBuffer, ref hGetRequestBufferPointer, HGET);
            SetupOperation(ref hGetAllRequestBuffer, ref hGetAllRequestBufferPointer, HGETALL);
            SetupOperation(ref hIncrbyRequestBuffer, ref hIncrbyRequestBufferPointer, HINCRBY);
            SetupOperation(ref hIncrbyFloatRequestBuffer, ref hIncrbyFloatRequestBufferPointer, HINCRBYFLOAT);
            SetupOperation(ref hKeysRequestBuffer, ref hKeysRequestBufferPointer, HKEYS);
            SetupOperation(ref hLenRequestBuffer, ref hLenRequestBufferPointer, HLEN);
            SetupOperation(ref hMGetRequestBuffer, ref hMGetRequestBufferPointer, HMGET);
            SetupOperation(ref hMSetRequestBuffer, ref hMSetRequestBufferPointer, HMSET);
            SetupOperation(ref hRandFieldRequestBuffer, ref hRandFieldRequestBufferPointer, HRANDFIELD);
            SetupOperation(ref hScanRequestBuffer, ref hScanRequestBufferPointer, HSCAN);
            SetupOperation(ref hSetNxRequestBuffer, ref hSetNxRequestBufferPointer, HSETNX);
            SetupOperation(ref hStrLenRequestBuffer, ref hStrLenRequestBufferPointer, HSTRLEN);
            SetupOperation(ref hValsRequestBuffer, ref hValsRequestBufferPointer, HVALS);

            // Pre-populate data
            SlowConsumeMessage("*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8);
            SlowConsumeMessage("*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\nf\r\n"u8);
            SlowConsumeMessage("*3\r\n$4\r\nSADD\r\n$1\r\ne\r\n$1\r\nb\r\n"u8);
            SlowConsumeMessage("*3\r\n$4\r\nHSET\r\n$1\r\nf\r\n$1\r\nb\r\n$1\r\nb\r\n"u8);
            SlowConsumeMessage("*4\r\n$4\r\nHSET\r\n$1\r\nf\r\n$1\r\nc\r\n$1\r\n5\r\n"u8);
            SlowConsumeMessage("*4\r\n$4\r\nHSET\r\n$1\r\nf\r\n$1\r\nd\r\n$3\r\n5.5\r\n"u8);
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

        [Benchmark]
        public void HExists()
        {
            _ = session.TryConsumeMessages(hExistsRequestBufferPointer, hExistsRequestBuffer.Length);
        }

        [Benchmark]
        public void HGet()
        {
            _ = session.TryConsumeMessages(hGetRequestBufferPointer, hGetRequestBuffer.Length);
        }

        [Benchmark]
        public void HGetAll()
        {
            _ = session.TryConsumeMessages(hGetAllRequestBufferPointer, hGetAllRequestBuffer.Length);
        }

        [Benchmark]
        public void HIncrby()
        {
            _ = session.TryConsumeMessages(hIncrbyRequestBufferPointer, hIncrbyRequestBuffer.Length);
        }

        [Benchmark]
        public void HIncrbyFloat()
        {
            _ = session.TryConsumeMessages(hIncrbyFloatRequestBufferPointer, hIncrbyFloatRequestBuffer.Length);
        }

        [Benchmark]
        public void HKeys()
        {
            _ = session.TryConsumeMessages(hKeysRequestBufferPointer, hKeysRequestBuffer.Length);
        }

        [Benchmark]
        public void HLen()
        {
            _ = session.TryConsumeMessages(hLenRequestBufferPointer, hLenRequestBuffer.Length);
        }

        [Benchmark]
        public void HMGet()
        {
            _ = session.TryConsumeMessages(hMGetRequestBufferPointer, hMGetRequestBuffer.Length);
        }

        [Benchmark]
        public void HMSet()
        {
            _ = session.TryConsumeMessages(hMSetRequestBufferPointer, hMSetRequestBuffer.Length);
        }

        [Benchmark]
        public void HRandField()
        {
            _ = session.TryConsumeMessages(hRandFieldRequestBufferPointer, hRandFieldRequestBuffer.Length);
        }

        [Benchmark]
        public void HScan()
        {
            _ = session.TryConsumeMessages(hScanRequestBufferPointer, hScanRequestBuffer.Length);
        }

        [Benchmark]
        public void HSetNx()
        {
            _ = session.TryConsumeMessages(hSetNxRequestBufferPointer, hSetNxRequestBuffer.Length);
        }

        [Benchmark]
        public void HStrLen()
        {
            _ = session.TryConsumeMessages(hStrLenRequestBufferPointer, hStrLenRequestBuffer.Length);
        }

        [Benchmark]
        public void HVals()
        {
            _ = session.TryConsumeMessages(hValsRequestBufferPointer, hValsRequestBuffer.Length);
        }
    }
}