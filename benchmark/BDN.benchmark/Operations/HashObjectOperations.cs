// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Benchmark for HashObjectOperations
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class HashObjectOperations : OperationsBase
    {
        static ReadOnlySpan<byte> HSETDEL => "*4\r\n$4\r\nHSET\r\n$1\r\nf\r\n$1\r\na\r\n$1\r\na\r\n*3\r\n$4\r\nHDEL\r\n$1\r\nf\r\n$1\r\na\r\n"u8;
        Request hSetDel;

        static ReadOnlySpan<byte> HEXISTS => "*3\r\n$7\r\nHEXISTS\r\n$1\r\nf\r\n$1\r\na\r\n"u8;
        Request hExists;

        static ReadOnlySpan<byte> HGET => "*3\r\n$4\r\nHGET\r\n$1\r\nf\r\n$1\r\na\r\n"u8;
        Request hGet;

        static ReadOnlySpan<byte> HGETALL => "*2\r\n$7\r\nHGETALL\r\n$1\r\nf\r\n"u8;
        Request hGetAll;

        static ReadOnlySpan<byte> HINCRBY => "*4\r\n$7\r\nHINCRBY\r\n$1\r\nf\r\n$1\r\nc\r\n$1\r\n1\r\n"u8;
        Request hIncrby;

        static ReadOnlySpan<byte> HINCRBYFLOAT => "*4\r\n$12\r\nHINCRBYFLOAT\r\n$1\r\nf\r\n$1\r\nd\r\n$3\r\n1.5\r\n"u8;
        Request hIncrbyFloat;

        static ReadOnlySpan<byte> HKEYS => "*2\r\n$5\r\nHKEYS\r\n$1\r\nf\r\n"u8;
        Request hKeys;

        static ReadOnlySpan<byte> HLEN => "*2\r\n$4\r\nHLEN\r\n$1\r\nf\r\n"u8;
        Request hLen;

        static ReadOnlySpan<byte> HMGET => "*4\r\n$5\r\nHMGET\r\n$1\r\nf\r\n$1\r\na\r\n$1\r\nb\r\n"u8;
        Request hMGet;

        static ReadOnlySpan<byte> HMSET => "*6\r\n$5\r\nHMSET\r\n$1\r\nf\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"u8;
        Request hMSet;

        static ReadOnlySpan<byte> HRANDFIELD => "*2\r\n$10\r\nHRANDFIELD\r\n$1\r\nf\r\n"u8;
        Request hRandField;

        static ReadOnlySpan<byte> HSCAN => "*6\r\n$5\r\nHSCAN\r\n$1\r\nf\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$1\r\n5\r\n"u8;
        Request hScan;

        static ReadOnlySpan<byte> HSETNX => "*4\r\n$6\r\nHSETNX\r\n$1\r\nf\r\n$1\r\nx\r\n$1\r\n1\r\n"u8;
        Request hSetNx;

        static ReadOnlySpan<byte> HSTRLEN => "*3\r\n$7\r\nHSTRLEN\r\n$1\r\nf\r\n$1\r\na\r\n"u8;
        Request hStrLen;

        static ReadOnlySpan<byte> HVALS => "*2\r\n$5\r\nHVALS\r\n$1\r\nf\r\n"u8;
        Request hVals;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            SetupOperation(ref hSetDel, HSETDEL);
            SetupOperation(ref hExists, HEXISTS);
            SetupOperation(ref hGet, HGET);
            SetupOperation(ref hGetAll, HGETALL);
            SetupOperation(ref hIncrby, HINCRBY);
            SetupOperation(ref hIncrbyFloat, HINCRBYFLOAT);
            SetupOperation(ref hKeys, HKEYS);
            SetupOperation(ref hLen, HLEN);
            SetupOperation(ref hMGet, HMGET);
            SetupOperation(ref hMSet, HMSET);
            SetupOperation(ref hRandField, HRANDFIELD);
            SetupOperation(ref hScan, HSCAN);
            SetupOperation(ref hSetNx, HSETNX);
            SetupOperation(ref hStrLen, HSTRLEN);
            SetupOperation(ref hVals, HVALS);

            // Pre-populate data
            SlowConsumeMessage("*3\r\n$4\r\nHSET\r\n$1\r\nf\r\n$1\r\nb\r\n$1\r\nb\r\n"u8);
            SlowConsumeMessage("*4\r\n$4\r\nHSET\r\n$1\r\nf\r\n$1\r\nc\r\n$1\r\n5\r\n"u8);
            SlowConsumeMessage("*4\r\n$4\r\nHSET\r\n$1\r\nf\r\n$1\r\nd\r\n$3\r\n5.5\r\n"u8);
        }

        [Benchmark]
        public void HSetDel()
        {
            Send(hSetDel);
        }

        [Benchmark]
        public void HExists()
        {
            Send(hExists);
        }

        [Benchmark]
        public void HGet()
        {
            Send(hGet);
        }

        [Benchmark]
        public void HGetAll()
        {
            Send(hGetAll);
        }

        [Benchmark]
        public void HIncrby()
        {
            Send(hIncrby);
        }

        [Benchmark]
        public void HIncrbyFloat()
        {
            Send(hIncrbyFloat);
        }

        [Benchmark]
        public void HKeys()
        {
            Send(hKeys);
        }

        [Benchmark]
        public void HLen()
        {
            Send(hLen);
        }

        [Benchmark]
        public void HMGet()
        {
            Send(hMGet);
        }

        [Benchmark]
        public void HMSet()
        {
            Send(hMSet);
        }

        [Benchmark]
        public void HRandField()
        {
            Send(hRandField);
        }

        [Benchmark]
        public void HScan()
        {
            Send(hScan);
        }

        [Benchmark]
        public void HSetNx()
        {
            Send(hSetNx);
        }

        [Benchmark]
        public void HStrLen()
        {
            Send(hStrLen);
        }

        [Benchmark]
        public void HVals()
        {
            Send(hVals);
        }
    }
}