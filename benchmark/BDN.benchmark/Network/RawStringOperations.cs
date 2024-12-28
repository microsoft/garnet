// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Embedded.server;

namespace BDN.benchmark.Network
{
    /// <summary>
    /// Benchmark for RawStringOperations
    /// </summary>
    [MemoryDiagnoser]
    public class RawStringOperations : NetworkBase
    {
        static ReadOnlySpan<byte> SET => "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n"u8;
        Request set;

        static ReadOnlySpan<byte> SETEX => "*4\r\n$5\r\nSETEX\r\n$1\r\nd\r\n$1\r\n9\r\n$1\r\nd\r\n"u8;
        Request setex;

        static ReadOnlySpan<byte> SETNX => "*4\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n$2\r\nNX\r\n"u8;
        Request setnx;

        static ReadOnlySpan<byte> SETXX => "*4\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n$2\r\nXX\r\n"u8;
        Request setxx;

        static ReadOnlySpan<byte> GETNF => "*2\r\n$3\r\nGET\r\n$1\r\nb\r\n"u8;
        Request getnf;

        static ReadOnlySpan<byte> GETF => "*2\r\n$3\r\nGET\r\n$1\r\na\r\n"u8;
        Request getf;

        static ReadOnlySpan<byte> INCR => "*2\r\n$4\r\nINCR\r\n$1\r\ni\r\n"u8;
        Request incr;

        static ReadOnlySpan<byte> DECR => "*2\r\n$4\r\nDECR\r\n$1\r\nj\r\n"u8;
        Request decr;

        static ReadOnlySpan<byte> INCRBY => "*3\r\n$6\r\nINCRBY\r\n$1\r\nk\r\n$10\r\n1234567890\r\n"u8;
        Request incrby;

        static ReadOnlySpan<byte> DECRBY => "*3\r\n$6\r\nDECRBY\r\n$1\r\nl\r\n$10\r\n1234567890\r\n"u8;
        Request decrby;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            SetupOperation(ref set, SET);
            SetupOperation(ref setex, SETEX);
            SetupOperation(ref setnx, SETNX);
            SetupOperation(ref setxx, SETXX);
            SetupOperation(ref getf, GETF);
            SetupOperation(ref getnf, GETNF);
            SetupOperation(ref incr, INCR);
            SetupOperation(ref decr, DECR);
            SetupOperation(ref incrby, INCRBY);
            SetupOperation(ref decrby, DECRBY);

            // Pre-populate data
            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n"u8);
            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$1\r\ni\r\n$1\r\n0\r\n"u8);
            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$1\r\nj\r\n$1\r\n0\r\n"u8);
            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n0\r\n"u8);
            SlowConsumeMessage("*3\r\n$3\r\nSET\r\n$1\r\nl\r\n$1\r\n0\r\n"u8);
        }

        [Benchmark]
        public async ValueTask Set()
        {
            await Send(set);
        }

        [Benchmark]
        public async ValueTask SetEx()
        {
            await Send(setex);
        }

        [Benchmark]
        public async ValueTask SetNx()
        {
            await Send(setnx);
        }

        [Benchmark]
        public async ValueTask SetXx()
        {
            await Send(setxx);
        }

        [Benchmark]
        public async ValueTask GetFound()
        {
            await Send(getf);
        }

        [Benchmark]
        public async ValueTask GetNotFound()
        {
            await Send(getnf);
        }

        [Benchmark]
        public async ValueTask Increment()
        {
            await Send(incr);
        }

        [Benchmark]
        public async ValueTask Decrement()
        {
            await Send(decr);
        }

        [Benchmark]
        public async ValueTask IncrementBy()
        {
            await Send(incrby);
        }

        [Benchmark]
        public async ValueTask DecrementBy()
        {
            await Send(decrby);
        }
    }
}