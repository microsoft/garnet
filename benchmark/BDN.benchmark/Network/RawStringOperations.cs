// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Network
{
    /// <summary>
    /// Benchmark for RawStringOperations
    /// </summary>
    [MemoryDiagnoser]
    public class RawStringNetworkOperations : NetworkBase
    {
        static ReadOnlySpan<byte> SET => "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n"u8;
        byte[] setRequestBuffer;
        unsafe byte* setRequestBufferPointer;

        static ReadOnlySpan<byte> SETEX => "*4\r\n$5\r\nSETEX\r\n$1\r\nd\r\n$1\r\n9\r\n$1\r\nd\r\n"u8;
        byte[] setexRequestBuffer;
        unsafe byte* setexRequestBufferPointer;

        static ReadOnlySpan<byte> SETNX => "*4\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n$2\r\nNX\r\n"u8;
        byte[] setnxRequestBuffer;
        unsafe byte* setnxRequestBufferPointer;

        static ReadOnlySpan<byte> SETXX => "*4\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n$2\r\nXX\r\n"u8;
        byte[] setxxRequestBuffer;
        unsafe byte* setxxRequestBufferPointer;

        static ReadOnlySpan<byte> GETNF => "*2\r\n$3\r\nGET\r\n$1\r\nb\r\n"u8;
        byte[] getnfRequestBuffer;
        unsafe byte* getnfRequestBufferPointer;

        static ReadOnlySpan<byte> GETF => "*2\r\n$3\r\nGET\r\n$1\r\na\r\n"u8;
        byte[] getfRequestBuffer;
        unsafe byte* getfRequestBufferPointer;

        static ReadOnlySpan<byte> INCR => "*2\r\n$4\r\nINCR\r\n$1\r\ni\r\n"u8;
        byte[] incrRequestBuffer;
        unsafe byte* incrRequestBufferPointer;

        static ReadOnlySpan<byte> DECR => "*2\r\n$4\r\nDECR\r\n$1\r\nj\r\n"u8;
        byte[] decrRequestBuffer;
        unsafe byte* decrRequestBufferPointer;

        static ReadOnlySpan<byte> INCRBY => "*3\r\n$6\r\nINCRBY\r\n$1\r\nk\r\n$10\r\n1234567890\r\n"u8;
        byte[] incrbyRequestBuffer;
        unsafe byte* incrbyRequestBufferPointer;

        static ReadOnlySpan<byte> DECRBY => "*3\r\n$6\r\nDECRBY\r\n$1\r\nl\r\n$10\r\n1234567890\r\n"u8;
        byte[] decrbyRequestBuffer;
        unsafe byte* decrbyRequestBufferPointer;

        public override unsafe void GlobalSetup()
        {
            base.GlobalSetup();
            SetupOperation(ref setRequestBuffer, ref setRequestBufferPointer, SET);
            SetupOperation(ref setexRequestBuffer, ref setexRequestBufferPointer, SETEX);
            SetupOperation(ref setnxRequestBuffer, ref setnxRequestBufferPointer, SETNX);
            SetupOperation(ref setxxRequestBuffer, ref setxxRequestBufferPointer, SETXX);
            SetupOperation(ref getfRequestBuffer, ref getfRequestBufferPointer, GETF);
            SetupOperation(ref getnfRequestBuffer, ref getnfRequestBufferPointer, GETNF);
            SetupOperation(ref incrRequestBuffer, ref incrRequestBufferPointer, INCR);
            SetupOperation(ref decrRequestBuffer, ref decrRequestBufferPointer, DECR);
            SetupOperation(ref incrbyRequestBuffer, ref incrbyRequestBufferPointer, INCRBY);
            SetupOperation(ref decrbyRequestBuffer, ref decrbyRequestBufferPointer, DECRBY);

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
            unsafe
            {
                PrepareBuffer(setRequestBuffer, setRequestBufferPointer);
            }
            await Send(setRequestBuffer.Length);
        }

        [Benchmark]
        public async ValueTask SetEx()
        {
            unsafe
            {
                PrepareBuffer(setexRequestBuffer, setexRequestBufferPointer);
            }
            await Send(setexRequestBuffer.Length);
        }

        [Benchmark]
        public async ValueTask SetNx()
        {
            unsafe
            {
                PrepareBuffer(setnxRequestBuffer, setnxRequestBufferPointer);
            }
            await Send(setnxRequestBuffer.Length);
        }

        [Benchmark]
        public async ValueTask SetXx()
        {
            unsafe
            {
                PrepareBuffer(setxxRequestBuffer, setxxRequestBufferPointer);

            }
            await Send(setxxRequestBuffer.Length);
        }

        [Benchmark]
        public async ValueTask GetFound()
        {
            unsafe
            {
                PrepareBuffer(getfRequestBuffer, getfRequestBufferPointer);
            }
            await Send(getfRequestBuffer.Length);
        }

        [Benchmark]
        public async ValueTask GetNotFound()
        {
            unsafe
            {
                PrepareBuffer(getnfRequestBuffer, getnfRequestBufferPointer);
            }
            await Send(getnfRequestBuffer.Length);
        }

        [Benchmark]
        public async ValueTask Increment()
        {
            unsafe
            {
                PrepareBuffer(incrRequestBuffer, incrRequestBufferPointer);
            }
            await Send(incrRequestBuffer.Length);
        }

        [Benchmark]
        public async ValueTask Decrement()
        {
            unsafe
            {
                PrepareBuffer(decrRequestBuffer, decrRequestBufferPointer);
            }
            await Send(decrRequestBuffer.Length);
        }

        [Benchmark]
        public async ValueTask IncrementBy()
        {
            unsafe
            {
                PrepareBuffer(incrbyRequestBuffer, incrbyRequestBufferPointer);
            }
            await Send(incrbyRequestBuffer.Length);
        }

        [Benchmark]
        public async ValueTask DecrementBy()
        {
            unsafe
            {
                PrepareBuffer(decrbyRequestBuffer, decrbyRequestBufferPointer);
            }
            await Send(decrbyRequestBuffer.Length);
        }
    }
}