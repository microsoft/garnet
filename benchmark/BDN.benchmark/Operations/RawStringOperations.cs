// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Operations
{
    [MemoryDiagnoser]
    public unsafe class RawStringOperations : OperationsBase
    {
        static ReadOnlySpan<byte> SET => "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n"u8;
        byte[] setRequestBuffer;
        byte* setRequestBufferPointer;

        static ReadOnlySpan<byte> SETEX => "*4\r\n$5\r\nSETEX\r\n$1\r\nd\r\n$1\r\n9\r\n$1\r\nd\r\n"u8;
        byte[] setexRequestBuffer;
        byte* setexRequestBufferPointer;

        static ReadOnlySpan<byte> SETNX => "*4\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n$2\r\nNX\r\n"u8;
        byte[] setnxRequestBuffer;
        byte* setnxRequestBufferPointer;

        static ReadOnlySpan<byte> SETXX => "*4\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n$2\r\nXX\r\n"u8;
        byte[] setxxRequestBuffer;
        byte* setxxRequestBufferPointer;

        static ReadOnlySpan<byte> GETNF => "*2\r\n$3\r\nGET\r\n$1\r\nb\r\n"u8;
        byte[] getnfRequestBuffer;
        byte* getnfRequestBufferPointer;

        static ReadOnlySpan<byte> GETF => "*2\r\n$3\r\nGET\r\n$1\r\na\r\n"u8;
        byte[] getfRequestBuffer;
        byte* getfRequestBufferPointer;

        static ReadOnlySpan<byte> INCR => "*2\r\n$4\r\nINCR\r\n$1\r\ni\r\n"u8;
        byte[] incrRequestBuffer;
        byte* incrRequestBufferPointer;

        static ReadOnlySpan<byte> DECR => "*2\r\n$4\r\nDECR\r\n$1\r\nj\r\n"u8;
        byte[] decrRequestBuffer;
        byte* decrRequestBufferPointer;

        static ReadOnlySpan<byte> INCRBY => "*3\r\n$6\r\nINCRBY\r\n$1\r\nk\r\n$10\r\n1234567890\r\n"u8;
        byte[] incrbyRequestBuffer;
        byte* incrbyRequestBufferPointer;

        static ReadOnlySpan<byte> DECRBY => "*3\r\n$6\r\nDECRBY\r\n$1\r\nl\r\n$10\r\n1234567890\r\n"u8;
        byte[] decrbyRequestBuffer;
        byte* decrbyRequestBufferPointer;

        public override void GlobalSetup()
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
        public void Set()
        {
            _ = session.TryConsumeMessages(setRequestBufferPointer, setRequestBuffer.Length);
        }

        [Benchmark]
        public void SetEx()
        {
            _ = session.TryConsumeMessages(setexRequestBufferPointer, setexRequestBuffer.Length);
        }

        [Benchmark]
        public void SetNx()
        {
            _ = session.TryConsumeMessages(setnxRequestBufferPointer, setnxRequestBuffer.Length);
        }

        [Benchmark]
        public void SetXx()
        {
            _ = session.TryConsumeMessages(setxxRequestBufferPointer, setxxRequestBuffer.Length);
        }

        [Benchmark]
        public void GetFound()
        {
            _ = session.TryConsumeMessages(getfRequestBufferPointer, getfRequestBuffer.Length);
        }

        [Benchmark]
        public void GetNotFound()
        {
            _ = session.TryConsumeMessages(getnfRequestBufferPointer, getnfRequestBuffer.Length);
        }

        [Benchmark]
        public void Increment()
        {
            _ = session.TryConsumeMessages(incrRequestBufferPointer, incrRequestBuffer.Length);
        }

        [Benchmark]
        public void Decrement()
        {
            _ = session.TryConsumeMessages(decrRequestBufferPointer, decrRequestBuffer.Length);
        }

        [Benchmark]
        public void IncrementBy()
        {
            _ = session.TryConsumeMessages(incrbyRequestBufferPointer, incrbyRequestBuffer.Length);
        }

        [Benchmark]
        public void DecrementBy()
        {
            _ = session.TryConsumeMessages(decrbyRequestBufferPointer, decrbyRequestBuffer.Length);
        }
    }
}