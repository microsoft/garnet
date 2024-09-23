// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Embedded.perftest;
using Garnet.server;

namespace BDN.benchmark.Resp
{
    [MemoryDiagnoser]
    public unsafe class RespAofStress
    {
        EmbeddedRespServer server;
        RespServerSession session;
        const int batchSize = 128;

        static ReadOnlySpan<byte> SET => "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n"u8;
        byte[] setRequestBuffer;
        byte* setRequestBufferPointer;

        static ReadOnlySpan<byte> INCR => "*2\r\n$4\r\nINCR\r\n$1\r\ni\r\n"u8;
        byte[] incrRequestBuffer;
        byte* incrRequestBufferPointer;

        static ReadOnlySpan<byte> LPUSHPOP => "*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\ne\r\n*2\r\n$4\r\nLPOP\r\n$1\r\nd\r\n"u8;
        byte[] lPushPopRequestBuffer;
        byte* lPushPopRequestBufferPointer;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var opt = new GarnetServerOptions
            {
                QuietMode = true,
                EnableAOF = true,
                UseAofNullDevice = true,
                MainMemoryReplication = true,
                CommitFrequencyMs = -1,
                AofPageSize = "128m",
                AofMemorySize = "256m",
            };
            server = new EmbeddedRespServer(opt);

            session = server.GetRespSession();

            setRequestBuffer = GC.AllocateArray<byte>(SET.Length * batchSize, pinned: true);
            setRequestBufferPointer = (byte*)Unsafe.AsPointer(ref setRequestBuffer[0]);
            for (int i = 0; i < batchSize; i++)
                SET.CopyTo(new Span<byte>(setRequestBuffer).Slice(i * SET.Length));

            _ = session.TryConsumeMessages(setRequestBufferPointer, setRequestBuffer.Length);

            incrRequestBuffer = GC.AllocateArray<byte>(INCR.Length * batchSize, pinned: true);
            incrRequestBufferPointer = (byte*)Unsafe.AsPointer(ref incrRequestBuffer[0]);
            for (int i = 0; i < batchSize; i++)
                INCR.CopyTo(new Span<byte>(incrRequestBuffer).Slice(i * INCR.Length));

            lPushPopRequestBuffer = GC.AllocateArray<byte>(LPUSHPOP.Length * batchSize, pinned: true);
            lPushPopRequestBufferPointer = (byte*)Unsafe.AsPointer(ref lPushPopRequestBuffer[0]);
            for (int i = 0; i < batchSize; i++)
                LPUSHPOP.CopyTo(new Span<byte>(lPushPopRequestBuffer).Slice(i * LPUSHPOP.Length));

            // Pre-populate list with a single element to avoid repeatedly emptying it during the benchmark
            SlowConsumeMessage("*3\r\n$5\r\nLPUSH\r\n$1\r\nd\r\n$1\r\nf\r\n"u8);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            session.Dispose();
            server.Dispose();
        }

        [Benchmark]
        public void Set()
        {
            _ = session.TryConsumeMessages(setRequestBufferPointer, setRequestBuffer.Length);
        }

        [Benchmark]
        public void Increment()
        {
            _ = session.TryConsumeMessages(incrRequestBufferPointer, incrRequestBuffer.Length);
        }

        [Benchmark]
        public void LPushPop()
        {
            _ = session.TryConsumeMessages(lPushPopRequestBufferPointer, lPushPopRequestBuffer.Length);
        }

        private void SlowConsumeMessage(ReadOnlySpan<byte> message)
        {
            var buffer = GC.AllocateArray<byte>(message.Length, pinned: true);
            var bufferPointer = (byte*)Unsafe.AsPointer(ref buffer[0]);
            message.CopyTo(new Span<byte>(buffer));
            _ = session.TryConsumeMessages(bufferPointer, buffer.Length);
        }
    }
}