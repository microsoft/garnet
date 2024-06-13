// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Embedded.perftest;
using Garnet.server;
using Garnet.server.Auth.Settings;

namespace BDN.benchmark.Resp
{
    [MemoryDiagnoser]
    public unsafe class RespParseStress
    {
        EmbeddedRespServer server;
        RespServerSession session;
        protected IAuthenticationSettings authSettings = null;

        const int batchSize = 128;

        static ReadOnlySpan<byte> INLINE_PING => "PING\r\n"u8;
        byte[] pingRequestBuffer;
        byte* pingRequestBufferPointer;

        static ReadOnlySpan<byte> SET => "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\na\r\n"u8;
        byte[] setRequestBuffer;
        byte* setRequestBufferPointer;

        static ReadOnlySpan<byte> SETEX => "*4\r\n$5\r\nSETEX\r\n$1\r\nd\r\n$1\r\n9\r\n$1\r\nd\r\n"u8;
        byte[] setexRequestBuffer;
        byte* setexRequestBufferPointer;

        static ReadOnlySpan<byte> GET => "*2\r\n$3\r\nGET\r\n$1\r\nb\r\n"u8;
        byte[] getRequestBuffer;
        byte* getRequestBufferPointer;

        static ReadOnlySpan<byte> ZADDREM => "*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nc\r\n*3\r\n$4\r\nZREM\r\n$1\r\nc\r\n$1\r\nc\r\n"u8;
        byte[] zAddRemRequestBuffer;
        byte* zAddRemRequestBufferPointer;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var opt = new GarnetServerOptions
            {
                QuietMode = true,
                AuthSettings = authSettings,
            };
            server = new EmbeddedRespServer(opt);
            session = server.GetRespSession();

            pingRequestBuffer = GC.AllocateArray<byte>(INLINE_PING.Length * batchSize, pinned: true);
            pingRequestBufferPointer = (byte*)Unsafe.AsPointer(ref pingRequestBuffer[0]);
            for (int i = 0; i < batchSize; i++)
                INLINE_PING.CopyTo(new Span<byte>(pingRequestBuffer).Slice(i * INLINE_PING.Length));

            setRequestBuffer = GC.AllocateArray<byte>(SET.Length * batchSize, pinned: true);
            setRequestBufferPointer = (byte*)Unsafe.AsPointer(ref setRequestBuffer[0]);
            for (int i = 0; i < batchSize; i++)
                SET.CopyTo(new Span<byte>(setRequestBuffer).Slice(i * SET.Length));

            setexRequestBuffer = GC.AllocateArray<byte>(SETEX.Length * batchSize, pinned: true);
            setexRequestBufferPointer = (byte*)Unsafe.AsPointer(ref setexRequestBuffer[0]);
            for (int i = 0; i < batchSize; i++)
                SETEX.CopyTo(new Span<byte>(setexRequestBuffer).Slice(i * SETEX.Length));

            getRequestBuffer = GC.AllocateArray<byte>(GET.Length * batchSize, pinned: true);
            getRequestBufferPointer = (byte*)Unsafe.AsPointer(ref getRequestBuffer[0]);
            for (int i = 0; i < batchSize; i++)
                GET.CopyTo(new Span<byte>(getRequestBuffer).Slice(i * GET.Length));

            zAddRemRequestBuffer = GC.AllocateArray<byte>(ZADDREM.Length * batchSize, pinned: true);
            zAddRemRequestBufferPointer = (byte*)Unsafe.AsPointer(ref zAddRemRequestBuffer[0]);
            for (int i = 0; i < batchSize; i++)
                ZADDREM.CopyTo(new Span<byte>(zAddRemRequestBuffer).Slice(i * ZADDREM.Length));

            // Pre-populate sorted set with a single element to avoid repeatedly emptying it during the benchmark
            SlowConsumeMessage("*4\r\n$4\r\nZADD\r\n$1\r\nc\r\n$1\r\n1\r\n$1\r\nd\r\n"u8);
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            session.Dispose();
            server.Dispose();
        }

        [Benchmark]
        public void InlinePing()
        {
            _ = session.TryConsumeMessages(pingRequestBufferPointer, pingRequestBuffer.Length);
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
        public void Get()
        {
            _ = session.TryConsumeMessages(getRequestBufferPointer, getRequestBuffer.Length);
        }

        [Benchmark]
        public void ZAddRem()
        {
            _ = session.TryConsumeMessages(zAddRemRequestBufferPointer, zAddRemRequestBuffer.Length);
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