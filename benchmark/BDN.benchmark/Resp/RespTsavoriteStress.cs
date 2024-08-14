// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Embedded.perftest;
using Garnet.server;

namespace BDN.benchmark.Resp
{
    public unsafe class RespTsavoriteStress
    {
        EmbeddedRespServer server;
        RespServerSession session;

        const int batchSize = 128;

        static ReadOnlySpan<byte> GET => "*2\r\n$3\r\nGET\r\n$1\r\nx\r\n"u8;
        byte[] getRequestBuffer;
        byte* getRequestBufferPointer;

        static ReadOnlySpan<byte> SET => "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n"u8;
        byte[] setRequestBuffer;
        byte* setRequestBufferPointer;

        static ReadOnlySpan<byte> INCR => "*2\r\n$4\r\nINCR\r\n$1\r\nx\r\n"u8;
        byte[] incrRequestBuffer;
        byte* incrRequestBufferPointer;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var opt = new GarnetServerOptions
            {
                QuietMode = true
            };
            server = new EmbeddedRespServer(opt);
            session = server.GetRespSession();

            CreateBuffer(GET, out getRequestBuffer, out getRequestBufferPointer);
            CreateBuffer(SET, out setRequestBuffer, out setRequestBufferPointer);
            CreateBuffer(INCR, out incrRequestBuffer, out incrRequestBufferPointer);

            // Set the initial value (needed for GET)
            _ = session.TryConsumeMessages(setRequestBufferPointer, setRequestBuffer.Length);
        }

        unsafe void CreateBuffer(ReadOnlySpan<byte> cmd, out byte[] buffer, out byte* bufferPointer)
        {
            buffer = GC.AllocateArray<byte>(cmd.Length * batchSize, pinned: true);
            bufferPointer = (byte*)Unsafe.AsPointer(ref buffer[0]);
            for (int i = 0; i < batchSize; i++)
                cmd.CopyTo(new Span<byte>(buffer).Slice(i * cmd.Length));
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            session.Dispose();
            server.Dispose();
        }

        [Benchmark]
        public void Get()
        {
            _ = session.TryConsumeMessages(getRequestBufferPointer, getRequestBuffer.Length);
        }

        [Benchmark]
        public void Set()
        {
            _ = session.TryConsumeMessages(setRequestBufferPointer, setRequestBuffer.Length);
        }

        [Benchmark]
        public void Incr()
        {
            _ = session.TryConsumeMessages(incrRequestBufferPointer, incrRequestBuffer.Length);
        }
    }
}