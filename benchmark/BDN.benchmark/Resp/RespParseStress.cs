// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Embedded.perftest;
using Garnet.server;

namespace BDN.benchmark.Resp
{
    public unsafe class RespParseStress
    {
        EmbeddedRespServer server;
        RespServerSession session;
        byte[] batchBuffer;
        byte* batchBufferPtr;

        static ReadOnlySpan<byte> INLINE_PING => "PING\r\n"u8;
        static readonly int batchSize = 128;
        static readonly Random rand = new Random();

        [GlobalSetup]
        public void GlobalSetup()
        {
            var opt = new GarnetServerOptions
            {
                QuietMode = true
            };
            server = new EmbeddedRespServer(opt);
            session = server.GetRespSession();

            batchBuffer = GC.AllocateArray<byte>(INLINE_PING.Length * batchSize, pinned: true);
            batchBufferPtr = (byte*)Unsafe.AsPointer(ref batchBuffer[0]);
            var batchBufferSpan = new Span<byte>(batchBuffer);

            int batchBufferLength = 0;
            for (int i = 0; i < batchSize; i++)
            {
                INLINE_PING.CopyTo(batchBufferSpan.Slice(batchBufferLength));
                batchBufferLength += INLINE_PING.Length;
            }
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
            _ = session.TryConsumeMessages(batchBufferPtr, batchBuffer.Length);
        }
    }
}