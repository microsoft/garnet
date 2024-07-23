// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using BenchmarkDotNet.Attributes;
using Embedded.perftest;
using Garnet.common;
using Garnet.server;

namespace BDN.benchmark.Resp
{
    public unsafe class RespGetSetStress
    {
        EmbeddedRespServer server;
        RespServerSession session;

        static ReadOnlySpan<byte> SINGLE_GET => "*2\r\n$3\r\nGET\r\n$1\r\nx\r\n"u8;
        static ReadOnlySpan<byte> SINGLE_SET => "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\n1\r\n"u8;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var opt = new GarnetServerOptions
            {
                QuietMode = true
            };
            server = new EmbeddedRespServer(opt);
            session = server.GetRespSession();

            // Set the initial value
            fixed (byte* ptr = SINGLE_SET)
            {
                _ = session.TryConsumeMessages(ptr, SINGLE_SET.Length);
            }
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
            fixed (byte* ptr = SINGLE_GET)
            {
                _ = session.TryConsumeMessages(ptr, SINGLE_GET.Length);
            }
        }

        [Benchmark]
        public void Set()
        {
            fixed (byte* ptr = SINGLE_SET)
            {
                _ = session.TryConsumeMessages(ptr, SINGLE_SET.Length);
            }
        }
    }
}