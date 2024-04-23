// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using BenchmarkDotNet.Attributes;
using Embedded.perftest;
using Garnet.common;
using Garnet.server;

namespace BDN.benchmark.Resp
{
    public unsafe class RespIncrementStress
    {
        EmbeddedRespServer server;
        RespServerSession session;

        static ReadOnlySpan<byte> SINGLE_INCR => "*2\r\n$4\r\nINCR\r\n$1\r\nx\r\n"u8;
        static ReadOnlySpan<byte> SINGLE_DECR => "*2\r\n$4\r\nDECR\r\n$1\r\nx\r\n"u8;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var opt = new GarnetServerOptions
            {
                QuietMode = true
            };
            server = new EmbeddedRespServer(opt);
            session = server.GetRespSession();
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            session.Dispose();
            server.Dispose();
        }

        [Benchmark]
        public void Increment()
        {
            fixed (byte* ptr = SINGLE_INCR)
            {
                _ = session.TryConsumeMessages(ptr, SINGLE_INCR.Length);
            }
        }

        [Benchmark]
        public void Decrement()
        {
            fixed (byte* ptr = SINGLE_DECR)
            {
                _ = session.TryConsumeMessages(ptr, SINGLE_DECR.Length);
            }
        }

        [Benchmark]
        [ArgumentsSource(nameof(IncrByBenchInput))]
        public void IncrementBy(byte[] input)
        {
            fixed (byte* ptr = input)
            {
                _ = session.TryConsumeMessages(ptr, input.Length);
            }
        }

        [Benchmark]
        [ArgumentsSource(nameof(DecrByBenchInput))]
        public void DecrementBy(byte[] input)
        {
            fixed (byte* ptr = input)
            {
                _ = session.TryConsumeMessages(ptr, input.Length);
            }
        }

        public static IEnumerable<object> IncrByBenchInput => values.Select(x => Get(RespCommand.INCRBY, x));
        public static IEnumerable<object> DecrByBenchInput => values.Select(x => Get(RespCommand.DECRBY, x));

        public static int[] values => [int.MinValue, -1, 0, int.MaxValue];

        public static byte[] Get(RespCommand cmd, long val)
        {
            var length = NumUtils.NumDigitsInLong(val);
            return cmd switch
            {
                RespCommand.INCRBY => Encoding.ASCII.GetBytes($"*2\r\n$6\r\nINCRBY\r\n$1\r\nx\r\n${length}\r\n{val}\r\n"),
                RespCommand.DECRBY => Encoding.ASCII.GetBytes($"*2\r\n$6\r\nDECRBY\r\n$1\r\nx\r\n${length}\r\n{val}\r\n"),
                _ => throw new Exception($"Option {cmd} not supported")
            };
        }
    }
}