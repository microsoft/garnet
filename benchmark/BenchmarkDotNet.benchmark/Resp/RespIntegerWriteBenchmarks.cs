// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using BenchmarkDotNet.Attributes;

namespace BenchmarkDotNet.benchmark.Resp
{
    public unsafe class RespIntegerWriteBenchmarks
    {
        // Big enough buffer for the benchmarks
        private const int BufferSize = 32;

        private readonly int[] _random32BitIntegers = new int[32];
        private readonly long[] _random64BitIntegers = new long[32];

        public RespIntegerWriteBenchmarks()
        {
            var random = new Random(42);
            for (int i = 0; i < _random32BitIntegers.Length; i++)
            {
                _random32BitIntegers[i] = random.Next();
            }
            for (int i = 0; i < _random64BitIntegers.Length; i++)
            {
                _random64BitIntegers[i] = random.NextInt64();
            }
        }

        [Benchmark]
        [ArgumentsSource(nameof(SignedInt32Values))]
        public bool WriteInt32(int value)
        {
            byte* bufferPtr = stackalloc byte[BufferSize];
            return RespWriteUtils.WriteInteger(value, ref bufferPtr, bufferPtr + BufferSize);
        }

        [Benchmark]
        [ArgumentsSource(nameof(SignedInt64Values))]
        public bool WriteInt64(long value)
        {
            byte* bufferPtr = stackalloc byte[BufferSize];
            return RespWriteUtils.WriteInteger(value, ref bufferPtr, bufferPtr + BufferSize);
        }

        [Benchmark]
        [ArgumentsSource(nameof(SignedInt32Values))]
        public bool WriteInt32AsBulkString(int value)
        {
            byte* bufferPtr = stackalloc byte[BufferSize];
            return RespWriteUtils.WriteIntegerAsBulkString(value, ref bufferPtr, bufferPtr + BufferSize);
        }

        [Benchmark]
        public void WriteRandomInt32()
        {
            byte* bufferPtr = stackalloc byte[BufferSize];
            for (int i = 0; i < _random32BitIntegers.Length; i++)
            {
                var startPtr = bufferPtr;
                RespWriteUtils.WriteInteger(_random32BitIntegers[i], ref startPtr, bufferPtr + BufferSize);
            }
        }

        [Benchmark]
        public void WriteRandomInt64()
        {
            byte* bufferPtr = stackalloc byte[BufferSize];
            for (int i = 0; i < _random64BitIntegers.Length; i++)
            {
                var startPtr = bufferPtr;
                RespWriteUtils.WriteInteger(_random64BitIntegers[i], ref startPtr, bufferPtr + BufferSize);
            }
        }

        [Benchmark]
        public void WriteRandomInt32BulkStrings()
        {
            byte* bufferPtr = stackalloc byte[BufferSize];
            for (int i = 0; i < _random32BitIntegers.Length; i++)
            {
                var startPtr = bufferPtr;
                RespWriteUtils.WriteIntegerAsBulkString(_random32BitIntegers[i], ref startPtr, bufferPtr + BufferSize);
            }
        }

        public static int[] SignedInt32Values => [int.MinValue, -1, 0, int.MaxValue];
        public static long[] SignedInt64Values => [long.MinValue, int.MinValue, -1, 0, int.MaxValue, long.MaxValue];
        public static ulong[] UnsignedInt64Values => [0, int.MaxValue, ulong.MaxValue];
    }
}
