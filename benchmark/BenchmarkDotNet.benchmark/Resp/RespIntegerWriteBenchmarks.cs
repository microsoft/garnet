// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using BenchmarkDotNet.Attributes;
using System.Runtime.InteropServices;

namespace BenchmarkDotNet.benchmark.Resp
{
    public unsafe class RespIntegerWriteBenchmarks
    {
        private const int OperationsPerInvoke = 16;

        // Big enough buffer for the benchmarks
        private readonly byte[] _buffer = new byte[32];

        private readonly int[] _random32BitIntegers = new int[OperationsPerInvoke];
        private readonly long[] _random64BitIntegers = new long[OperationsPerInvoke];

        private byte* _bufferPtr;
        private GCHandle _bufferHandle;

        [GlobalSetup]
        public void GlobalSetup()
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

            // Pin the buffer for the benchmarks
            _bufferHandle = GCHandle.Alloc(_buffer, GCHandleType.Pinned);
            _bufferPtr = (byte*)_bufferHandle.AddrOfPinnedObject();
        }

        [GlobalCleanup]
        public void GlobalCleanup() => _bufferHandle.Free();

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        [ArgumentsSource(nameof(SignedInt32Values))]
        public bool WriteInt32(int value)
        {
            var result = false;
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                var startPtr = _bufferPtr;
                result |= RespWriteUtils.WriteInteger(value, ref startPtr, startPtr + _buffer.Length);
            }
            return result;
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        [ArgumentsSource(nameof(SignedInt64Values))]
        public bool WriteInt64(long value)
        {
            var result = false;
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                var startPtr = _bufferPtr;
                result |= RespWriteUtils.WriteInteger(value, ref startPtr, _bufferPtr + _buffer.Length);
            }
            return result;
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        [ArgumentsSource(nameof(SignedInt32Values))]
        public bool WriteInt32AsBulkString(int value)
        {
            var result = false;
            for (int i = 0; i < OperationsPerInvoke; i++)
            {
                var startPtr = _bufferPtr;
                result |= RespWriteUtils.WriteIntegerAsBulkString(value, ref startPtr, _bufferPtr + _buffer.Length);
            }
            return result;
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public bool WriteRandomInt32()
        {
            var result = false;
            for (int i = 0; i < _random32BitIntegers.Length; i++)
            {
                var startPtr = _bufferPtr;
                result |= RespWriteUtils.WriteInteger(_random32BitIntegers[i], ref startPtr, _bufferPtr + _buffer.Length);
            }
            return result;
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public bool WriteRandomInt64()
        {
            var result = false;
            for (int i = 0; i < _random64BitIntegers.Length; i++)
            {
                var startPtr = _bufferPtr;
                result |= RespWriteUtils.WriteInteger(_random64BitIntegers[i], ref startPtr, _bufferPtr + _buffer.Length);
            }
            return result;
        }

        [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
        public bool WriteRandomInt32BulkStrings()
        {
            var result = false;
            for (int i = 0; i < _random32BitIntegers.Length; i++)
            {
                var startPtr = _bufferPtr;
                result |= RespWriteUtils.WriteIntegerAsBulkString(_random32BitIntegers[i], ref startPtr, _bufferPtr + _buffer.Length);
            }
            return result;
        }

        public static int[] SignedInt32Values => [int.MinValue, -1, 0, int.MaxValue];
        public static long[] SignedInt64Values => [long.MinValue, int.MinValue, -1, 0, int.MaxValue, long.MaxValue];
        public static ulong[] UnsignedInt64Values => [0, int.MaxValue, ulong.MaxValue];
    }
}
