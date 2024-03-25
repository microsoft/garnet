// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Garnet.common;

namespace BenchmarkDotNet.benchmark.Resp
{
    public unsafe class RespIntegerWriteBenchmarks
    {
        // Big enough buffer for the benchmarks
        private readonly byte[] _buffer = new byte[32];

        private byte* _bufferPtr;
        private GCHandle _bufferHandle;

        [GlobalSetup]
        public void GlobalSetup()
        {
            // Pin the buffer for the benchmarks
            _bufferHandle = GCHandle.Alloc(_buffer, GCHandleType.Pinned);
            _bufferPtr = (byte*)_bufferHandle.AddrOfPinnedObject();
        }

        [GlobalCleanup]
        public void GlobalCleanup() => _bufferHandle.Free();

        [Benchmark]
        [ArgumentsSource(nameof(SignedInt32Values))]
        public bool WriteInt32(int value)
        {
            var startPtr = _bufferPtr;
            return RespWriteUtils.WriteInteger(value, ref startPtr, _bufferPtr + _buffer.Length);
        }

        [Benchmark]
        [ArgumentsSource(nameof(SignedInt64Values))]
        public bool WriteInt64(long value)
        {
            var startPtr = _bufferPtr;
            return RespWriteUtils.WriteInteger(value, ref startPtr, _bufferPtr + _buffer.Length);
        }

        [Benchmark]
        [ArgumentsSource(nameof(SignedInt32Values))]
        public bool WriteInt32AsBulkString(int value)
        {
            var startPtr = _bufferPtr;
            return RespWriteUtils.WriteIntegerAsBulkString(value, ref startPtr, _bufferPtr + _buffer.Length);
        }

        [Benchmark]
        public void WriteInt32_AllAsciiLengths()
        {
            for (int i = 0; i < SignedInt32MultiplesOfTen.Length; i++)
            {
                var startPtr = _bufferPtr;
                RespWriteUtils.WriteInteger(SignedInt32MultiplesOfTen[i], ref startPtr, _bufferPtr + _buffer.Length);
            }
        }

        [Benchmark]
        public void WriteInt64_AllAsciiLengths()
        {
            for (int i = 0; i < SignedInt64MultiplesOfTen.Length; i++)
            {
                var startPtr = _bufferPtr;
                RespWriteUtils.WriteInteger(SignedInt64MultiplesOfTen[i], ref startPtr, _bufferPtr + _buffer.Length);
            }
        }

        [Benchmark]
        public void WriteInt32BulkString_AllAsciiLengths()
        {
            for (int i = 0; i < SignedInt32MultiplesOfTen.Length; i++)
            {
                var startPtr = _bufferPtr;
                RespWriteUtils.WriteIntegerAsBulkString(SignedInt32MultiplesOfTen[i], ref startPtr, _bufferPtr + _buffer.Length);
            }
        }

        public const int UnsignedInt32MaxValueDigits = 10; // The number of digits in (u)int.MaxValue
        public const int UnsignedInt64MaxValueDigits = 19; // The number of digits in (u)long.MaxValue

        // All multiples of 10 from 10^-10 to 10^10
        public static int[] SignedInt32MultiplesOfTen => [
            ..UnsignedInt32MultiplesOfTen.Select(n => n * -1),
            ..UnsignedInt32MultiplesOfTen
        ];
        public static int[] UnsignedInt32MultiplesOfTen => Enumerable.Range(0, 10).Select(n => (int)Math.Pow(10, n)).ToArray();

        // All multiples of 10 from 10^-19 to 10^19
        public static long[] SignedInt64MultiplesOfTen => [
            ..UnsignedInt64MultiplesOfTen.Select(n => n * -1),
            ..UnsignedInt64MultiplesOfTen
        ];
        public static long[] UnsignedInt64MultiplesOfTen => Enumerable.Range(0, 19).Select(n => (long)Math.Pow(10, n)).ToArray();

        public static int[] SignedInt32Values => [int.MinValue, -1, 0, int.MaxValue];
        public static long[] SignedInt64Values => [long.MinValue, -1, 0, long.MaxValue];
        public static ulong[] UnsignedInt64Values => [0, int.MaxValue, ulong.MaxValue];
    }
}