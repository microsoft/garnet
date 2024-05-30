// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Globalization;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Garnet.common;

namespace BDN.benchmark.Resp
{
    [MemoryDiagnoser]
    public unsafe class RespDoubleWriteBenchmarks
    {
        // Big enough buffer for the benchmarks
        private readonly byte[] _buffer = new byte[512];

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
        [ArgumentsSource(nameof(TestDoubles))]
        public bool WriteDoubleAsBulkString(double value)
        {
            var startPtr = _bufferPtr;
            return RespWriteUtils.TryWriteDoubleBulkString(value, ref startPtr, _bufferPtr + _buffer.Length);
        }

        [Benchmark]
        [ArgumentsSource(nameof(TestDoubles))]
        public bool WriteDoubleAsBulkStringByTranscoding(double value)
        {
            var startPtr = _bufferPtr;
            return RespWriteUtils.WriteAsciiBulkString(value.ToString(CultureInfo.InvariantCulture), ref startPtr, _bufferPtr + _buffer.Length);
        }

        public static double[] TestDoubles => [
            double.MinValue,
            -1.0,
            0.0,
            1.0,
            3.141592653589793,
            double.MaxValue
        ];
    }
}