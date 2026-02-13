// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Tsavorite.core;

namespace BenchmarkDotNetTests
{
    public class HashBytesTests
    {
        byte[] data;

        [Params(8, 16, 32, 64, 128, 256)]
        public int NumBytes;

        [GlobalSetup]
        public void Setup()
        {
            data = new byte[NumBytes];
            new Random(42).NextBytes(data);
        }

        [Benchmark(Baseline = true)]
        public long Original()
        {
            long h = 0;
            for (int i = 0; i < 1_000_000; i++)
                h = Utility.HashBytes(data);
            return h;
        }

        [Benchmark]
        public long Unrolled()
        {
            long h = 0;
            for (int i = 0; i < 1_000_000; i++)
                h = Utility.HashBytes_Unrolled(data);
            return h;
        }

        [Benchmark]
        public long MultiLane()
        {
            long h = 0;
            for (int i = 0; i < 1_000_000; i++)
                h = Utility.HashBytes_MultiLane(data);
            return h;
        }

        [Benchmark]
        public long XxHash()
        {
            long h = 0;
            for (int i = 0; i < 1_000_000; i++)
                h = Utility.HashBytes_XxHash(data);
            return h;
        }

        [Benchmark]
        public long WideRead()
        {
            long h = 0;
            for (int i = 0; i < 1_000_000; i++)
                h = Utility.HashBytes_WideRead(data);
            return h;
        }
    }
}
