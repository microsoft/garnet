// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Tsavorite.core;

#pragma warning disable 0649 // Field 'field' is never assigned to, and will always have its default value 'value'; happens due to [Params(..)] 
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
#pragma warning disable IDE0048 // Add parentheses for clarity
#pragma warning disable IDE0130 // Namespace does not match folder structure

namespace BenchmarkDotNetTests
{
#pragma warning disable IDE0065 // Misplaced using directive
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>;

    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    public class OperationTests
    {
        public int NumRecords => BenchmarkDotNetTestsApp.NumRecords;

        TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        IDevice logDevice;
        string logDirectory;

        ClientSession<SpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> session;
        BasicContext<SpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> bContext;

        void SetupStore()
        {
            logDirectory = BenchmarkDotNetTestsApp.TestDirectory;
            var logFilename = Path.Combine(logDirectory, $"{nameof(OperationTests)}_{Guid.NewGuid()}.log");
            logDevice = Devices.CreateLogDevice(logFilename, preallocateFile: true, deleteOnClose: true, useIoCompletionPort: true);

            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = logDevice
            }, StoreFunctions.Create(new SpanByteComparer(), new SpanByteRecordDisposer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            session = store.NewSession<SpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new());
            bContext = session.BasicContext;
        }

        void PopulateStore(int start = 0)
        {
            Span<byte> keySpan = stackalloc byte[sizeof(long)];
            Span<byte> valueSpan = stackalloc byte[sizeof(long)];

            var end = start + NumRecords;

            for (long ii = start; ii < end; ++ii)
            {
                MemoryMarshal.Cast<byte, long>(keySpan)[0] = ii;
                MemoryMarshal.Cast<byte, long>(valueSpan)[0] = ii + NumRecords;
                _ = bContext.Upsert(new SpanByteKey(keySpan), valueSpan);
            }
        }

        [GlobalSetup]
        public void SetupPopulatedStore()
        {
            SetupStore();
            PopulateStore();
        }

        [GlobalCleanup]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            logDevice?.Dispose();
            logDevice = null;
            try
            {
                Directory.Delete(logDirectory);
            }
            catch { }
        }

        [BenchmarkCategory("Upsert"), Benchmark]
        public void Insert()
        {
            // Populate with a second batch
            PopulateStore(NumRecords);
        }

        [BenchmarkCategory("Upsert"), Benchmark]
        public void Upsert()
        {
            Span<byte> keySpan = stackalloc byte[sizeof(long)];
            Span<byte> valueSpan = stackalloc byte[sizeof(long)];

            for (long ii = 0; ii < NumRecords; ++ii)
            {
                MemoryMarshal.Cast<byte, long>(keySpan)[0] = ii;
                MemoryMarshal.Cast<byte, long>(valueSpan)[0] = ii + NumRecords * 2;
                _ = bContext.Upsert(new SpanByteKey(keySpan), valueSpan);
            }
        }

        [BenchmarkCategory("RMW"), Benchmark]
        public void RMW()
        {
            Span<byte> key = stackalloc byte[sizeof(long)];
            Span<byte> input = stackalloc byte[sizeof(long)];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

            for (long ii = 0; ii < NumRecords; ++ii)
            {
                MemoryMarshal.Cast<byte, long>(key)[0] = ii;
                MemoryMarshal.Cast<byte, long>(input)[0] = ii + NumRecords * 3;
                _ = bContext.RMW(new SpanByteKey(key), ref pinnedInputSpan);
            }

            _ = bContext.CompletePending();
        }

        [BenchmarkCategory("Read"), Benchmark]
        public void Read()
        {
            Span<byte> keySpan = stackalloc byte[sizeof(long)];

            for (long ii = 0; ii < NumRecords; ++ii)
            {
                MemoryMarshal.Cast<byte, long>(keySpan)[0] = ii;
                _ = bContext.Read(new SpanByteKey(keySpan));
            }
            _ = bContext.CompletePending();
        }
    }
}