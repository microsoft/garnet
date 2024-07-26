// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using Tsavorite.core;

#pragma warning disable 0649 // Field 'field' is never assigned to, and will always have its default value 'value'; happens due to [Params(..)] 
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
#pragma warning disable IDE0048 // Add parentheses for clarity
#pragma warning disable IDE0130 // Namespace does not match folder structure

namespace BenchmarkDotNetTests
{
#pragma warning disable IDE0065 // Misplaced using directive
    using SpanByteStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    [InliningDiagnoser(logFailuresOnly: true, allowedNamespaces: ["Tsavorite.core"])]
    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    public class InliningTests
    {
        [Params(1_000_000)]
        public int NumRecords;

        TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        IDevice logDevice;
        string logDirectory;

        void SetupStore()
        {
            logDirectory = BenchmarkDotNetTestsApp.TestDirectory;
            var logFilename = Path.Combine(logDirectory, $"{nameof(InliningTests)}_{Guid.NewGuid()}.log");
            logDevice = Devices.CreateLogDevice(logFilename, preallocateFile: true, deleteOnClose: true, useIoCompletionPort: true);

            store = new (new () 
                {
                    IndexSize = 1L << 26,
                    LogDevice = logDevice
                }, StoreFunctions<SpanByte, SpanByte>.Create()
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        unsafe void PopulateStore()
        {
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new());
            var bContext = session.BasicContext;

            Span<byte> keyVec = stackalloc byte[sizeof(long)];
            var keySpanByte = SpanByte.FromPinnedSpan(keyVec);

            Span<byte> valueVec = stackalloc byte[sizeof(long)];
            var valueSpanByte = SpanByte.FromPinnedSpan(valueVec);

            for (long ii = 0; ii < NumRecords; ++ii)
            {
                *(long*)keySpanByte.ToPointer() = ii;
                *(long*)valueSpanByte.ToPointer() = ii + NumRecords;
                _ = bContext.Upsert(keySpanByte, valueSpanByte);
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
        public unsafe void Upsert()
        {
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new());
            var bContext = session.BasicContext;

            Span<byte> keyVec = stackalloc byte[sizeof(long)];
            var keySpanByte = SpanByte.FromPinnedSpan(keyVec);

            Span<byte> valueVec = stackalloc byte[sizeof(long)];
            var valueSpanByte = SpanByte.FromPinnedSpan(valueVec);

            for (long ii = 0; ii < NumRecords; ++ii)
            {
                *(long*)keySpanByte.ToPointer() = ii;
                *(long*)valueSpanByte.ToPointer() = ii + NumRecords * 2;
                _ = bContext.Upsert(keySpanByte, valueSpanByte);
            }
        }

        [BenchmarkCategory("RMW"), Benchmark]
        public unsafe void RMW()
        {
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new());
            var bContext = session.BasicContext;

            Span<byte> keyVec = stackalloc byte[sizeof(long)];
            var keySpanByte = SpanByte.FromPinnedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[sizeof(long)];
            var inputSpanByte = SpanByte.FromPinnedSpan(inputVec);

            for (long ii = 0; ii < NumRecords; ++ii)
            {
                *(long*)keySpanByte.ToPointer() = ii;
                *(long*)inputSpanByte.ToPointer() = ii + NumRecords * 3;
                _ = bContext.RMW(keySpanByte, inputSpanByte);
            }

            _ = bContext.CompletePending();
        }

        [BenchmarkCategory("Read"), Benchmark]
        public unsafe void Read()
        {
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new());
            var bContext = session.BasicContext;

            Span<byte> keyVec = stackalloc byte[sizeof(long)];
            var keySpanByte = SpanByte.FromPinnedSpan(keyVec);

            for (long ii = 0; ii < NumRecords; ++ii)
            {
                *(long*)keySpanByte.ToPointer() = ii;
                _ = bContext.Read(keySpanByte);
            }
            _ = bContext.CompletePending();
        }
    }
}
