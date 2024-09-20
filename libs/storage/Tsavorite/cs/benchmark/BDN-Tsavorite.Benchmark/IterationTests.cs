// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

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
    using SpanByteStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    public class IterationTests
    {
        const int NumRecords = 1_000_000;

        [Params(true, false)]
        public bool FlushAndEvict;

        TsavoriteKV<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        IDevice logDevice;
        string logDirectory;

        void SetupStore()
        {
            logDirectory = BenchmarkDotNetTestsApp.TestDirectory;
            var logFilename = Path.Combine(logDirectory, $"{nameof(IterationTests)}_{Guid.NewGuid()}.log");
            logDevice = Devices.CreateLogDevice(logFilename, preallocateFile: true, deleteOnClose: true, useIoCompletionPort: true);

            store = new(new()
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

            if (FlushAndEvict)
                store.Log.FlushAndEvict(wait: true);
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

        [BenchmarkCategory("Cursor"), Benchmark]
        public void Cursor()
        {
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new());

            var scanFunctions = new ScanFunctions();
            var cursor = 0L;
            session.ScanCursor(ref cursor, long.MaxValue, scanFunctions);
            if (scanFunctions.Count < NumRecords)
                throw new ApplicationException($"Incomplete iteration; {scanFunctions.Count} of {NumRecords} records returned");
        }

        class ScanCounter
        {
            internal int count;
        }

        internal struct ScanFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
        {
            private readonly ScanCounter counter;

            internal readonly int Count => counter.count;

            public ScanFunctions() => counter = new();

            /// <inheritdoc/>
            public bool OnStart(long beginAddress, long endAddress) => true;

            /// <inheritdoc/>
            public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                ++counter.count;
                cursorRecordResult = CursorRecordResult.Accept;
                return true;
            }

            /// <inheritdoc/>
            public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            /// <inheritdoc/>
            public void OnStop(bool completed, long numberOfRecords) { }

            /// <inheritdoc/>
            public void OnException(Exception exception, long numberOfRecords) { }
        }
    }
}