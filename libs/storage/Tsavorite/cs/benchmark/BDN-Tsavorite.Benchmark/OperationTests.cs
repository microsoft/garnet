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
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>;

    [GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory, BenchmarkLogicalGroupRule.ByParams)]
    public class OperationTests
    {
        public int NumRecords => BenchmarkDotNetTestsApp.NumRecords;

        TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        IDevice logDevice;
        string logDirectory;

        ClientSession<SpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, BDNSpanByteFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> session;
        BasicContext<SpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, BDNSpanByteFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> bContext;

        void SetupStore()
        {
            logDirectory = BenchmarkDotNetTestsApp.TestDirectory;
            var logFilename = Path.Combine(logDirectory, $"{nameof(OperationTests)}_{Guid.NewGuid()}.log");
            logDevice = Devices.CreateLogDevice(logFilename, preallocateFile: true, deleteOnClose: true, useIoCompletionPort: true);

            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = logDevice
            }, StoreFunctions.Create(new SpanByteComparer(), new SpanByteRecordTriggers())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            session = store.NewSession<SpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, BDNSpanByteFunctions>(new());
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

        [Benchmark]
        public void Insert()
        {
            // Populate with a second batch
            PopulateStore(NumRecords);
        }

        [Benchmark]
        public void Upsert()
        {
            Span<byte> keySpan = stackalloc byte[sizeof(long)];
            var key = new SpanByteKey(keySpan);
            ref var keyLongRef = ref MemoryMarshal.Cast<byte, long>(keySpan)[0];

            Span<byte> valueSpan = stackalloc byte[sizeof(long)];
            ref var valueLongRef = ref MemoryMarshal.Cast<byte, long>(valueSpan)[0];

            for (long ii = 0; ii < NumRecords; ++ii)
            {
                keyLongRef = ii;
                valueLongRef = ii + NumRecords * 2;
                _ = bContext.Upsert(key, valueSpan);
            }
        }

        [Benchmark]
        public void RMW()
        {
            Span<byte> keySpan = stackalloc byte[sizeof(long)];
            var key = new SpanByteKey(keySpan);
            ref var keyLongRef = ref MemoryMarshal.Cast<byte, long>(keySpan)[0];

            Span<byte> inputSpan = stackalloc byte[sizeof(long)];
            ref var inputLongRef = ref MemoryMarshal.Cast<byte, long>(inputSpan)[0];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(inputSpan);

            for (long ii = 0; ii < NumRecords; ++ii)
            {
                keyLongRef = ii;
                inputLongRef = ii + NumRecords * 3;
                _ = bContext.RMW(key, ref pinnedInputSpan);
            }

            _ = bContext.CompletePending();
        }

        [Benchmark]
        public void Read()
        {
            Span<byte> keySpan = stackalloc byte[sizeof(long)];
            var key = new SpanByteKey(keySpan);
            ref var keyLongRef = ref MemoryMarshal.Cast<byte, long>(keySpan)[0];

            Span<byte> outputSpan = stackalloc byte[sizeof(long)];
            var output = SpanByteAndMemory.FromPinnedSpan(outputSpan);

            for (long ii = 0; ii < NumRecords; ++ii)
            {
                keyLongRef = ii;
                _ = bContext.Read(key, ref output);
            }
            _ = bContext.CompletePending();
        }
    }

    public sealed class BDNSpanByteFunctions : SpanByteFunctions<Empty>
    {
        /// <inheritdoc />
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
        {
            srcLogRecord.ValueSpan.CopyTo(output.SpanByte.Span);
            return true;
        }

        // Note: Currently, only the ReadOnlySpan<byte> form of InPlaceWriter value is used here.

        /// <inheritdoc/>
        public override bool InPlaceWriter(ref LogRecord logRecord, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration
            srcValue.CopyTo(logRecord.ValueSpan);
            return true;
        }

        /// <inheritdoc/>
        public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            // This does not try to set ETag or Expiration
            srcValue.CopyTo(dstLogRecord.ValueSpan);
            return true;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            => throw new TsavoriteException("InitialUpdater not implemented for BDN");

        /// <inheritdoc/>
        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            => throw new TsavoriteException("CopyUpdater not implemented for BDN");

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref LogRecord logRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
        {
            // This does not try to set ETag or Expiration
            input.CopyTo(logRecord.ValueSpan);
            return true;
        }
    }
}