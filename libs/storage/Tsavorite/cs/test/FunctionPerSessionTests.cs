// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    // Must be in a separate block so the "using StructStoreFunctions" is the first line in its namespace declaration.
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct RefCountedValueStruct
    {
        public static unsafe int Size => sizeof(RefCountedValueStruct);
        public int ReferenceCount;
        public long Value;
    }
}

namespace Tsavorite.test
{
    using StructAllocator = SpanByteAllocator<StoreFunctions<IntKeyComparer, SpanByteRecordDisposer>>;
    using StructStoreFunctions = StoreFunctions<IntKeyComparer, SpanByteRecordDisposer>;

    public class RefCountedAdder : SessionFunctionsBase<long, Empty, Empty>
    {
        public int InitialCount;
        public int InPlaceCount;
        public int CopyCount;

        public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref long input, ref Empty output, ref RMWInfo rmwInfo)
        {
            _ = Interlocked.Increment(ref InitialCount);

            ref var value = ref logRecord.ValueSpan.AsRef<RefCountedValueStruct>();
            value.Value = input;
            value.ReferenceCount = 1;
            return true;
        }

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref long input, ref Empty output, ref RMWInfo rmwInfo)
        {
            _ = Interlocked.Increment(ref InPlaceCount);

            ref var value = ref logRecord.ValueSpan.AsRef<RefCountedValueStruct>();
            value.Value = input;
            value.ReferenceCount++;
            return true;
        }

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref long input, ref Empty output, ref RMWInfo rmwInfo)
        {
            _ = Interlocked.Increment(ref CopyCount);

            ref var oldValue = ref srcLogRecord.ValueSpan.AsRef<RefCountedValueStruct>();
            ref var newValue = ref dstLogRecord.ValueSpan.AsRef<RefCountedValueStruct>();
            newValue.Value = input;
            newValue.ReferenceCount = oldValue.ReferenceCount + 1;
            return true;
        }

        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref long input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = RefCountedValueStruct.Size, ValueIsObject = false };
        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref long input)
            => new() { KeySize = key.Length, ValueSize = RefCountedValueStruct.Size, ValueIsObject = false };
    }

    public class RefCountedRemover : SessionFunctionsBase<Empty, Empty, Empty>
    {
        public int InitialCount;
        public int InPlaceCount;
        public int CopyCount;

        public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Empty input, ref Empty output, ref RMWInfo rmwInfo)
        {
            _ = Interlocked.Increment(ref InitialCount);

            ref var value = ref logRecord.ValueSpan.AsRef<RefCountedValueStruct>();
            value.Value = 0;
            value.ReferenceCount = 0;
            return true;
        }

        public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref Empty input, ref Empty output, ref RMWInfo rmwInfo)
        {
            _ = Interlocked.Increment(ref InPlaceCount);

            ref var value = ref logRecord.ValueSpan.AsRef<RefCountedValueStruct>();
            if (value.ReferenceCount > 0)
                value.ReferenceCount--;

            return true;
        }

        public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref Empty input, ref Empty output, ref RMWInfo rmwInfo)
        {
            _ = Interlocked.Increment(ref CopyCount);

            ref var oldValue = ref srcLogRecord.ValueSpan.AsRef<RefCountedValueStruct>();
            ref var newValue = ref dstLogRecord.ValueSpan.AsRef<RefCountedValueStruct>();

            newValue.ReferenceCount = oldValue.ReferenceCount;
            if (newValue.ReferenceCount > 0)
                newValue.ReferenceCount--;
            newValue.Value = oldValue.Value;
            return true;
        }

        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref Empty input)
            => new() { KeySize = srcLogRecord.Key.Length, ValueSize = RefCountedValueStruct.Size, ValueIsObject = false };
        /// <inheritdoc/>
        public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref Empty input)
            => new() { KeySize = key.Length, ValueSize = RefCountedValueStruct.Size, ValueIsObject = false };
    }

    public class RefCountedReader : SessionFunctionsBase<Empty, RefCountedValueStruct, Empty>
    {
        public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref Empty input, ref RefCountedValueStruct output, ref ReadInfo readInfo)
        {
            output = srcLogRecord.ValueSpan.AsRef<RefCountedValueStruct>();
            return true;
        }
    }

    [TestFixture]
    public class FunctionPerSessionTests
    {
        private IDevice log;
        private TsavoriteKV<StructStoreFunctions, StructAllocator> store;
        private RefCountedAdder adder;
        private RefCountedRemover remover;
        private RefCountedReader reader;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "FunctionPerSessionTests1.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
            }, StoreFunctions.Create(IntKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            adder = new RefCountedAdder();
            remover = new RefCountedRemover();
            reader = new RefCountedReader();
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void Should_create_multiple_sessions_with_different_callbacks()
        {
            using var adderSession = store.NewSession<long, Empty, Empty, RefCountedAdder>(adder);
            using var removerSession = store.NewSession<Empty, Empty, Empty, RefCountedRemover>(remover);
            using var readerSession = store.NewSession<Empty, RefCountedValueStruct, Empty, RefCountedReader>(reader);
            var key = 101;
            var input = 1000L;

            _ = adderSession.BasicContext.RMW(SpanByte.FromPinnedVariable(ref key), ref input);
            _ = adderSession.BasicContext.RMW(SpanByte.FromPinnedVariable(ref key), ref input);
            _ = adderSession.BasicContext.RMW(SpanByte.FromPinnedVariable(ref key), ref input);

            ClassicAssert.AreEqual(1, adder.InitialCount);
            ClassicAssert.AreEqual(2, adder.InPlaceCount);

            var empty = default(Empty);
            _ = removerSession.BasicContext.RMW(SpanByte.FromPinnedVariable(ref key), ref empty);

            ClassicAssert.AreEqual(1, remover.InPlaceCount);

            RefCountedValueStruct output = new();
            _ = readerSession.BasicContext.Read(SpanByte.FromPinnedVariable(ref key), ref output);

            ClassicAssert.AreEqual(2, output.ReferenceCount);
            ClassicAssert.AreEqual(1000L, output.Value);

            store.Log.FlushAndEvict(true);

            _ = removerSession.BasicContext.RMW(SpanByte.FromPinnedVariable(ref key), ref empty);
            _ = removerSession.BasicContext.CompletePending(wait: true);
            _ = readerSession.BasicContext.Read(SpanByte.FromPinnedVariable(ref key), ref empty, ref output);

            ClassicAssert.AreEqual(1, output.ReferenceCount);
            ClassicAssert.AreEqual(1000L, output.Value);
            ClassicAssert.AreEqual(1, remover.CopyCount);
        }
    }
}