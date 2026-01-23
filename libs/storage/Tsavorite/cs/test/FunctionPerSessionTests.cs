// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    // Must be in a separate block so the "using StructStoreFunctions" is the first line in its namespace declaration.
    public struct RefCountedValueStruct
    {
        public int ReferenceCount;
        public long Value;
    }
}

namespace Tsavorite.test
{
    using StructAllocator = BlittableAllocator<int, RefCountedValueStruct, StoreFunctions<int, RefCountedValueStruct, IntKeyComparer, DefaultRecordDisposer<int, RefCountedValueStruct>>>;
    using StructStoreFunctions = StoreFunctions<int, RefCountedValueStruct, IntKeyComparer, DefaultRecordDisposer<int, RefCountedValueStruct>>;

    public class RefCountedAdder : SessionFunctionsBase<int, RefCountedValueStruct, long, Empty, Empty>
    {
        public int InitialCount;
        public int InPlaceCount;
        public int CopyCount;

        public override bool InitialUpdater(ref int key, ref long input, ref RefCountedValueStruct value, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref InitialCount);

            value.Value = input;
            value.ReferenceCount = 1;
            return true;
        }

        public override bool InPlaceUpdater(ref int key, ref long input, ref RefCountedValueStruct value, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref InPlaceCount);

            value.Value = input;
            value.ReferenceCount++;
            return true;
        }

        public override bool CopyUpdater(ref int key, ref long input, ref RefCountedValueStruct oldValue, ref RefCountedValueStruct newValue, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref CopyCount);

            newValue.Value = input;
            newValue.ReferenceCount = oldValue.ReferenceCount + 1;
            return true;
        }
    }

    public class RefCountedRemover : SessionFunctionsBase<int, RefCountedValueStruct, Empty, Empty, Empty>
    {
        public int InitialCount;
        public int InPlaceCount;
        public int CopyCount;

        public override bool InitialUpdater(ref int key, ref Empty input, ref RefCountedValueStruct value, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref InitialCount);

            value.Value = 0;
            value.ReferenceCount = 0;
            return true;
        }

        public override bool InPlaceUpdater(ref int key, ref Empty input, ref RefCountedValueStruct value, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref InPlaceCount);

            if (value.ReferenceCount > 0)
                value.ReferenceCount--;

            return true;
        }

        public override bool CopyUpdater(ref int key, ref Empty input, ref RefCountedValueStruct oldValue, ref RefCountedValueStruct newValue, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref CopyCount);

            newValue.ReferenceCount = oldValue.ReferenceCount;
            if (newValue.ReferenceCount > 0)
                newValue.ReferenceCount--;
            newValue.Value = oldValue.Value;
            return true;
        }
    }

    public class RefCountedReader : SessionFunctionsBase<int, RefCountedValueStruct, Empty, RefCountedValueStruct, Empty>
    {
        public override bool SingleReader(ref int key, ref Empty input, ref RefCountedValueStruct value, ref RefCountedValueStruct dst, ref ReadInfo readInfo)
        {
            dst = value;
            return true;
        }

        public override bool ConcurrentReader(ref int key, ref Empty input, ref RefCountedValueStruct value, ref RefCountedValueStruct dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst = value;
            return true;
        }
    }

    [AllureNUnit]
    [TestFixture]
    public class FunctionPerSessionTests : AllureTestBase
    {
        private IDevice _log;
        private TsavoriteKV<int, RefCountedValueStruct, StructStoreFunctions, StructAllocator> store;
        private RefCountedAdder _adder;
        private RefCountedRemover _remover;
        private RefCountedReader _reader;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            _log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "FunctionPerSessionTests1.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = _log,
            }, StoreFunctions<int, RefCountedValueStruct>.Create(IntKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            _adder = new RefCountedAdder();
            _remover = new RefCountedRemover();
            _reader = new RefCountedReader();
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            _log?.Dispose();
            _log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void Should_create_multiple_sessions_with_different_callbacks()
        {
            using var adderSession = store.NewSession<long, Empty, Empty, RefCountedAdder>(_adder);
            using var removerSession = store.NewSession<Empty, Empty, Empty, RefCountedRemover>(_remover);
            using var readerSession = store.NewSession<Empty, RefCountedValueStruct, Empty, RefCountedReader>(_reader);
            var key = 101;
            var input = 1000L;

            _ = adderSession.BasicContext.RMW(ref key, ref input);
            _ = adderSession.BasicContext.RMW(ref key, ref input);
            _ = adderSession.BasicContext.RMW(ref key, ref input);

            ClassicAssert.AreEqual(1, _adder.InitialCount);
            ClassicAssert.AreEqual(2, _adder.InPlaceCount);

            var empty = default(Empty);
            _ = removerSession.BasicContext.RMW(ref key, ref empty);

            ClassicAssert.AreEqual(1, _remover.InPlaceCount);

            RefCountedValueStruct output = new();
            _ = readerSession.BasicContext.Read(ref key, ref output);

            ClassicAssert.AreEqual(2, output.ReferenceCount);
            ClassicAssert.AreEqual(1000L, output.Value);

            store.Log.FlushAndEvict(true);

            _ = removerSession.BasicContext.RMW(ref key, ref empty);
            _ = removerSession.BasicContext.CompletePending(wait: true);
            _ = readerSession.BasicContext.Read(ref key, ref empty, ref output);

            ClassicAssert.AreEqual(1, output.ReferenceCount);
            ClassicAssert.AreEqual(1000L, output.Value);
            ClassicAssert.AreEqual(1, _remover.CopyCount);
        }
    }
}