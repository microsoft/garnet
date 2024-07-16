// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    // Must be in a separate block so the "using StructStoreFunctions" is the first line in its namespace declaration.
    public struct RefCountedValue
    {
        public int ReferenceCount;
        public long Value;
    }
}

namespace Tsavorite.test
{
    using StructStoreFunctions = StoreFunctions<int, RefCountedValue, IntKeyComparer, DefaultRecordDisposer<int, RefCountedValue>>;
    using StructAllocator = BlittableAllocator<int, RefCountedValue, StoreFunctions<int, RefCountedValue, IntKeyComparer, DefaultRecordDisposer<int, RefCountedValue>>>;

    public class RefCountedAdder : SessionFunctionsBase<int, RefCountedValue, long, Empty, Empty>
    {
        public int InitialCount;
        public int InPlaceCount;
        public int CopyCount;

        public override bool InitialUpdater(ref int key, ref long input, ref RefCountedValue value, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref InitialCount);

            value.Value = input;
            value.ReferenceCount = 1;
            return true;
        }

        public override bool InPlaceUpdater(ref int key, ref long input, ref RefCountedValue value, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref InPlaceCount);

            value.Value = input;
            value.ReferenceCount++;
            return true;
        }

        public override bool CopyUpdater(ref int key, ref long input, ref RefCountedValue oldValue, ref RefCountedValue newValue, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref CopyCount);

            newValue.Value = input;
            newValue.ReferenceCount = oldValue.ReferenceCount + 1;
            return true;
        }
    }

    public class RefCountedRemover : SessionFunctionsBase<int, RefCountedValue, Empty, Empty, Empty>
    {
        public int InitialCount;
        public int InPlaceCount;
        public int CopyCount;

        public override bool InitialUpdater(ref int key, ref Empty input, ref RefCountedValue value, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref InitialCount);

            value.Value = 0;
            value.ReferenceCount = 0;
            return true;
        }

        public override bool InPlaceUpdater(ref int key, ref Empty input, ref RefCountedValue value, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref InPlaceCount);

            if (value.ReferenceCount > 0)
                value.ReferenceCount--;

            return true;
        }

        public override bool CopyUpdater(ref int key, ref Empty input, ref RefCountedValue oldValue, ref RefCountedValue newValue, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            _ = Interlocked.Increment(ref CopyCount);

            newValue.ReferenceCount = oldValue.ReferenceCount;
            if (newValue.ReferenceCount > 0)
                newValue.ReferenceCount--;
            newValue.Value = oldValue.Value;
            return true;
        }
    }

    public class RefCountedReader : SessionFunctionsBase<int, RefCountedValue, Empty, RefCountedValue, Empty>
    {
        public override bool SingleReader(ref int key, ref Empty input, ref RefCountedValue value, ref RefCountedValue dst, ref ReadInfo readInfo)
        {
            dst = value;
            return true;
        }

        public override bool ConcurrentReader(ref int key, ref Empty input, ref RefCountedValue value, ref RefCountedValue dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst = value;
            return true;
        }
    }

    [TestFixture]
    public class FunctionPerSessionTests
    {
        private IDevice _log;
        private TsavoriteKV<int, RefCountedValue, StructStoreFunctions, StructAllocator> store;
        private RefCountedAdder _adder;
        private RefCountedRemover _remover;
        private RefCountedReader _reader;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            _log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "FunctionPerSessionTests1.log"), deleteOnClose: true);

            store = new (new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = _log,
                }, StoreFunctions<int, RefCountedValue>.Create(IntKeyComparer.Instance)
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
            using var readerSession = store.NewSession<Empty, RefCountedValue, Empty, RefCountedReader>(_reader);
            var key = 101;
            var input = 1000L;

            _ = adderSession.BasicContext.RMW(ref key, ref input);
            _ = adderSession.BasicContext.RMW(ref key, ref input);
            _ = adderSession.BasicContext.RMW(ref key, ref input);

            Assert.AreEqual(1, _adder.InitialCount);
            Assert.AreEqual(2, _adder.InPlaceCount);

            var empty = default(Empty);
            _ = removerSession.BasicContext.RMW(ref key, ref empty);

            Assert.AreEqual(1, _remover.InPlaceCount);

            RefCountedValue output = new();
            _ = readerSession.BasicContext.Read(ref key, ref output);

            Assert.AreEqual(2, output.ReferenceCount);
            Assert.AreEqual(1000L, output.Value);

            store.Log.FlushAndEvict(true);

            _ = removerSession.BasicContext.RMW(ref key, ref empty);
            _ = removerSession.BasicContext.CompletePending(wait: true);
            _ = readerSession.BasicContext.Read(ref key, ref empty, ref output);

            Assert.AreEqual(1, output.ReferenceCount);
            Assert.AreEqual(1000L, output.Value);
            Assert.AreEqual(1, _remover.CopyCount);
        }
    }
}