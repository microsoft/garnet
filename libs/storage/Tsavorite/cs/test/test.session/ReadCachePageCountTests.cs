// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.ReadCacheTests
{
    using StructAllocator = SpanByteAllocator<StoreFunctions<KeyStruct.Comparer, SpanByteRecordTriggers>>;
    using StructStoreFunctions = StoreFunctions<KeyStruct.Comparer, SpanByteRecordTriggers>;

    /// <summary>
    /// A configured read cache page count caps the read cache at that many pages, with or without a read cache memory size,
    /// as the main log's page count caps the main log.
    /// </summary>
    [TestFixture]
    public class ReadCachePageCountTests : TestBase
    {
        const int ReadCachePageCount = 4;

        // Enough records to fill the read cache several times over.
        const int NumKeys = 2000;

        private TsavoriteKV<StructStoreFunctions, StructAllocator> store;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ReadCachePageCountTests.log"), deleteOnClose: true);
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            OnTearDown();
        }

        private void CreateStore(long readCacheMemorySize)
            => store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize,
                ReadCacheEnabled = true,
                ReadCacheMemorySize = readCacheMemorySize,
                ReadCachePageSize = MinKvLogPageSize,
                ReadCachePageCount = ReadCachePageCount
            }, StoreFunctions.Create(new KeyStruct.Comparer(), SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

        /// <summary>A read cache sized only by its page count is created, and holds at most that many pages.</summary>
        [Test]
        [Category("TsavoriteKV"), Category("Smoke")]
        public void PageCountWithoutMemorySizeCapsTheReadCache()
        {
            CreateStore(readCacheMemorySize: 0);
            AssertReadCacheStaysWithinItsPageCount();
        }

        /// <summary>A read cache page count below what the read cache memory size allows caps the read cache at that count.</summary>
        [Test]
        [Category("TsavoriteKV"), Category("Smoke")]
        public void PageCountBelowMemorySizeCapsTheReadCache()
        {
            CreateStore(readCacheMemorySize: 4L * ReadCachePageCount * MinKvLogPageSize);
            AssertReadCacheStaysWithinItsPageCount();
        }

        private void AssertReadCacheStaysWithinItsPageCount()
        {
            ClassicAssert.AreEqual(ReadCachePageCount, store.ReadCache.MaxAllocatedPageCount);

            using var session = store.NewSession<KeyStruct, InputStruct, OutputStruct, Empty, Functions>(new Functions());
            var bContext = session.BasicContext;

            for (var i = 0; i < NumKeys; i++)
            {
                var key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                _ = bContext.Upsert(key, SpanByte.FromPinnedVariable(ref value), Empty.Default);
            }
            _ = bContext.CompletePending(true);
            store.Log.FlushAndEvict(wait: true);

            // Each read is served from disk and copies its record into the read cache.
            var initialHeadAddress = store.ReadCache.HeadAddress;
            var capBytes = ReadCachePageCount * store.ReadCache.PageSizeBytes;
            InputStruct input = default;
            for (var i = 0; i < NumKeys; i++)
            {
                OutputStruct output = default;
                var key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var status = bContext.Read(key, ref input, ref output, Empty.Default);
                ClassicAssert.IsTrue(status.IsPending, $"key {i} must be read from disk");
                _ = bContext.CompletePending(true);

                ClassicAssert.LessOrEqual(store.ReadCache.TailAddress - store.ReadCache.HeadAddress, capBytes,
                    $"The read cache exceeded its {ReadCachePageCount}-page cap after reading key {i}");
            }
            ClassicAssert.Greater(store.ReadCache.HeadAddress, initialHeadAddress, "The read cache must have evicted pages to stay within its cap");

            // The most recently read record is served from the read cache.
            OutputStruct lastOutput = default;
            var lastKey = new KeyStruct { kfield1 = NumKeys - 1, kfield2 = NumKeys };
            var lastStatus = bContext.Read(lastKey, ref input, ref lastOutput, Empty.Default);
            ClassicAssert.IsTrue(lastStatus.Found && !lastStatus.IsPending, lastStatus.ToString());
            ClassicAssert.AreEqual(NumKeys - 1, lastOutput.value.vfield1);
        }
    }
}