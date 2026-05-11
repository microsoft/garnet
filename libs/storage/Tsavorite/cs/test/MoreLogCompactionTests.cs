// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    [AllureNUnit]
    [TestFixture]
    internal class MoreLogCompactionTests : AllureTestBase
    {
        private TsavoriteKV<LongStoreFunctions, LongAllocator> store;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "MoreLogCompactionTests.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                LogMemorySize = 1L << 15,
                PageSize = 1L << 9
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            TestUtils.OnTearDown();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        [Category("Smoke")]

        public void DeleteCompactLookup([Values] CompactionType compactionType)
        {
            using var session = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;
            long compactUntil = 0;

            for (long key = 0; key < totalRecords; key++)
            {
                if (key == 1010)
                    compactUntil = store.Log.TailAddress;
                _ = bContext.Upsert(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), SpanByte.FromPinnedVariable(ref key));
            }

            for (long key = 0; key < totalRecords / 2; key++)
                _ = bContext.Delete(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)));

            compactUntil = session.Compact(compactUntil, compactionType);

            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            using var session2 = store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext2 = session2.BasicContext;

            // Verify records by reading
            for (long key = 0; key < totalRecords; key++)
            {
                (var status, var output) = bContext2.Read(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)));
                if (status.IsPending)
                {
                    _ = bContext2.CompletePendingWithOutputs(out var completedOutputs, true);
                    ClassicAssert.IsTrue(completedOutputs.Next());
                    (status, output) = (completedOutputs.Current.Status, completedOutputs.Current.Output);
                    ClassicAssert.IsFalse(completedOutputs.Next());
                }

                if (key < totalRecords / 2)
                {
                    ClassicAssert.IsTrue(status.NotFound);
                }
                else
                {
                    ClassicAssert.IsTrue(status.Found);
                    ClassicAssert.AreEqual(key, output);
                }
            }
        }
    }
}