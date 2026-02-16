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
    using LongAllocator = BlittableAllocator<long, long, StoreFunctions<long, long, LongKeyComparer, DefaultRecordDisposer<long, long>>>;
    using LongStoreFunctions = StoreFunctions<long, long, LongKeyComparer, DefaultRecordDisposer<long, long>>;

    [AllureNUnit]
    [TestFixture]
    internal class MoreLogCompactionTests : AllureTestBase
    {
        private TsavoriteKV<long, long, LongStoreFunctions, LongAllocator> store;
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
                MemorySize = 1L << 15,
                PageSize = 1L << 9
            }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
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
            using var session = store.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
            var bContext = session.BasicContext;

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1010)
                    compactUntil = store.Log.TailAddress;
                _ = bContext.Upsert(i, i);
            }

            for (int i = 0; i < totalRecords / 2; i++)
                _ = bContext.Delete(i);

            compactUntil = session.Compact(compactUntil, compactionType);

            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            using var session2 = store.NewSession<long, long, Empty, SimpleSimpleFunctions<long, long>>(new SimpleSimpleFunctions<long, long>());
            var bContext2 = session2.BasicContext;

            // Verify records by reading
            for (int i = 0; i < totalRecords; i++)
            {
                (var status, var output) = bContext2.Read(i);
                if (status.IsPending)
                {
                    _ = bContext2.CompletePendingWithOutputs(out var completedOutputs, true);
                    ClassicAssert.IsTrue(completedOutputs.Next());
                    (status, output) = (completedOutputs.Current.Status, completedOutputs.Current.Output);
                    ClassicAssert.IsFalse(completedOutputs.Next());
                }

                if (i < totalRecords / 2)
                {
                    ClassicAssert.IsTrue(status.NotFound);
                }
                else
                {
                    ClassicAssert.IsTrue(status.Found);
                    ClassicAssert.AreEqual(i, output);
                }
            }
        }
    }
}