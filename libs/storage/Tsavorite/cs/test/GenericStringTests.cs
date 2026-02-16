// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using StringAllocator = GenericAllocator<string, string, StoreFunctions<string, string, StringKeyComparer, DefaultRecordDisposer<string, string>>>;
    using StringStoreFunctions = StoreFunctions<string, string, StringKeyComparer, DefaultRecordDisposer<string, string>>;

    [AllureNUnit]
    [TestFixture]
    internal class GenericStringTests : AllureTestBase
    {
        private TsavoriteKV<string, string, StringStoreFunctions, StringAllocator> store;
        private ClientSession<string, string, string, string, Empty, MyFuncs, StringStoreFunctions, StringAllocator> session;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;

            OnTearDown();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void StringBasicTest([Values] TestDeviceType deviceType)
        {
            string logfilename = Path.Join(MethodTestDir, "GenericStringTests" + deviceType.ToString() + ".log");
            string objlogfilename = Path.Join(MethodTestDir, "GenericStringTests" + deviceType.ToString() + ".obj.log");

            log = CreateTestDevice(deviceType, logfilename);
            objlog = CreateTestDevice(deviceType, objlogfilename);

            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 14,
                PageSize = 1L << 9,
                SegmentSize = 1L << 22
            }, StoreFunctions<string, string>.Create(StringKeyComparer.Instance, () => new StringBinaryObjectSerializer(), () => new StringBinaryObjectSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            session = store.NewSession<string, string, Empty, MyFuncs>(new MyFuncs());
            var bContext = session.BasicContext;

            const int totalRecords = 200;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = $"{i}";
                var _value = $"{i}"; ;
                _ = bContext.Upsert(ref _key, ref _value, Empty.Default);
            }
            _ = bContext.CompletePending(true);
            ClassicAssert.AreEqual(totalRecords, store.EntryCount);

            for (int i = 0; i < totalRecords; i++)
            {
                string input = default;
                string output = default;
                var key = $"{i}";
                var value = $"{i}";

                var status = bContext.Read(ref key, ref input, ref output, Empty.Default);
                if (status.IsPending)
                {
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(value, output);
            }
        }

        class MyFuncs : SimpleSimpleFunctions<string, string>
        {
            public override void ReadCompletionCallback(ref string key, ref string input, ref string output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.AreEqual(key, output);
            }
        }
    }
}