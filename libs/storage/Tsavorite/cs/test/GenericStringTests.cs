// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    internal class GenericStringTests
    {
        private TsavoriteKV<string, string> store;
        private ClientSession<string, string, string, string, Empty, MyFuncs> session;
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

            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void StringBasicTest([Values] DeviceType deviceType)
        {
            string logfilename = Path.Join(MethodTestDir, "GenericStringTests" + deviceType.ToString() + ".log");
            string objlogfilename = Path.Join(MethodTestDir, "GenericStringTests" + deviceType.ToString() + ".obj.log");

            log = CreateTestDevice(deviceType, logfilename);
            objlog = CreateTestDevice(deviceType, objlogfilename);

            store = new TsavoriteKV<string, string>(
                    1L << 20, // size of hash table in #cache lines; 64 bytes per cache line
                    new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9, SegmentSizeBits = 22 } // log device
                    );

            session = store.NewSession<string, string, Empty, MyFuncs>(new MyFuncs());

            const int totalRecords = 200;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = $"{i}";
                var _value = $"{i}"; ;
                session.Upsert(ref _key, ref _value, Empty.Default);
            }
            session.CompletePending(true);
            Assert.AreEqual(totalRecords, store.EntryCount);

            for (int i = 0; i < totalRecords; i++)
            {
                string input = default;
                string output = default;
                var key = $"{i}";
                var value = $"{i}";

                var status = session.Read(ref key, ref input, ref output, Empty.Default);
                if (status.IsPending)
                {
                    session.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }
                Assert.IsTrue(status.Found);
                Assert.AreEqual(value, output);
            }
        }

        class MyFuncs : SimpleFunctions<string, string>
        {
            public override void ReadCompletionCallback(ref string key, ref string input, ref string output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(status.Found);
                Assert.AreEqual(key, output);
            }
        }
    }
}