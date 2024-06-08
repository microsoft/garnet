﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    internal class GenericDiskDeleteTests
    {
        private TsavoriteKV<MyKey, MyValue> store;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> session;
        private BasicContext<MyKey, MyValue, MyInput, MyOutput, int, MyFunctionsDelete> bContext;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "GenericDiskDeleteTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "GenericDiskDeleteTests.obj.log"), deleteOnClose: true);

            store = new TsavoriteKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9 },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() });
            session = store.NewSession<MyInput, MyOutput, int, MyFunctionsDelete>(new MyFunctionsDelete());
            bContext = session.BasicContext;
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

        public void DiskDeleteBasicTest1()
        {
            const int totalRecords = 2000;
            var start = store.Log.TailAddress;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                bContext.Upsert(ref _key, ref _value, 0);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var input = new MyInput();
                var output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                if (bContext.Read(ref key1, ref input, ref output, 0).IsPending)
                {
                    bContext.CompletePending(true);
                }
                else
                {
                    Assert.AreEqual(value.value, output.value.value);
                }
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new MyKey { key = i };
                bContext.Delete(ref key1);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                var input = new MyInput();
                var output = new MyOutput();
                var key1 = new MyKey { key = i };

                var status = bContext.Read(ref key1, ref input, ref output, 1);

                if (status.IsPending)
                {
                    bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, _) = GetSinglePendingResult(outputs);
                }
                Assert.IsFalse(status.Found);
            }


            using var iter = store.Log.Scan(start, store.Log.TailAddress, ScanBufferingMode.SinglePageBuffering);
            int val = 0;
            while (iter.GetNext(out RecordInfo recordInfo, out MyKey key, out MyValue value))
            {
                if (recordInfo.Tombstone)
                    val++;
            }
            Assert.AreEqual(val, totalRecords);
        }


        [Test]
        [Category("TsavoriteKV")]
        public void DiskDeleteBasicTest2()
        {
            const int totalRecords = 2000;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                bContext.Upsert(ref _key, ref _value, 0);
            }

            var key100 = new MyKey { key = 100 };
            var value100 = new MyValue { value = 100 };
            var key200 = new MyKey { key = 200 };

            bContext.Delete(ref key100);

            var input = new MyInput { value = 1000 };
            var output = new MyOutput();
            var status = bContext.Read(ref key100, ref input, ref output, 1);
            Assert.IsFalse(status.Found, status.ToString());

            status = bContext.Upsert(ref key100, ref value100, 0);
            Assert.IsTrue(!status.Found, status.ToString());

            status = bContext.Read(ref key100, ref input, ref output, 0);
            Assert.IsTrue(status.Found, status.ToString());
            Assert.AreEqual(value100.value, output.value.value);

            bContext.Delete(ref key100);
            bContext.Delete(ref key200);

            // This RMW should create new initial value, since item is deleted
            status = bContext.RMW(ref key200, ref input, 1);
            Assert.IsFalse(status.Found);

            status = bContext.Read(ref key200, ref input, ref output, 0);
            Assert.IsTrue(status.Found, status.ToString());
            Assert.AreEqual(input.value, output.value.value);

            // Delete key 200 again
            bContext.Delete(ref key200);

            // Eliminate all records from memory
            for (int i = 201; i < 2000; i++)
            {
                var _key = new MyKey { key = i };
                var _value = new MyValue { value = i };
                bContext.Upsert(ref _key, ref _value, 0);
            }
            status = bContext.Read(ref key100, ref input, ref output, 1);
            Assert.IsTrue(status.IsPending);
            bContext.CompletePending(true);

            // This RMW should create new initial value, since item is deleted
            status = bContext.RMW(ref key200, ref input, 1);
            Assert.IsTrue(status.IsPending);
            bContext.CompletePending(true);

            status = bContext.Read(ref key200, ref input, ref output, 0);
            Assert.IsTrue(status.Found, status.ToString());
            Assert.AreEqual(input.value, output.value.value);
        }
    }
}