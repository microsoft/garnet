// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    internal class ObjectTests
    {
        private TsavoriteKV<MyKey, MyValue> store;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(MethodTestDir + "/ObjectTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(MethodTestDir + "/ObjectTests.obj.log", deleteOnClose: true);

            store = new TsavoriteKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 10 },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );
        }

        [TearDown]
        public void TearDown()
        {
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
        public void ObjectInMemWriteRead()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());

            MyKey key1 = new() { key = 9999999 };
            MyValue value = new() { value = 23 };

            MyInput input = null;
            MyOutput output = new();

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            Assert.AreEqual(value.value, output.value.value);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjectInMemWriteRead2()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());

            MyKey key1 = new() { key = 8999998 };
            MyInput input1 = new() { value = 23 };
            MyOutput output = new();

            session.RMW(ref key1, ref input1, Empty.Default, 0);

            MyKey key2 = new() { key = 8999999 };
            MyInput input2 = new() { value = 24 };
            session.RMW(ref key2, ref input2, Empty.Default, 0);

            session.Read(ref key1, ref input1, ref output, Empty.Default, 0);

            Assert.AreEqual(input1.value, output.value.value);

            session.Read(ref key2, ref input2, ref output, Empty.Default, 0);
            Assert.AreEqual(input2.value, output.value.value);

        }


        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void ObjectDiskWriteRead()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };
                session.Upsert(ref key, ref value, Empty.Default, 0);
                // store.ShiftReadOnlyAddress(store.LogTailAddress);
            }

            MyKey key2 = new() { key = 23 };
            MyInput input = new();
            MyOutput g1 = new();
            var status = session.Read(ref key2, ref input, ref g1, Empty.Default, 0);

            if (status.IsPending)
            {
                session.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, g1) = GetSinglePendingResult(outputs);
            }

            Assert.IsTrue(status.Found);
            Assert.AreEqual(23, g1.value.value);

            key2 = new MyKey { key = 99999 };
            status = session.Read(ref key2, ref input, ref g1, Empty.Default, 0);

            if (status.IsPending)
            {
                session.CompletePending(true);
            }
            else
            {
                Assert.IsFalse(status.Found);
            }

            // Update first 100 using RMW from storage
            for (int i = 0; i < 100; i++)
            {
                var key1 = new MyKey { key = i };
                input = new MyInput { value = 1 };
                status = session.RMW(ref key1, ref input, Empty.Default, 0);
                if (status.IsPending)
                    session.CompletePending(true);
            }

            for (int i = 0; i < 2000; i++)
            {
                var output = new MyOutput();
                var key1 = new MyKey { key = i };
                var value = new MyValue { value = i };

                if (session.Read(ref key1, ref input, ref output, Empty.Default, 0).IsPending)
                {
                    session.CompletePending(true);
                }
                else
                {
                    if (i < 100)
                    {
                        Assert.AreEqual(value.value + 1, output.value.value);
                        Assert.AreEqual(value.value + 1, output.value.value);
                    }
                    else
                    {
                        Assert.AreEqual(value.value, output.value.value);
                        Assert.AreEqual(value.value, output.value.value);
                    }
                }
            }

        }

        [Test]
        [Category("TsavoriteKV")]
        public async Task ReadAsyncObjectDiskWriteRead()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(new MyFunctions());

            for (int i = 0; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };

                var r = await session.UpsertAsync(ref key, ref value);
                while (r.Status.IsPending)
                    r = await r.CompleteAsync(); // test async version of Upsert completion
            }

            var key1 = new MyKey { key = 1989 };
            var input = new MyInput();
            var readResult = await session.ReadAsync(ref key1, ref input, Empty.Default);
            var result = readResult.Complete();
            Assert.IsTrue(result.status.Found);
            Assert.AreEqual(1989, result.output.value.value);

            var key2 = new MyKey { key = 23 };
            readResult = await session.ReadAsync(ref key2, ref input, Empty.Default);
            result = readResult.Complete();

            Assert.IsTrue(result.status.Found);
            Assert.AreEqual(23, result.output.value.value);

            var key3 = new MyKey { key = 9999 };
            readResult = await session.ReadAsync(ref key3, ref input, Empty.Default);
            result = readResult.Complete();

            Assert.IsFalse(result.status.Found);

            // Update last 100 using RMW in memory
            for (int i = 1900; i < 2000; i++)
            {
                var key = new MyKey { key = i };
                input = new MyInput { value = 1 };
                var r = await session.RMWAsync(ref key, ref input, Empty.Default);
                while (r.Status.IsPending)
                {
                    r = await r.CompleteAsync(); // test async version of RMW completion
                }
            }

            // Update first 100 using RMW from storage
            for (int i = 0; i < 100; i++)
            {
                var key = new MyKey { key = i };
                input = new MyInput { value = 1 };
                (await session.RMWAsync(ref key, ref input, Empty.Default)).Complete();
            }

            for (int i = 0; i < 2000; i++)
            {
                var output = new MyOutput();
                var key = new MyKey { key = i };
                var value = new MyValue { value = i };

                readResult = await session.ReadAsync(ref key, ref input, Empty.Default);
                result = readResult.Complete();
                Assert.IsTrue(result.status.Found);
                if (i < 100 || i >= 1900)
                    Assert.AreEqual(value.value + 1, result.output.value.value);
                else
                    Assert.AreEqual(value.value, result.output.value.value);
            }
        }
    }
}