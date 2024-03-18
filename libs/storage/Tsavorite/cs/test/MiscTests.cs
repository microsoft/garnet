// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    internal class MiscTests
    {
        private TsavoriteKV<int, MyValue> store;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(MethodTestDir + "/MiscTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(MethodTestDir + "/MiscTests.obj.log", deleteOnClose: true);

            store = new TsavoriteKV<int, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 10 },
                serializerSettings: new SerializerSettings<int, MyValue> { valueSerializer = () => new MyValueSerializer() }
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
        public void MixedTest1()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MixedFunctions>(new MixedFunctions());

            int key = 8999998;
            var input1 = new MyInput { value = 23 };
            MyOutput output = new();

            session.RMW(ref key, ref input1, Empty.Default, 0);

            int key2 = 8999999;
            var input2 = new MyInput { value = 24 };
            session.RMW(ref key2, ref input2, Empty.Default, 0);

            session.Read(ref key, ref input1, ref output, Empty.Default, 0);
            Assert.AreEqual(input1.value, output.value.value);

            session.Read(ref key2, ref input2, ref output, Empty.Default, 0);
            Assert.AreEqual(input2.value, output.value.value);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void MixedTest2()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MixedFunctions>(new MixedFunctions());

            for (int i = 0; i < 2000; i++)
            {
                var value = new MyValue { value = i };
                session.Upsert(ref i, ref value, Empty.Default, 0);
            }

            var key2 = 23;
            MyInput input = new();
            MyOutput g1 = new();
            var status = session.Read(ref key2, ref input, ref g1, Empty.Default, 0);

            if (status.IsPending)
            {
                session.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, _) = GetSinglePendingResult(outputs);
            }
            Assert.IsTrue(status.Found);

            Assert.AreEqual(23, g1.value.value);

            key2 = 99999;
            status = session.Read(ref key2, ref input, ref g1, Empty.Default, 0);

            if (status.IsPending)
            {
                session.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, _) = GetSinglePendingResult(outputs);
            }
            Assert.IsFalse(status.Found);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ForceRCUAndRecover([Values(UpdateOp.Upsert, UpdateOp.Delete)] UpdateOp updateOp)
        {
            var copyOnWrite = new FunctionsCopyOnWrite();

            // FunctionsCopyOnWrite
            var log = default(IDevice);
            TsavoriteKV<KeyStruct, ValueStruct> store = default;
            ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, FunctionsCopyOnWrite> session = default;

            try
            {
                var checkpointDir = MethodTestDir + $"/checkpoints";
                log = Devices.CreateLogDevice(MethodTestDir + "/hlog1.log", deleteOnClose: true);
                store = new TsavoriteKV<KeyStruct, ValueStruct>
                    (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 },
                    checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir },
                    concurrencyControlMode: ConcurrencyControlMode.None);

                session = store.NewSession<InputStruct, OutputStruct, Empty, FunctionsCopyOnWrite>(copyOnWrite);

                var key = default(KeyStruct);
                var value = default(ValueStruct);
                var input = default(InputStruct);
                var output = default(OutputStruct);

                key = new KeyStruct() { kfield1 = 1, kfield2 = 2 };
                value = new ValueStruct() { vfield1 = 1000, vfield2 = 2000 };

                var status = session.Upsert(ref key, ref input, ref value, ref output, out RecordMetadata recordMetadata1);
                Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());

                // ConcurrentWriter and InPlaceUpater return false, so we create a new record.
                RecordMetadata recordMetadata2;
                value = new ValueStruct() { vfield1 = 1001, vfield2 = 2002 };
                if (updateOp == UpdateOp.Upsert)
                {
                    status = session.Upsert(ref key, ref input, ref value, ref output, out recordMetadata2);
                    Assert.AreEqual(1, copyOnWrite.ConcurrentWriterCallCount);
                    Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                }
                else
                {
                    status = session.RMW(ref key, ref input, ref output, out recordMetadata2);
                    Assert.AreEqual(1, copyOnWrite.InPlaceUpdaterCallCount);
                    Assert.IsTrue(status.Found && status.Record.CopyUpdated, status.ToString());
                }
                Assert.Greater(recordMetadata2.Address, recordMetadata1.Address);

                using (var iterator = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
                {
                    Assert.True(iterator.GetNext(out var info));    // We should only get the new record...
                    Assert.False(iterator.GetNext(out info));       // ... the old record was elided, so was Sealed and invalidated.
                }
                status = session.Read(ref key, ref output);
                Assert.IsTrue(status.Found, status.ToString());

                store.TryInitiateFullCheckpoint(out Guid token, CheckpointType.Snapshot);
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

                session.Dispose();
                store.Dispose();

                store = new TsavoriteKV<KeyStruct, ValueStruct>
                    (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 },
                    checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir },
                    concurrencyControlMode: ConcurrencyControlMode.None);

                store.Recover(token);
                session = store.NewSession<InputStruct, OutputStruct, Empty, FunctionsCopyOnWrite>(copyOnWrite);

                using (var iterator = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
                {
                    Assert.True(iterator.GetNext(out var info));    // We should only get one record...
                    Assert.False(iterator.GetNext(out info));       // ... the old record was Unsealed by Recovery, but remains invalid.
                }
                status = session.Read(ref key, ref output);
                Assert.IsTrue(status.Found, status.ToString());
            }
            finally
            {
                session?.Dispose();
                store?.Dispose();
                log?.Dispose();
            }
        }
    }
}