// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ClassAllocator = GenericAllocator<int, MyValue, StoreFunctions<int, MyValue, IntKeyComparer, DefaultRecordDisposer<int, MyValue>>>;
    using ClassStoreFunctions = StoreFunctions<int, MyValue, IntKeyComparer, DefaultRecordDisposer<int, MyValue>>;
    using StructAllocator = BlittableAllocator<KeyStruct, ValueStruct, StoreFunctions<KeyStruct, ValueStruct, KeyStruct.Comparer, DefaultRecordDisposer<KeyStruct, ValueStruct>>>;
    using StructStoreFunctions = StoreFunctions<KeyStruct, ValueStruct, KeyStruct.Comparer, DefaultRecordDisposer<KeyStruct, ValueStruct>>;

    [AllureNUnit]
    [TestFixture]
    internal class MiscTests : AllureTestBase
    {
        private TsavoriteKV<int, MyValue, ClassStoreFunctions, ClassAllocator> store;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "MiscTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "MiscTests.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 15,
                PageSize = 1L << 10
            }, StoreFunctions<int, MyValue>.Create(IntKeyComparer.Instance, null, () => new MyValueSerializer())
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
            objlog?.Dispose();
            objlog = null;
            OnTearDown();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void MixedTest1()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MixedFunctions>(new MixedFunctions());
            var bContext = session.BasicContext;

            int key = 8999998;
            var input1 = new MyInput { value = 23 };
            MyOutput output = new();

            _ = bContext.RMW(ref key, ref input1, Empty.Default);

            int key2 = 8999999;
            var input2 = new MyInput { value = 24 };
            _ = bContext.RMW(ref key2, ref input2, Empty.Default);

            _ = bContext.Read(ref key, ref input1, ref output, Empty.Default);
            ClassicAssert.AreEqual(input1.value, output.value.value);

            _ = bContext.Read(ref key2, ref input2, ref output, Empty.Default);
            ClassicAssert.AreEqual(input2.value, output.value.value);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void MixedTest2()
        {
            using var session = store.NewSession<MyInput, MyOutput, Empty, MixedFunctions>(new MixedFunctions());
            var bContext = session.BasicContext;

            for (int i = 0; i < 2000; i++)
            {
                var value = new MyValue { value = i };
                _ = bContext.Upsert(ref i, ref value, Empty.Default);
            }

            var key2 = 23;
            MyInput input = new();
            MyOutput g1 = new();
            var status = bContext.Read(ref key2, ref input, ref g1, Empty.Default);

            if (status.IsPending)
            {
                _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, _) = GetSinglePendingResult(outputs);
            }
            ClassicAssert.IsTrue(status.Found);

            ClassicAssert.AreEqual(23, g1.value.value);

            key2 = 99999;
            status = bContext.Read(ref key2, ref input, ref g1, Empty.Default);

            if (status.IsPending)
            {
                _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                (status, _) = GetSinglePendingResult(outputs);
            }
            ClassicAssert.IsFalse(status.Found);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ForceRCUAndRecover([Values(UpdateOp.Upsert, UpdateOp.Delete)] UpdateOp updateOp)
        {
            var copyOnWrite = new FunctionsCopyOnWrite();

            // FunctionsCopyOnWrite
            var log = default(IDevice);
            TsavoriteKV<KeyStruct, ValueStruct, StructStoreFunctions, StructAllocator> store = default;
            ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, FunctionsCopyOnWrite, StructStoreFunctions, StructAllocator> session = default;

            try
            {
                var checkpointDir = Path.Join(MethodTestDir, "checkpoints");
                log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "hlog1.log"), deleteOnClose: true);
                store = new(new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    MemorySize = 1L << 29,
                    CheckpointDir = checkpointDir
                }, StoreFunctions<KeyStruct, ValueStruct>.Create(KeyStruct.Comparer.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );

                session = store.NewSession<InputStruct, OutputStruct, Empty, FunctionsCopyOnWrite>(copyOnWrite);
                var bContext = session.BasicContext;

                var key = default(KeyStruct);
                var value = default(ValueStruct);
                var input = default(InputStruct);
                var output = default(OutputStruct);

                key = new KeyStruct() { kfield1 = 1, kfield2 = 2 };
                value = new ValueStruct() { vfield1 = 1000, vfield2 = 2000 };

                var status = bContext.Upsert(ref key, ref input, ref value, ref output, out RecordMetadata recordMetadata1);
                ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());

                // ConcurrentWriter and InPlaceUpater return false, so we create a new record.
                RecordMetadata recordMetadata2;
                value = new ValueStruct() { vfield1 = 1001, vfield2 = 2002 };
                if (updateOp == UpdateOp.Upsert)
                {
                    status = bContext.Upsert(ref key, ref input, ref value, ref output, out recordMetadata2);
                    ClassicAssert.AreEqual(1, copyOnWrite.ConcurrentWriterCallCount);
                    ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                }
                else
                {
                    status = bContext.RMW(ref key, ref input, ref output, out recordMetadata2);
                    ClassicAssert.AreEqual(1, copyOnWrite.InPlaceUpdaterCallCount);
                    ClassicAssert.IsTrue(status.Found && status.Record.CopyUpdated, status.ToString());
                }
                ClassicAssert.Greater(recordMetadata2.Address, recordMetadata1.Address);

                using (var iterator = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
                {
                    ClassicAssert.True(iterator.GetNext(out var info));    // We should only get the new record...
                    ClassicAssert.False(iterator.GetNext(out info));       // ... the old record was elided, so was Sealed and invalidated.
                }
                status = bContext.Read(ref key, ref output);
                ClassicAssert.IsTrue(status.Found, status.ToString());

                _ = store.TryInitiateFullCheckpoint(out Guid token, CheckpointType.Snapshot);
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

                session.Dispose();
                store.Dispose();

                store = new(new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    MemorySize = 1L << 29,
                    CheckpointDir = checkpointDir
                }, StoreFunctions<KeyStruct, ValueStruct>.Create(KeyStruct.Comparer.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );

                _ = store.Recover(token);
                session = store.NewSession<InputStruct, OutputStruct, Empty, FunctionsCopyOnWrite>(copyOnWrite);

                using (var iterator = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
                {
                    ClassicAssert.True(iterator.GetNext(out var info));    // We should only get one record...
                    ClassicAssert.False(iterator.GetNext(out info));       // ... the old record was Unsealed by Recovery, but remains invalid.
                }
                status = bContext.Read(ref key, ref output);
                ClassicAssert.IsTrue(status.Found, status.ToString());
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