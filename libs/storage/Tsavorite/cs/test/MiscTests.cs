// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if LOGRECORD_TODO

using System;
using System.IO;
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

    [TestFixture]
    internal class MiscTests
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
            DeleteDirectory(MethodTestDir);
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

                // InPlaceWriter and InPlaceUpater return false, so we create a new record.
                RecordMetadata recordMetadata2;
                value = new ValueStruct() { vfield1 = 1001, vfield2 = 2002 };
                if (updateOp == UpdateOp.Upsert)
                {
                    status = bContext.Upsert(ref key, ref input, ref value, ref output, out recordMetadata2);
                    ClassicAssert.AreEqual(1, copyOnWrite.InPlaceWriterCallCount);
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

#endif // LOGRECORD_TODO