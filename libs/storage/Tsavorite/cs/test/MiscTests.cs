// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using StructAllocator = SpanByteAllocator<StoreFunctions<KeyStruct.Comparer, SpanByteRecordDisposer>>;
    using StructStoreFunctions = StoreFunctions<KeyStruct.Comparer, SpanByteRecordDisposer>;

    [TestFixture]
    internal class MiscTests
    {
        private TsavoriteKV<StructStoreFunctions, StructAllocator> store;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "MiscTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "MiscTests.obj.log"), deleteOnClose: true);
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
            ClientSession<InputStruct, OutputStruct, Empty, FunctionsCopyOnWrite, StructStoreFunctions, StructAllocator> session = default;

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
                }, StoreFunctions.Create(KeyStruct.Comparer.Instance, SpanByteRecordDisposer.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );

                session = store.NewSession<InputStruct, OutputStruct, Empty, FunctionsCopyOnWrite>(copyOnWrite);
                var bContext = session.BasicContext;

                var key = new KeyStruct() { kfield1 = 1, kfield2 = 2 };
                var value = new ValueStruct() { vfield1 = 1000, vfield2 = 2000 };
                var input = default(InputStruct);
                var output = default(OutputStruct);

                var upsertOptions = new UpsertOptions();
                var status = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), ref input, SpanByte.FromPinnedVariable(ref value), ref output, ref upsertOptions, out RecordMetadata recordMetadata1);
                ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());

                // InPlaceWriter and InPlaceUpater return false, so we create a new record.
                RecordMetadata recordMetadata2;
                value = new ValueStruct() { vfield1 = 1001, vfield2 = 2002 };
                if (updateOp == UpdateOp.Upsert)
                {
                    status = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), ref input, SpanByte.FromPinnedVariable(ref value), ref output, ref upsertOptions, out recordMetadata2);
                    ClassicAssert.AreEqual(1, copyOnWrite.InPlaceWriterCallCount);
                    ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                }
                else
                {
                    status = bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref input, ref output, out recordMetadata2);
                    ClassicAssert.AreEqual(1, copyOnWrite.InPlaceUpdaterCallCount);
                    ClassicAssert.IsTrue(status.Found && status.Record.CopyUpdated, status.ToString());
                }
                ClassicAssert.Greater(recordMetadata2.Address, recordMetadata1.Address);

                using (var iterator = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
                {
                    ClassicAssert.True(iterator.GetNext());     // We should only get the new record...
                    ClassicAssert.False(iterator.GetNext());    // ... the old record was elided, so was Sealed and invalidated.
                }
                status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                ClassicAssert.IsTrue(status.Found, status.ToString());

                _ = store.TryInitiateFullCheckpoint(out Guid token, CheckpointType.Snapshot);
                store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

                session.Dispose();
                session = null;
                store.Dispose();
                store = null;

                store = new(new()
                {
                    IndexSize = 1L << 13,
                    LogDevice = log,
                    MemorySize = 1L << 29,
                    CheckpointDir = checkpointDir
                }, StoreFunctions.Create(KeyStruct.Comparer.Instance, SpanByteRecordDisposer.Instance)
                    , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
                );

                _ = store.Recover(token);
                session = store.NewSession<InputStruct, OutputStruct, Empty, FunctionsCopyOnWrite>(copyOnWrite);
                bContext = session.BasicContext;

                using (var iterator = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
                {
                    ClassicAssert.True(iterator.GetNext());     // We should only get one record...
                    ClassicAssert.False(iterator.GetNext());    // ... the old record was Unsealed by Recovery, but remains invalid.
                }
                status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref output);
                ClassicAssert.IsTrue(status.Found, status.ToString());
            }
            finally
            {
                session?.Dispose();
            }
        }
    }
}