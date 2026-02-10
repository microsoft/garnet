// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class GarnetObjectTests
    {
        TsavoriteKV<StoreFunctions, StoreAllocator> store;
        IDevice logDevice, objectLogDevice;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            CreateStore();
        }

        [TearDown]
        public void TearDown()
        {
            store.Dispose();
            logDevice.Dispose();
            objectLogDevice.Dispose();
            logDevice = objectLogDevice = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        public void WriteRead()
        {
            using var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, SimpleGarnetObjectSessionFunctions>(new SimpleGarnetObjectSessionFunctions());
            var bContext = session.BasicContext;

            var key = new ReadOnlySpan<byte>([0]);
            var obj = new SortedSetObject();

            _ = bContext.Upsert(key, obj);

            IGarnetObject output = null;
            var status = bContext.Read(key, ref output);

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(obj, output);
        }

        const int keyNum = 0;

        [Test]
        public async Task WriteCheckpointRead()
        {
            var obj = new SortedSetObject();

            LocalWrite();
            _ = await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);
            store.Dispose();
            CreateStore();
            _ = store.Recover();
            LocalRead();

            void LocalWrite()
            {
                using var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
                var bContext = session.BasicContext;

                var key = new ReadOnlySpan<byte>([keyNum]);
                obj.Add([15], 10);

                _ = bContext.Upsert(key, obj);
            }

            void LocalRead()
            {
                using var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
                var bContext = session.BasicContext;

                IGarnetObject output = null;
                var key = new ReadOnlySpan<byte>([keyNum]);
                var status = bContext.Read(key, ref output);

                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.IsTrue(obj.Equals((SortedSetObject)output));
            }
        }

        [Test]
        public async Task WriteCheckpointCopyUpdate()
        {
            IGarnetObject obj = new SortedSetObject();

            LocalWrite();
            _ = await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);
            store.Dispose();
            CreateStore();
            _ = store.Recover();
            LocalRead();

            void LocalWrite()
            {
                using var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
                var bContext = session.BasicContext;

                var key = new ReadOnlySpan<byte>([keyNum]);
                ((SortedSetObject)obj).Add([15], 10);

                _ = bContext.Upsert(key, obj);
                store.Log.Flush(true);
                _ = bContext.RMW(key, ref obj);
            }

            void LocalRead()
            {
                using var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
                var bContext = session.BasicContext;

                IGarnetObject output = null;
                var key = new ReadOnlySpan<byte>([keyNum]);
                var status = bContext.Read(key, ref output);

                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.IsTrue(((SortedSetObject)obj).Equals((SortedSetObject)output));
            }
        }

        private class MyFunctions : SessionFunctionsBase<IGarnetObject, IGarnetObject, Empty>
        {
            public MyFunctions()
            { }

            public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref IGarnetObject input, ref IGarnetObject output, ref ReadInfo readInfo)
            {
                output = (IGarnetObject)srcLogRecord.ValueObject;
                return true;
            }

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref IGarnetObject input, ref IGarnetObject output, ref RMWInfo rmwInfo)
            {
                Assert.That(dstLogRecord.Info.ValueIsObject, Is.True);
                dstLogRecord.TrySetValueObject(srcLogRecord.ValueObject.Clone());
                return true;
            }

            public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref IGarnetObject input)
                => new() { KeySize = srcLogRecord.Key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
            public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref IGarnetObject input)
                => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
            public override RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref IGarnetObject input)
                => new() { KeySize = key.Length, ValueSize = value.Length, ValueIsObject = false };
            public override RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, IHeapObject value, ref IGarnetObject input)
                => new() { KeySize = key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        }

        private void CreateStore()
        {
            logDevice ??= Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog.log");
            objectLogDevice ??= Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog.obj.log");

            var kvSettings = new KVSettings
            {
                IndexSize = 1L << 13,
                LogDevice = logDevice,
                ObjectLogDevice = objectLogDevice,
                CheckpointDir = TestUtils.MethodTestDir
            };

            store = new(kvSettings
                , Tsavorite.core.StoreFunctions.Create(new SpanByteComparer(), () => new MyGarnetObjectSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }
    }

    /// <summary>
    /// Serializer for IGarnetObject
    /// </summary>
    sealed class MyGarnetObjectSerializer : BinaryObjectSerializer<IHeapObject>
    {
        /// <inheritdoc />
        public override void Deserialize(out IHeapObject obj)
        {
            var type = (GarnetObjectType)reader.ReadByte();
            obj = type switch
            {
                GarnetObjectType.SortedSet => new SortedSetObject(reader),
                GarnetObjectType.List => new ListObject(reader),
                GarnetObjectType.Hash => new HashObject(reader),
                GarnetObjectType.Set => new SetObject(reader),
                _ => null,
            };
        }

        /// <inheritdoc />
        public override void Serialize(IHeapObject obj)
        {
            if (obj == null)
                writer.Write((byte)GarnetObjectType.Null);
            else
                ((IGarnetObject)obj).Serialize(writer);
        }
    }
}