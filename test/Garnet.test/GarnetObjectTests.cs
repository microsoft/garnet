// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using Tsavorite.core;

namespace Garnet.test
{
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;

    [TestFixture]
    public class GarnetObjectTests
    {
        TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> store;
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
            using var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, SimpleSessionFunctions<byte[], IGarnetObject, Empty>>(new SimpleSessionFunctions<byte[], IGarnetObject, Empty>());
            var bContext = session.BasicContext;

            var key = new byte[] { 0 };
            var obj = new SortedSetObject();

            bContext.Upsert(key, obj);

            IGarnetObject output = null;
            var status = bContext.Read(ref key, ref output);

            Assert.IsTrue(status.Found);
            Assert.AreEqual(obj, output);
        }

        [Test]
        public async Task WriteCheckpointRead()
        {
            var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
            var bContext = session.BasicContext;

            var key = new byte[] { 0 };
            var obj = new SortedSetObject();
            obj.Add([15], 10);

            bContext.Upsert(key, obj);

            session.Dispose();

            await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);

            store.Dispose();
            CreateStore();

            store.Recover();

            session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
            bContext = session.BasicContext;

            IGarnetObject output = null;
            var status = bContext.Read(ref key, ref output);

            session.Dispose();

            Assert.IsTrue(status.Found);
            Assert.IsTrue(obj.Equals((SortedSetObject)output));
        }

        [Test]
        public async Task CopyUpdate()
        {
            var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
            var bContext = session.BasicContext;

            var key = new byte[] { 0 };
            IGarnetObject obj = new SortedSetObject();
            ((SortedSetObject)obj).Add([15], 10);

            bContext.Upsert(key, obj);

            store.Log.Flush(true);

            bContext.RMW(ref key, ref obj);

            session.Dispose();

            await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);

            store.Dispose();
            CreateStore();

            store.Recover();

            session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
            bContext = session.BasicContext;

            IGarnetObject output = null;
            var status = bContext.Read(ref key, ref output);

            session.Dispose();

            Assert.IsTrue(status.Found);
            Assert.IsTrue(((SortedSetObject)obj).Equals((SortedSetObject)output));
        }

        private class MyFunctions : SessionFunctionsBase<byte[], IGarnetObject, IGarnetObject, IGarnetObject, Empty>
        {
            public MyFunctions()
            { }

            public override bool SingleReader(ref byte[] key, ref IGarnetObject input, ref IGarnetObject value, ref IGarnetObject dst, ref ReadInfo updateInfo)
            {
                dst = value;
                return true;
            }

            public override bool ConcurrentReader(ref byte[] key, ref IGarnetObject input, ref IGarnetObject value, ref IGarnetObject dst, ref ReadInfo updateInfo, ref RecordInfo recordInfo)
            {
                dst = value;
                return true;
            }

            public override bool CopyUpdater(ref byte[] key, ref IGarnetObject input, ref IGarnetObject oldValue, ref IGarnetObject newValue, ref IGarnetObject output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                oldValue.CopyUpdate(ref oldValue, ref newValue, false);
                return true;
            }
        }

        private void CreateStore()
        {
            logDevice ??= Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog.log");
            objectLogDevice ??= Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog.obj.log");

            var kvSettings = new TsavoriteKVSettings<byte[], IGarnetObject>
            {
                IndexSize = 1L << 13,
                LogDevice = logDevice,
                ObjectLogDevice = objectLogDevice,
                CheckpointDir = TestUtils.MethodTestDir
            };

            store = new (kvSettings
                , StoreFunctions<byte[], IGarnetObject>.Create(new ByteArrayKeyComparer(), () => new Tsavorite.core.ByteArrayBinaryObjectSerializer(), () => new MyGarnetObjectSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }
    }

    /// <summary>
    /// Serializer for IGarnetObject
    /// </summary>
    sealed class MyGarnetObjectSerializer : BinaryObjectSerializer<IGarnetObject>
    {
        /// <inheritdoc />
        public override void Deserialize(out IGarnetObject obj)
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
        public override void Serialize(ref IGarnetObject obj)
        {
            if (obj == null)
                writer.Write((byte)GarnetObjectType.Null);
            else
                obj.Serialize(writer);
        }
    }
}