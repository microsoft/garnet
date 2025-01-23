// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    using ObjectStoreAllocator = ObjectAllocator<IGarnetObject, StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>;

    [TestFixture]
    public class GarnetObjectTests
    {
        TsavoriteKV<IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> store;
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
            using var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, SimpleSessionFunctions<IGarnetObject, Empty>>(new SimpleSessionFunctions<IGarnetObject, Empty>());
            var bContext = session.BasicContext;

            var key = SpanByte.FromPinnedSpan([0]);
            var obj = new SortedSetObject();

            _ = bContext.Upsert(key, obj);

            IGarnetObject output = null;
            var status = bContext.Read(key, ref output);

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(obj, output);
        }

        [Test]
        public async Task WriteCheckpointRead()
        {
            var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
            var bContext = session.BasicContext;

            var key = SpanByte.FromPinnedSpan([0]);
            var obj = new SortedSetObject();
            obj.Add([15], 10);

            _ = bContext.Upsert(key, obj);

            session.Dispose();

            _ = await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);

            store.Dispose();
            CreateStore();

            _ = store.Recover();

            session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
            bContext = session.BasicContext;

            IGarnetObject output = null;
            var status = bContext.Read(key, ref output);

            session.Dispose();

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.IsTrue(obj.Equals((SortedSetObject)output));
        }

        [Test]
        public async Task CopyUpdate()
        {
            var session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
            var bContext = session.BasicContext;

            var key = SpanByte.FromPinnedSpan([0]);
            IGarnetObject obj = new SortedSetObject();
            ((SortedSetObject)obj).Add([15], 10);

            _ = bContext.Upsert(key, obj);

            store.Log.Flush(true);

            _ = bContext.RMW(key, ref obj);

            session.Dispose();

            _ = await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);

            store.Dispose();
            CreateStore();

            _ = store.Recover();

            session = store.NewSession<IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
            bContext = session.BasicContext;

            IGarnetObject output = null;
            var status = bContext.Read(key, ref output);

            session.Dispose();

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.IsTrue(((SortedSetObject)obj).Equals((SortedSetObject)output));
        }

        private class MyFunctions : SessionFunctionsBase<IGarnetObject, IGarnetObject, IGarnetObject, Empty>
        {
            public MyFunctions()
            { }

            public override bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref IGarnetObject input, ref IGarnetObject output, ref ReadInfo readInfo)
            {
                output = srcLogRecord.ValueObject;
                return true;
            }

            public override bool ConcurrentReader(ref LogRecord<IGarnetObject> logRecord, ref IGarnetObject input, ref IGarnetObject output, ref ReadInfo readInfo)
            {
                output = logRecord.ValueObject;
                return true;
            }

            public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<IGarnetObject> dstLogRecord, ref IGarnetObject input, ref IGarnetObject output, ref RMWInfo rmwInfo)
            {
                _ = srcLogRecord.ValueObject.CopyUpdate(srcLogRecord.Info.IsInNewVersion, ref rmwInfo);
                return true;
            }
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
                , StoreFunctions<IGarnetObject>.Create(new SpanByteComparer(), () => new MyGarnetObjectSerializer())
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