// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    [TestFixture]
    public class GarnetObjectTests : TestBase
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
            TestUtils.OnTearDown();
        }

        [Test]
        public void WriteRead()
        {
            using var session = store.NewSession<FixedSpanByteKey, IGarnetObject, IGarnetObject, Empty, SimpleGarnetObjectSessionFunctions>(new SimpleGarnetObjectSessionFunctions());
            var bContext = session.BasicContext;

            var key = new ReadOnlySpan<byte>([0]);
            var obj = new SortedSetObject();

            _ = bContext.Upsert((FixedSpanByteKey)key, obj);

            IGarnetObject output = null;
            var status = bContext.Read((FixedSpanByteKey)key, ref output);

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(obj, output);
        }

        const int keyNum = 0;

        [Test]
        public async Task WriteCheckpointRead()
        {
            var obj = new SortedSetObject();

            LocalWrite();
            _ = await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);
            store.Dispose();
            CreateStore();
            _ = await store.RecoverAsync().ConfigureAwait(false);
            LocalRead();

            void LocalWrite()
            {
                using var session = store.NewSession<FixedSpanByteKey, IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
                var bContext = session.BasicContext;

                var key = new ReadOnlySpan<byte>([keyNum]);
                obj.Add([15], 10);

                _ = bContext.Upsert((FixedSpanByteKey)key, obj);
            }

            void LocalRead()
            {
                using var session = store.NewSession<FixedSpanByteKey, IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
                var bContext = session.BasicContext;

                IGarnetObject output = null;
                var key = new ReadOnlySpan<byte>([keyNum]);
                var status = bContext.Read((FixedSpanByteKey)key, ref output);

                ClassicAssert.IsTrue(status.Found);
                ClassicAssert.IsTrue(obj.Equals((SortedSetObject)output));
            }
        }

        [Test]
        public async Task WriteCheckpointCopyUpdate()
        {
            IGarnetObject obj = new SortedSetObject();

            LocalWrite();
            _ = await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);
            store.Dispose();
            CreateStore();
            _ = await store.RecoverAsync().ConfigureAwait(false);
            LocalRead();

            void LocalWrite()
            {
                using var session = store.NewSession<FixedSpanByteKey, IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
                var bContext = session.BasicContext;

                var key = new ReadOnlySpan<byte>([keyNum]);
                ((SortedSetObject)obj).Add([15], 10);

                _ = bContext.Upsert((FixedSpanByteKey)key, obj);
                store.Log.Flush(true);
                _ = bContext.RMW((FixedSpanByteKey)key, ref obj);
            }

            void LocalRead()
            {
                using var session = store.NewSession<FixedSpanByteKey, IGarnetObject, IGarnetObject, Empty, MyFunctions>(new MyFunctions());
                var bContext = session.BasicContext;

                IGarnetObject output = null;
                var key = new ReadOnlySpan<byte>([keyNum]);
                var status = bContext.Read((FixedSpanByteKey)key, ref output);

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
                Assert.That(dstLogRecord.DataHeader.ValueIsObject, Is.True);
                dstLogRecord.TrySetValueObject(srcLogRecord.ValueObject.Clone());
                return true;
            }

            public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref IGarnetObject input)
                => new() { KeySize = srcLogRecord.Key.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
            public override RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref IGarnetObject input)
                => new() { KeySize = key.KeyBytes.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
            public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ReadOnlySpan<byte> value, ref IGarnetObject input)
                => new() { KeySize = key.KeyBytes.Length, ValueSize = value.Length, ValueIsObject = false };
            public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, IHeapObject value, ref IGarnetObject input)
                => new() { KeySize = key.KeyBytes.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
        }

        /// <summary>
        /// Serialization runs on the flush path while readers may concurrently access the same instance, and only
        /// writers are excluded from a record that is being serialized. Serializing must therefore be a pure read:
        /// if it deleted expired members it would race those readers and corrupt the collection.
        /// </summary>
        [Test]
        public void SerializeDoesNotMutateSortedSetWithExpiredMembers()
        {
            var obj = new SortedSetObject(new BinaryReader(new MemoryStream(BuildSortedSetBytes())));

            ClassicAssert.IsTrue(obj.HasExpirableItems());
            Thread.Sleep(400);

            var sizeBeforeSerialize = obj.HeapMemorySize;

            using var stream = new MemoryStream();
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
                obj.Serialize(writer);

            ClassicAssert.AreEqual(sizeBeforeSerialize, obj.HeapMemorySize,
                "Serializing a sorted set must not delete its expired members.");
            ClassicAssert.IsTrue(obj.HasExpirableItems(),
                "Serializing a sorted set must not tear down its expiration structures.");

            // The expired member is dropped when the serialized form is read back.
            var roundTripped = new SortedSetObject(new BinaryReader(new MemoryStream(stream.ToArray()[1..])));
            ClassicAssert.AreEqual(1, roundTripped.Count());
        }

        /// <summary>
        /// Serializing a hash must not delete expired fields, for the same reason as the sorted set case above.
        /// </summary>
        [Test]
        public void SerializeDoesNotMutateHashWithExpiredFields()
        {
            var obj = new HashObject(new BinaryReader(new MemoryStream(BuildHashBytes())));

            var sizeAfterLoad = obj.HeapMemorySize;
            Thread.Sleep(400);

            using var stream = new MemoryStream();
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
                obj.Serialize(writer);

            ClassicAssert.AreEqual(sizeAfterLoad, obj.HeapMemorySize,
                "Serializing a hash must not delete its expired fields.");

            // The expired field is dropped when the serialized form is read back, so the reloaded object is smaller.
            var roundTripped = new HashObject(new BinaryReader(new MemoryStream(stream.ToArray()[1..])));
            ClassicAssert.Less(roundTripped.HeapMemorySize, obj.HeapMemorySize);
        }

        // One member expiring shortly, one that never expires. Matches the layout read by the deserializing ctor.
        private const int ExpirationBitMask = 1 << 31;

        private static byte[] BuildSortedSetBytes()
        {
            var soonExpiring = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromMilliseconds(200).Ticks;

            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            writer.Write(2);

            var expiring = Encoding.ASCII.GetBytes("expiring");
            writer.Write(expiring.Length | ExpirationBitMask);
            writer.Write(expiring);
            writer.Write(1.0d);
            writer.Write(soonExpiring);

            var persistent = Encoding.ASCII.GetBytes("persistent");
            writer.Write(persistent.Length);
            writer.Write(persistent);
            writer.Write(2.0d);

            writer.Flush();
            return stream.ToArray();
        }

        private static byte[] BuildHashBytes()
        {
            var soonExpiring = DateTimeOffset.UtcNow.Ticks + TimeSpan.FromMilliseconds(200).Ticks;

            using var stream = new MemoryStream();
            using var writer = new BinaryWriter(stream);

            writer.Write(2);

            var expiring = Encoding.ASCII.GetBytes("expiring");
            writer.Write(expiring.Length | ExpirationBitMask);
            writer.Write(expiring);
            var expiringValue = Encoding.ASCII.GetBytes("v1");
            writer.Write(expiringValue.Length);
            writer.Write(expiringValue);
            writer.Write(soonExpiring);

            var persistent = Encoding.ASCII.GetBytes("persistent");
            writer.Write(persistent.Length);
            writer.Write(persistent);
            var persistentValue = Encoding.ASCII.GetBytes("v2");
            writer.Write(persistentValue.Length);
            writer.Write(persistentValue);

            writer.Flush();
            return stream.ToArray();
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
                , Tsavorite.core.StoreFunctions.Create(new GarnetKeyComparer(), () => new MyGarnetObjectSerializer(),
                    new GarnetRecordTriggers())
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