// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.recovery.objects
{
    [TestFixture]
    public class ObjectRecoveryTests2
    {
        int iterations;
        string TsavoriteFolderPath { get; set; }

        [SetUp]
        public void Setup()
        {
            TsavoriteFolderPath = TestUtils.MethodTestDir;
            TestUtils.RecreateDirectory(TsavoriteFolderPath);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(TsavoriteFolderPath);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]

        public async ValueTask ObjectRecoveryTest2(
            [Values] CheckpointType checkpointType,
            [Range(300, 700, 300)] int iterations,
            [Values] bool isAsync)
        {
            this.iterations = iterations;
            Prepare(out _, out _, out IDevice log, out IDevice objlog, out TsavoriteKV<MyKey, MyValue> h, out MyContext context);

            var session1 = h.NewSession<MyInput, MyOutput, MyContext, MyFunctions>(new MyFunctions());
            Write(session1, context, h, checkpointType);
            Read(session1, context, false);
            session1.Dispose();

            h.TryInitiateFullCheckpoint(out _, checkpointType);
            h.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

            Destroy(log, objlog, h);

            Prepare(out _, out _, out log, out objlog, out h, out context);

            if (isAsync)
                await h.RecoverAsync();
            else
                h.Recover();

            var session2 = h.NewSession<MyInput, MyOutput, MyContext, MyFunctions>(new MyFunctions());
            Read(session2, context, true);
            session2.Dispose();

            Destroy(log, objlog, h);
        }

        private void Prepare(out string logPath, out string objPath, out IDevice log, out IDevice objlog, out TsavoriteKV<MyKey, MyValue> h, out MyContext context)
        {
            logPath = Path.Combine(TsavoriteFolderPath, $"RecoverTests.log");
            objPath = Path.Combine(TsavoriteFolderPath, $"RecoverTests_HEAP.log");
            log = Devices.CreateLogDevice(logPath);
            objlog = Devices.CreateLogDevice(objPath);
            h = new TsavoriteKV<MyKey, MyValue>
                (1L << 20,
                new LogSettings
                {
                    LogDevice = log,
                    ObjectLogDevice = objlog,
                    SegmentSizeBits = 12,
                    MemorySizeBits = 12,
                    PageSizeBits = 9
                },
                new CheckpointSettings()
                {
                    CheckpointDir = Path.Combine(TsavoriteFolderPath, "check-points")
                },
                new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
             );
            context = new MyContext();
        }

        private static void Destroy(IDevice log, IDevice objlog, TsavoriteKV<MyKey, MyValue> h)
        {
            // Dispose Tsavorite instance and log
            h.Dispose();
            log.Dispose();
            objlog.Dispose();
        }

        private void Write(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions> session, MyContext context, TsavoriteKV<MyKey, MyValue> store, CheckpointType checkpointType)
        {
            for (int i = 0; i < iterations; i++)
            {
                var _key = new MyKey { key = i, name = i.ToString() };
                var value = new MyValue { value = i.ToString() };
                session.Upsert(ref _key, ref value, context, 0);

                if (i % 100 == 0)
                {
                    store.TryInitiateFullCheckpoint(out _, checkpointType);
                    store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
                }
            }
        }

        private void Read(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions> session, MyContext context, bool delete)
        {
            for (int i = 0; i < iterations; i++)
            {
                MyKey key = new() { key = i, name = i.ToString() };
                MyInput input = default;
                MyOutput g1 = new();
                var status = session.Read(ref key, ref input, ref g1, context, 0);

                if (status.IsPending)
                {
                    session.CompletePending(true);
                    context.FinalizeRead(ref status, ref g1);
                }

                Assert.IsTrue(status.Found);
                Assert.AreEqual(i.ToString(), g1.value.value);
            }

            if (delete)
            {
                MyKey key = new() { key = 1, name = "1" };
                MyInput input = default;
                MyOutput output = new();
                session.Delete(ref key, context, 0);
                var status = session.Read(ref key, ref input, ref output, context, 0);

                if (status.IsPending)
                {
                    session.CompletePending(true);
                    context.FinalizeRead(ref status, ref output);
                }

                Assert.IsFalse(status.Found);
            }
        }
    }

    public class MyKeySerializer : BinaryObjectSerializer<MyKey>
    {
        public override void Serialize(ref MyKey key)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(key.name);
            writer.Write(4 + bytes.Length);
            writer.Write(key.key);
            writer.Write(bytes);
        }

        public override void Deserialize(out MyKey key)
        {
            key = new MyKey();
            var size = reader.ReadInt32();
            key.key = reader.ReadInt32();
            var bytes = new byte[size - 4];
            reader.Read(bytes, 0, size - 4);
            key.name = System.Text.Encoding.UTF8.GetString(bytes);

        }
    }

    public class MyValueSerializer : BinaryObjectSerializer<MyValue>
    {
        public override void Serialize(ref MyValue value)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(value.value);
            writer.Write(bytes.Length);
            writer.Write(bytes);
        }

        public override void Deserialize(out MyValue value)
        {
            value = new MyValue();
            var size = reader.ReadInt32();
            var bytes = new byte[size];
            reader.Read(bytes, 0, size);
            value.value = System.Text.Encoding.UTF8.GetString(bytes);
        }
    }

    public class MyKey : ITsavoriteEqualityComparer<MyKey>
    {
        public int key;
        public string name;

        public long GetHashCode64(ref MyKey key) => Utility.GetHashCode(key.key);
        public bool Equals(ref MyKey key1, ref MyKey key2) => key1.key == key2.key && key1.name == key2.name;
    }


    public class MyValue { public string value; }
    public class MyInput { public string value; }
    public class MyOutput { public MyValue value; }

    public class MyContext
    {
        private Status _status;
        private MyOutput _g1;

        internal void Populate(ref Status status, ref MyOutput g1)
        {
            _status = status;
            _g1 = g1;
        }
        internal void FinalizeRead(ref Status status, ref MyOutput g1)
        {
            status = _status;
            g1 = _g1;
        }
    }


    public class MyFunctions : FunctionsBase<MyKey, MyValue, MyInput, MyOutput, MyContext>
    {
        public override bool InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) { value.value = input.value; return true; }
        public override bool NeedCopyUpdate(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyOutput output, ref RMWInfo rmwInfo) => true;
        public override bool CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) { newValue = oldValue; return true; }
        public override bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
        {
            if (value.value.Length < input.value.Length)
                return false;
            value.value = input.value;
            return true;
        }


        public override bool SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool SingleWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo) { dst = src; return true; }

        public override bool ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            if (src == null)
                return false;

            if (dst.value.Length != src.value.Length)
                return false;

            dst = src;
            return true;
        }

        public override void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, MyContext ctx, Status status, RecordMetadata recordMetadata) => ctx.Populate(ref status, ref output);
    }
}