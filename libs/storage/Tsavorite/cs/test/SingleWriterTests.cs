// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.SingleWriter
{
    internal class SingleWriterTestFunctions : SimpleFunctions<int, int>
    {
        internal WriteReason actualReason;

        public override bool SingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            Assert.AreEqual((WriteReason)input, reason);
            actualReason = reason;
            return true;
        }

        public override void PostSingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            Assert.AreEqual((WriteReason)input, reason);
            actualReason = reason;
        }
    }

    class SingleWriterTests
    {
        const int numRecords = 1000;
        const int valueMult = 1_000_000;
        const WriteReason NoReason = (WriteReason)(-1);

        SingleWriterTestFunctions functions;

        private TsavoriteKV<int, int> store;
        private ClientSession<int, int, int, int, Empty, SingleWriterTestFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false);

            functions = new SingleWriterTestFunctions();
            LogSettings logSettings = new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22, ReadCopyOptions = new(ReadCopyFrom.Device, ReadCopyTo.MainLog) };
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCopyDestination dest)
                {
                    if (dest == ReadCopyDestination.ReadCache)
                    {
                        logSettings.ReadCacheSettings = new() { PageSizeBits = 12, MemorySizeBits = 22 };
                        logSettings.ReadCopyOptions = default;
                    }
                    break;
                }
            }

            store = new TsavoriteKV<int, int>(1L << 20, logSettings, new CheckpointSettings { CheckpointDir = MethodTestDir });
            session = store.NewSession<int, int, Empty, SingleWriterTestFunctions>(functions);
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        void Populate()
        {
            int input = (int)WriteReason.Upsert;
            int output = 0;
            for (int key = 0; key < numRecords; key++)
                Assert.False(session.Upsert(key, input, key * valueMult, ref output).IsPending);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void SingleWriterReasonsTest([Values] ReadCopyDestination readCopyDestination)
        {
            functions.actualReason = NoReason;
            Populate();
            Assert.AreEqual(WriteReason.Upsert, functions.actualReason);

            store.Log.FlushAndEvict(wait: true);

            functions.actualReason = NoReason;
            int key = 42;
            WriteReason expectedReason = readCopyDestination == ReadCopyDestination.ReadCache ? WriteReason.CopyToReadCache : WriteReason.CopyToTail;
            int input = (int)expectedReason;
            var status = session.Read(key, input, out int output);
            Assert.IsTrue(status.IsPending);
            session.CompletePending(wait: true);
            Assert.AreEqual(expectedReason, functions.actualReason);

            functions.actualReason = NoReason;
            key = 64;
            expectedReason = WriteReason.CopyToTail;
            input = (int)expectedReason;
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog) };
            status = session.Read(ref key, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending && !status.IsCompleted);
            session.CompletePendingWithOutputs(out var outputs, wait: true);
            (status, output) = GetSinglePendingResult(outputs);
            Assert.IsTrue(!status.IsPending && status.IsCompleted && status.IsCompletedSuccessfully);
            Assert.IsTrue(status.Found && !status.NotFound && status.Record.Copied);
            Assert.AreEqual(expectedReason, functions.actualReason);

            functions.actualReason = NoReason;
            expectedReason = WriteReason.Compaction;
            input = (int)expectedReason;
            store.Log.Compact<int, int, Empty, SingleWriterTestFunctions>(functions, ref input, ref output, store.Log.SafeReadOnlyAddress, CompactionType.Scan);
            Assert.AreEqual(expectedReason, functions.actualReason);
        }
    }

    [TestFixture]
    public class StructWithStringTests
    {
        public struct StructWithString
        {
            public int intField;
            public string stringField;

            public StructWithString(int intValue, string prefix)
            {
                intField = intValue;
                stringField = prefix + intValue.ToString();
            }

            public override string ToString() => stringField;

            public class Comparer : ITsavoriteEqualityComparer<StructWithString>
            {
                public long GetHashCode64(ref StructWithString k) => Utility.GetHashCode(k.intField);

                public bool Equals(ref StructWithString k1, ref StructWithString k2)
                    => k1.intField == k2.intField && k1.stringField == k2.stringField;
            }

            public class Serializer : BinaryObjectSerializer<StructWithString>
            {
                public override void Deserialize(out StructWithString obj)
                {
                    var intField = reader.ReadInt32();
                    var stringField = reader.ReadString();
                    obj = new() { intField = intField, stringField = stringField };
                }

                public override void Serialize(ref StructWithString obj)
                {
                    writer.Write(obj.intField);
                    writer.Write(obj.stringField);
                }
            }
        }

        internal class StructWithStringTestFunctions : SimpleFunctions<StructWithString, StructWithString>
        {
        }

        const int numRecords = 1_000;
        const string keyPrefix = "key_";
        string valuePrefix = "value_";

        StructWithStringTestFunctions functions;

        private TsavoriteKV<StructWithString, StructWithString> store;
        private ClientSession<StructWithString, StructWithString, StructWithString, StructWithString, Empty, StructWithStringTestFunctions> session;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            // create a string of size 1024 bytes
            valuePrefix = new string('a', 1024);

            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false);
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.obj.log"), deleteOnClose: false);
            SerializerSettings<StructWithString, StructWithString> serializerSettings = new()
            {
                keySerializer = () => new StructWithString.Serializer(),
                valueSerializer = () => new StructWithString.Serializer()
            };
            store = new TsavoriteKV<StructWithString, StructWithString>(1L << 20,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, PageSizeBits = 10, MemorySizeBits = 22, SegmentSizeBits = 16 },
                new CheckpointSettings { CheckpointDir = MethodTestDir },
                serializerSettings: serializerSettings, comparer: new StructWithString.Comparer());

            functions = new();
            session = store.NewSession<StructWithString, StructWithString, Empty, StructWithStringTestFunctions>(functions);
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            objlog?.Dispose();
            objlog = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        void Populate()
        {
            for (int ii = 0; ii < numRecords; ii++)
            {
                StructWithString key = new(ii, keyPrefix);
                StructWithString value = new(ii, valuePrefix);
                session.Upsert(ref key, ref value);
                if (ii % 3_000 == 0)
                {
                    store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).GetAwaiter().GetResult();
                    store.Recover();
                }
            }
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(SmokeTestCategory)]
        public void StructWithStringCompactTest([Values] CompactionType compactionType, [Values] bool flush)
        {
            void readKey(int keyInt)
            {
                StructWithString key = new(keyInt, keyPrefix);
                var (status, output) = session.Read(key);
                if (status.IsPending)
                {
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    using (completedOutputs)
                        (status, output) = GetSinglePendingResult(completedOutputs);
                }
                Assert.IsTrue(status.Found, status.ToString());
                Assert.AreEqual(key.intField, output.intField);
            }

            Populate();
            readKey(12);
            if (flush)
            {
                store.Log.FlushAndEvict(wait: true);
                readKey(24);
            }
            int count = 0;
            using var iter = store.Log.Scan(0, store.Log.TailAddress);
            while (iter.GetNext(out var _))
            {
                count++;
            }
            Assert.AreEqual(count, numRecords);

            store.Log.Compact<StructWithString, StructWithString, Empty, StructWithStringTestFunctions>(functions, store.Log.SafeReadOnlyAddress, compactionType);
            readKey(48);
        }
    }
}