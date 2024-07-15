// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.SingleWriter
{
    // Must be in a separate block so the "using StructStoreFunctions" is the first line in its namespace declaration.
    public struct StructWithString(int intValue, string prefix)
    {
        public int intField = intValue;
        public string stringField = prefix + intValue.ToString();

        public override readonly string ToString() => stringField;

        public class Comparer : IKeyComparer<StructWithString>
        {
            public long GetHashCode64(ref StructWithString k) => Utility.GetHashCode(k.intField);

            public bool Equals(ref StructWithString k1, ref StructWithString k2) => k1.intField == k2.intField && k1.stringField == k2.stringField;
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
}

namespace Tsavorite.test.SingleWriter
{
    using IntStoreFunctions = StoreFunctions<int, int, IntKeyComparer, DefaultRecordDisposer<int, int>>;
    using IntAllocator = BlittableAllocator<int, int, StoreFunctions<int, int, IntKeyComparer, DefaultRecordDisposer<int, int>>>;

    using StructStoreFunctions = StoreFunctions<StructWithString, StructWithString, StructWithString.Comparer, DefaultRecordDisposer<StructWithString, StructWithString>>;
    using StructAllocator = BlittableAllocator<StructWithString, StructWithString, StoreFunctions<StructWithString, StructWithString, StructWithString.Comparer, DefaultRecordDisposer<StructWithString, StructWithString>>>;

    internal class SingleWriterTestFunctions : SimpleSimpleFunctions<int, int>
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
        const int NumRecords = 1000;
        const int ValueMult = 1_000_000;
        const WriteReason NoReason = (WriteReason)255;

        SingleWriterTestFunctions functions;

        private TsavoriteKV<int, int, IntStoreFunctions, IntAllocator> store;
        private ClientSession<int, int, int, int, Empty, SingleWriterTestFunctions, IntStoreFunctions, IntAllocator> session;
        private BasicContext<int, int, int, int, Empty, SingleWriterTestFunctions, IntStoreFunctions, IntAllocator> bContext;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false);

            functions = new SingleWriterTestFunctions();
            TsavoriteKVSettings<int, int> kvSettings = new()
                {
                    IndexSize = 1 << 26,
                    LogDevice = log,
                    PageSize = 1 << 12,
                    MemorySize = 1 << 22,
                    ReadCopyOptions = new(ReadCopyFrom.Device, ReadCopyTo.MainLog),
                    CheckpointDir = MethodTestDir
                };
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCopyDestination dest)
                {
                    if (dest == ReadCopyDestination.ReadCache)
                    {
                        kvSettings.ReadCachePageSize = 1 << 12;
                        kvSettings.ReadCacheMemorySize = 1 << 22;
                        kvSettings.ReadCacheEnabled = true;
                        kvSettings.ReadCopyOptions = default;
                    }
                    break;
                }
            }

            store = new (kvSettings
                , StoreFunctions<int, int>.Create(IntKeyComparer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            session = store.NewSession<int, int, Empty, SingleWriterTestFunctions>(functions);
            bContext = session.BasicContext;
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
            for (int key = 0; key < NumRecords; key++)
                Assert.False(bContext.Upsert(key, input, key * ValueMult, ref output).IsPending);
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
            var status = bContext.Read(key, input, out int output);
            Assert.IsTrue(status.IsPending);
            _ = bContext.CompletePending(wait: true);
            Assert.AreEqual(expectedReason, functions.actualReason);

            functions.actualReason = NoReason;
            key = 64;
            expectedReason = WriteReason.CopyToTail;
            input = (int)expectedReason;
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog) };
            status = bContext.Read(ref key, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending && !status.IsCompleted);
            _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
            (status, output) = GetSinglePendingResult(outputs);
            Assert.IsTrue(!status.IsPending && status.IsCompleted && status.IsCompletedSuccessfully);
            Assert.IsTrue(status.Found && !status.NotFound && status.Record.Copied);
            Assert.AreEqual(expectedReason, functions.actualReason);

            functions.actualReason = NoReason;
            expectedReason = WriteReason.Compaction;
            input = (int)expectedReason;
            _ = store.Log.Compact<int, int, Empty, SingleWriterTestFunctions>(functions, ref input, ref output, store.Log.SafeReadOnlyAddress, CompactionType.Scan);
            Assert.AreEqual(expectedReason, functions.actualReason);
        }
    }

    [TestFixture]
    public class StructWithStringTests
    {
        internal class StructWithStringTestFunctions : SimpleSimpleFunctions<StructWithString, StructWithString>
        {
        }

        const int NumRecords = 1_000;
        const string KeyPrefix = "key_";
        string valuePrefix = "value_";

        StructWithStringTestFunctions functions;

        private TsavoriteKV<StructWithString, StructWithString, StructStoreFunctions, StructAllocator> store;
        private ClientSession<StructWithString, StructWithString, StructWithString, StructWithString, Empty, StructWithStringTestFunctions, StructStoreFunctions, StructAllocator> session;
        private BasicContext<StructWithString, StructWithString, StructWithString, StructWithString, Empty, StructWithStringTestFunctions, StructStoreFunctions, StructAllocator> bContext;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            // create a string of size 1024 bytes
            valuePrefix = new string('a', 1024);

            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false);
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.obj.log"), deleteOnClose: false);

            store = new (new TsavoriteKVSettings<StructWithString, StructWithString>()
                {
                    IndexSize = 1L << 26,
                    LogDevice = log, ObjectLogDevice = objlog, 
                    PageSize = 1 << 10, MemorySize = 1 << 22, SegmentSize = 1 << 16,
                    CheckpointDir = MethodTestDir
                }, StoreFunctions<StructWithString, StructWithString>.Create(new StructWithString.Comparer(), () => new StructWithString.Serializer(), () => new StructWithString.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            functions = new();
            session = store.NewSession<StructWithString, StructWithString, Empty, StructWithStringTestFunctions>(functions);
            bContext = session.BasicContext;
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
            for (int ii = 0; ii < NumRecords; ii++)
            {
                StructWithString key = new(ii, KeyPrefix);
                StructWithString value = new(ii, valuePrefix);
                bContext.Upsert(ref key, ref value);
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
                StructWithString key = new(keyInt, KeyPrefix);
                var (status, output) = bContext.Read(key);
                if (status.IsPending)
                {
                    bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
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
            Assert.AreEqual(count, NumRecords);

            store.Log.Compact<StructWithString, StructWithString, Empty, StructWithStringTestFunctions>(functions, store.Log.SafeReadOnlyAddress, compactionType);
            readKey(48);
        }
    }
}