// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.StructWithString
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

namespace Tsavorite.test.StructWithString
{
    using ClassAllocator = GenericAllocator<StructWithString, StructWithString, StoreFunctions<StructWithString, StructWithString, StructWithString.Comparer, DefaultRecordDisposer<StructWithString, StructWithString>>>;
    using ClassStoreFunctions = StoreFunctions<StructWithString, StructWithString, StructWithString.Comparer, DefaultRecordDisposer<StructWithString, StructWithString>>;

    [AllureNUnit]
    [TestFixture]
    public class StructWithStringTests : AllureTestBase
    {
        internal class StructWithStringTestFunctions : SimpleSimpleFunctions<StructWithString, StructWithString>
        {
        }

        const int NumRecords = 1_000;
        const string KeyPrefix = "key_";
        string valuePrefix = "value_";

        StructWithStringTestFunctions functions;

        private TsavoriteKV<StructWithString, StructWithString, ClassStoreFunctions, ClassAllocator> store;
        private ClientSession<StructWithString, StructWithString, StructWithString, StructWithString, Empty, StructWithStringTestFunctions, ClassStoreFunctions, ClassAllocator> session;
        private BasicContext<StructWithString, StructWithString, StructWithString, StructWithString, Empty, StructWithStringTestFunctions, ClassStoreFunctions, ClassAllocator> bContext;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            // create a string of size 1024 bytes
            valuePrefix = new string('a', 1024);

            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false);
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.obj.log"), deleteOnClose: false);

            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                ObjectLogDevice = objlog,
                PageSize = 1L << 10,
                MemorySize = 1L << 22,
                SegmentSize = 1L << 16,
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
                bool wasPending = status.IsPending;
                if (status.IsPending)
                {
                    bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    using (completedOutputs)
                        (status, output) = GetSinglePendingResult(completedOutputs);
                }

                ClassicAssert.IsTrue(status.Found, $"{status.ToString()}; wasPending = {wasPending}");
                ClassicAssert.AreEqual(key.intField, output.intField);
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
                count++;
            ClassicAssert.AreEqual(count, NumRecords);

            readKey(48);
            store.Log.Compact<StructWithString, StructWithString, Empty, StructWithStringTestFunctions>(functions, store.Log.SafeReadOnlyAddress, compactionType);
            readKey(48);
        }
    }
}