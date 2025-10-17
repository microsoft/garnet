// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

#pragma warning disable IDE0060 // Remove unused parameter == Some parameters are just to let [Setup] know what to do
namespace Tsavorite.test
{
    // Must be in a separate block so the "using StructStoreFunctions" is the first line in its namespace declaration.
    struct HashModuloKeyStructComparer : IKeyComparer
    {
        readonly HashModulo modRange;

        internal HashModuloKeyStructComparer(HashModulo mod) => modRange = mod;

        public readonly bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => k1.AsRef<KeyStruct>().kfield1 == k2.AsRef<KeyStruct>().kfield1;

        // Force collisions to create a chain
        public readonly long GetHashCode64(ReadOnlySpan<byte> k)
        {
            var value = Utility.GetHashCode(k.AsRef<KeyStruct>().kfield1);
            return modRange != HashModulo.NoMod ? value % (long)modRange : value;
        }
    }
}

namespace Tsavorite.test
{
    using StructAllocator = SpanByteAllocator<StoreFunctions<HashModuloKeyStructComparer, SpanByteRecordDisposer>>;
    using StructStoreFunctions = StoreFunctions<HashModuloKeyStructComparer, SpanByteRecordDisposer>;

    [TestFixture]
    public class SpanByteLogCompactionTests
    {
        private TsavoriteKV<StructStoreFunctions, StructAllocator> store;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "SpanByteLogCompactionTests.log"), deleteOnClose: true);

            var hashMod = HashModulo.NoMod;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is HashModulo mod)
                {
                    hashMod = mod;
                    continue;
                }
            }

            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                MemorySize = 1L << 15,
                PageSize = 1L << 9
            }, StoreFunctions.Create(new HashModuloKeyStructComparer(hashMod), SpanByteRecordDisposer.Instance)
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
            DeleteDirectory(MethodTestDir);
        }

        static void VerifyRead(ClientSession<InputStruct, OutputStruct, int, FunctionsCompaction, StructStoreFunctions, StructAllocator> session, int totalRecords, Func<int, bool> isDeleted)
        {
            InputStruct input = default;
            int numPending = 0;
            var bContext = session.BasicContext;

            void drainPending()
            {
                ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var outputs, wait: true));
                using (outputs)
                {
                    for (; outputs.Next(); --numPending)
                    {
                        if (isDeleted((int)outputs.Current.Key.AsRef<KeyStruct>().kfield1))
                        {
                            ClassicAssert.IsFalse(outputs.Current.Status.Found);
                            continue;
                        }
                        ClassicAssert.IsTrue(outputs.Current.Status.Found);
                        ClassicAssert.AreEqual(outputs.Current.Key.AsRef<KeyStruct>().kfield1, outputs.Current.Output.value.vfield1);
                        ClassicAssert.AreEqual(outputs.Current.Key.AsRef<KeyStruct>().kfield2, outputs.Current.Output.value.vfield2);
                    }
                }
                ClassicAssert.AreEqual(numPending, 0);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = bContext.Read(SpanByte.FromPinnedVariable(ref key1), ref input, ref output, isDeleted(i) ? 1 : 0);
                if (!status.IsPending)
                {
                    if (isDeleted(i))
                    {
                        ClassicAssert.IsFalse(status.Found);
                        continue;
                    }
                    ClassicAssert.IsTrue(status.Found);
                    ClassicAssert.AreEqual(value.vfield1, output.value.vfield1);
                    ClassicAssert.AreEqual(value.vfield2, output.value.vfield2);
                }
                else if (++numPending == 256)
                    drainPending();
            }

            if (numPending > 0)
                drainPending();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        [Category("Smoke")]

        public void SpanByteLogCompactionTest1([Values] CompactionType compactionType)
        {
            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());
            var bContext = session.BasicContext;

            const int totalRecords = 2_000;
            var start = store.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords - 1000)
                    compactUntil = store.Log.TailAddress;

                var keyStruct = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var valueStruct = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyStruct), value = SpanByte.FromPinnedVariable(ref valueStruct);
                _ = bContext.Upsert(key, value, 0);
            }

            store.Log.FlushAndEvict(wait: true);
            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();

            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read all keys - all should be present
            VerifyRead(session, totalRecords, key => false);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        public void SpanByteLogCompactionTest2([Values] CompactionType compactionType, [Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());
            var bContext = session.BasicContext;

            const int totalRecords = 2_000;
            var start = store.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords - 1000)
                    compactUntil = store.Log.TailAddress;

                var keyStruct = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var valueStruct = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyStruct), value = SpanByte.FromPinnedVariable(ref valueStruct);
                _ = bContext.Upsert(key, value, 0);
            }

            store.Log.FlushAndEvict(true);

            // Flush, then put fresh entries for half the records to force IO. We want this to have multiple levels before Compact:
            //      HeadAddress
            //      1. Addresses of these fresh records
            //      (HeadAddress after Flush)
            //      2. Addresses of original records
            //      BeginAddress
            // Without this, the Compaction logic will not caused I/O, because without Level 1, the FindTag would return an entry
            // whose address pointed to the record in Level 2 (which would be Level 1 then), which means it will be caught by the
            // test that the address is < minAddress, so no IO is needed.
            for (int i = 0; i < totalRecords / 2; i++)
            {
                var keyStruct = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var valueStruct = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyStruct), value = SpanByte.FromPinnedVariable(ref valueStruct);
                _ = bContext.Upsert(key, value, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read all keys - all should be present
            VerifyRead(session, totalRecords, key => false);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        public void SpanByteLogCompactionTest3([Values] CompactionType compactionType)
        {
            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());
            var bContext = session.BasicContext;

            const int totalRecords = 2_000;
            var start = store.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = store.Log.TailAddress;

                var keyStruct = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var valueStruct = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyStruct), value = SpanByte.FromPinnedVariable(ref valueStruct);
                _ = bContext.Upsert(key, value, 0);

                if (i % 8 == 0)
                {
                    int j = i / 4;
                    keyStruct = new KeyStruct { kfield1 = j, kfield2 = j + 1 };
                    _ = bContext.Delete(key, 0);
                }
            }

            var tail = store.Log.TailAddress;
            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read all keys - all should be present except those we deleted
            VerifyRead(session, totalRecords, key => (key < totalRecords / 4) && (key % 2 == 0));
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        [Category("Smoke")]

        public void SpanByteLogCompactionCustomFunctionsTest1([Values] CompactionType compactionType)
        {
            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());
            var bContext = session.BasicContext;

            InputStruct input = default;

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;
            var compactUntil = 0L;

            for (var i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = store.Log.TailAddress;

                var keyStruct = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var valueStruct = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyStruct), value = SpanByte.FromPinnedVariable(ref valueStruct);
                _ = bContext.Upsert(key, value, 0);
            }

            var tail = store.Log.TailAddress;

            // Only leave records with even vfield1
            compactUntil = session.Compact(compactUntil, compactionType, default(EvenCompactionFunctions));
            store.Log.Truncate();
            ClassicAssert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read 2000 keys - all should be present
            for (var i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var keyStruct = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var valueStruct = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyStruct), value = SpanByte.FromPinnedVariable(ref valueStruct);

                var ctx = (i < (totalRecords / 2) && (i % 2 != 0)) ? 1 : 0;

                var status = bContext.Read(key, ref input, ref output, ctx);
                if (status.IsPending)
                {
                    ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var outputs, wait: true));
                    (status, output) = GetSinglePendingResult(outputs);
                }

                if (ctx == 0)
                {
                    ClassicAssert.IsTrue(status.Found);
                    ClassicAssert.AreEqual(valueStruct.vfield1, output.value.vfield1);
                    ClassicAssert.AreEqual(valueStruct.vfield2, output.value.vfield2);
                }
                else
                {
                    ClassicAssert.IsFalse(status.Found);
                }
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        public void SpanByteLogCompactionCustomFunctionsTest2([Values] CompactionType compactionType, [Values(FlushMode.ReadOnly, FlushMode.OnDisk)] FlushMode flushMode)
        {
            // Update: irrelevant as session compaction no longer uses Copy/CopyInPlace
            // This test checks if CopyInPlace returning false triggers call to Copy

            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());
            var bContext = session.BasicContext;

            var keyStruct = new KeyStruct { kfield1 = 100, kfield2 = 101 };
            var valueStruct = new ValueStruct { vfield1 = 10, vfield2 = 20 };
            ReadOnlySpan<byte> key = SpanByte.FromPinnedVariable(ref keyStruct), value = SpanByte.FromPinnedVariable(ref valueStruct);

            var input = default(InputStruct);
            var output = default(OutputStruct);

            _ = bContext.Upsert(key, value, 0);
            var status = bContext.Read(key, ref input, ref output, 0);
            Debug.Assert(status.Found);

            store.Log.Flush(true);

            valueStruct = new ValueStruct { vfield1 = 11, vfield2 = 21 };
            _ = bContext.Upsert(key, value, 0);
            status = bContext.Read(key, ref input, ref output, 0);
            Debug.Assert(status.Found);

            if (flushMode == FlushMode.OnDisk)
                store.Log.FlushAndEvict(true);
            else
                store.Log.Flush(true);

            var compactUntil = session.Compact(store.Log.TailAddress, compactionType);
            store.Log.Truncate();

            status = bContext.Read(key, ref input, ref output, 0);
            if (status.IsPending)
            {
                ClassicAssert.IsTrue(bContext.CompletePendingWithOutputs(out var outputs, wait: true));
                (status, output) = GetSinglePendingResult(outputs);
            }

            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(valueStruct.vfield1, output.value.vfield1);
            ClassicAssert.AreEqual(valueStruct.vfield2, output.value.vfield2);
        }

        private struct EvenCompactionFunctions : ICompactionFunctions
        {
            public bool IsDeleted<TSourceLogRecord>(in TSourceLogRecord logRecord)
                where TSourceLogRecord : ISourceLogRecord
                => logRecord.ValueSpan.AsRef<ValueStruct>().vfield1 % 2 != 0;
        }
    }
}