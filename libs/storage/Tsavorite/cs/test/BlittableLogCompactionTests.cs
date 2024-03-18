// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

#pragma warning disable IDE0060 // Remove unused parameter == Some parameters are just to let [Setup] know what to do

namespace Tsavorite.test
{
    [TestFixture]
    public class BlittableLogCompactionTests
    {
        private TsavoriteKV<KeyStruct, ValueStruct> store;
        private IDevice log;

        struct HashModuloComparer : ITsavoriteEqualityComparer<KeyStruct>
        {
            readonly HashModulo modRange;

            internal HashModuloComparer(HashModulo mod) => modRange = mod;

            public bool Equals(ref KeyStruct k1, ref KeyStruct k2) => k1.kfield1 == k2.kfield1;

            // Force collisions to create a chain
            public long GetHashCode64(ref KeyStruct k)
            {
                var value = Utility.GetHashCode(k.kfield1);
                return modRange != HashModulo.NoMod ? value % (long)modRange : value;
            }
        }

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(MethodTestDir + "/BlittableLogCompactionTests.log", deleteOnClose: true);

            var concurrencyControlMode = ConcurrencyControlMode.LockTable;
            var hashMod = HashModulo.NoMod;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ConcurrencyControlMode locking_mode)
                {
                    concurrencyControlMode = locking_mode;
                    continue;
                }
                if (arg is HashModulo mod)
                {
                    hashMod = mod;
                    continue;
                }
            }

            store = new TsavoriteKV<KeyStruct, ValueStruct>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9 }, comparer: new HashModuloComparer(hashMod), concurrencyControlMode: concurrencyControlMode); ;
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

        void VerifyRead(ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, int, FunctionsCompaction> session, int totalRecords, Func<int, bool> isDeleted)
        {
            InputStruct input = default;
            int numPending = 0;

            void drainPending()
            {
                Assert.IsTrue(session.CompletePendingWithOutputs(out var outputs, wait: true));
                using (outputs)
                {
                    for (; outputs.Next(); --numPending)
                    {
                        if (isDeleted((int)outputs.Current.Key.kfield1))
                        {
                            Assert.IsFalse(outputs.Current.Status.Found);
                            continue;
                        }
                        Assert.IsTrue(outputs.Current.Status.Found);
                        Assert.AreEqual(outputs.Current.Key.kfield1, outputs.Current.Output.value.vfield1);
                        Assert.AreEqual(outputs.Current.Key.kfield2, outputs.Current.Output.value.vfield2);
                    }
                }
                Assert.AreEqual(numPending, 0);
            }

            for (int i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, isDeleted(i) ? 1 : 0, 0);
                if (!status.IsPending)
                {
                    if (isDeleted(i))
                    {
                        Assert.IsFalse(status.Found);
                        continue;
                    }
                    Assert.IsTrue(status.Found);
                    Assert.AreEqual(value.vfield1, output.value.vfield1);
                    Assert.AreEqual(value.vfield2, output.value.vfield2);
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

        public void BlittableLogCompactionTest1([Values] CompactionType compactionType, [Values(ConcurrencyControlMode.LockTable)] ConcurrencyControlMode concurrencyControlMode)
        {
            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());

            const int totalRecords = 2_000;
            var start = store.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords - 1000)
                    compactUntil = store.Log.TailAddress;

                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            store.Log.FlushAndEvict(wait: true);
            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();

            Assert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read all keys - all should be present
            VerifyRead(session, totalRecords, key => false);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        public void BlittableLogCompactionTest2([Values] CompactionType compactionType, [Values(ConcurrencyControlMode.LockTable)] ConcurrencyControlMode concurrencyControlMode,
                                                [Values(HashModulo.NoMod, HashModulo.Hundred)] HashModulo hashMod)
        {
            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());

            const int totalRecords = 2_000;
            var start = store.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords - 1000)
                    compactUntil = store.Log.TailAddress;

                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
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
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            Assert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read all keys - all should be present
            VerifyRead(session, totalRecords, key => false);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        public void BlittableLogCompactionTest3([Values] CompactionType compactionType, [Values(ConcurrencyControlMode.LockTable)] ConcurrencyControlMode concurrencyControlMode)
        {
            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());

            const int totalRecords = 2_000;
            var start = store.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = store.Log.TailAddress;

                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);

                if (i % 8 == 0)
                {
                    int j = i / 4;
                    key1 = new KeyStruct { kfield1 = j, kfield2 = j + 1 };
                    session.Delete(ref key1, 0, 0);
                }
            }

            var tail = store.Log.TailAddress;
            compactUntil = session.Compact(compactUntil, compactionType);
            store.Log.Truncate();
            Assert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read all keys - all should be present except those we deleted
            VerifyRead(session, totalRecords, key => (key < totalRecords / 4) && (key % 2 == 0));
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        [Category("Smoke")]

        public void BlittableLogCompactionCustomFunctionsTest1([Values] CompactionType compactionType, [Values(ConcurrencyControlMode.LockTable)] ConcurrencyControlMode concurrencyControlMode)
        {
            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());

            InputStruct input = default;

            const int totalRecords = 2000;
            var start = store.Log.TailAddress;
            var compactUntil = 0L;

            for (var i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = store.Log.TailAddress;

                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            var tail = store.Log.TailAddress;

            // Only leave records with even vfield1
            compactUntil = session.Compact(compactUntil, compactionType, default(EvenCompactionFunctions));
            store.Log.Truncate();
            Assert.AreEqual(compactUntil, store.Log.BeginAddress);

            // Read 2000 keys - all should be present
            for (var i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var ctx = (i < (totalRecords / 2) && (i % 2 != 0)) ? 1 : 0;

                var status = session.Read(ref key1, ref input, ref output, ctx, 0);
                if (status.IsPending)
                {
                    Assert.IsTrue(session.CompletePendingWithOutputs(out var outputs, wait: true));
                    (status, output) = GetSinglePendingResult(outputs);
                }

                if (ctx == 0)
                {
                    Assert.IsTrue(status.Found);
                    Assert.AreEqual(value.vfield1, output.value.vfield1);
                    Assert.AreEqual(value.vfield2, output.value.vfield2);
                }
                else
                {
                    Assert.IsFalse(status.Found);
                }
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Compaction")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void BlittableLogCompactionCustomFunctionsTest2([Values] CompactionType compactionType, [Values] bool flushAndEvict,
                                                                [Values(ConcurrencyControlMode.LockTable)] ConcurrencyControlMode concurrencyControlMode)
        {
            // Update: irrelevant as session compaction no longer uses Copy/CopyInPlace
            // This test checks if CopyInPlace returning false triggers call to Copy

            using var session = store.NewSession<InputStruct, OutputStruct, int, FunctionsCompaction>(new FunctionsCompaction());

            var key = new KeyStruct { kfield1 = 100, kfield2 = 101 };
            var value = new ValueStruct { vfield1 = 10, vfield2 = 20 };
            var input = default(InputStruct);
            var output = default(OutputStruct);

            session.Upsert(ref key, ref value, 0, 0);
            var status = session.Read(ref key, ref input, ref output, 0, 0);
            Debug.Assert(status.Found);

            store.Log.Flush(true);

            value = new ValueStruct { vfield1 = 11, vfield2 = 21 };
            session.Upsert(ref key, ref value, 0, 0);
            status = session.Read(ref key, ref input, ref output, 0, 0);
            Debug.Assert(status.Found);

            if (flushAndEvict)
                store.Log.FlushAndEvict(true);
            else
                store.Log.Flush(true);

            var compactUntil = session.Compact(store.Log.TailAddress, compactionType);
            store.Log.Truncate();

            status = session.Read(ref key, ref input, ref output, 0, 0);
            if (status.IsPending)
            {
                Assert.IsTrue(session.CompletePendingWithOutputs(out var outputs, wait: true));
                (status, output) = GetSinglePendingResult(outputs);
            }

            Assert.IsTrue(status.Found);
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
        }

        private struct EvenCompactionFunctions : ICompactionFunctions<KeyStruct, ValueStruct>
        {
            public bool IsDeleted(ref KeyStruct key, ref ValueStruct value) => value.vfield1 % 2 != 0;
        }
    }
}