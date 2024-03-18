// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

#pragma warning disable IDE0060 // Remove unused parameter: used by Setup

namespace Tsavorite.test.LockTests
{
    [TestFixture]
    internal class AdvancedLockTests
    {
        const int numKeys = 500;
        const int valueAdd = 1000000;
        const int mod = 100;

        public struct Input
        {
            internal bool doTest;   // For Populate() we don't want to do this
            internal bool expectSingleReader;
            internal int updatedValue;

            public override readonly string ToString() => $"doTest {doTest}, expectSingleReader {expectSingleReader}, updatedValue {updatedValue}";
        }

        [Flags]
        internal enum LockTestMode
        {
            UpdateAfterCTT = 1,
            CTTAfterUpdate = 2,
        }

        internal class Functions : FunctionsBase<int, int, Input, int, Empty>, IDisposable
        {
            // The update event is manual reset because it will retry and thus needs to remain set.
            internal readonly ManualResetEvent updateEvent = new(initialState: false);
            internal readonly AutoResetEvent readEvent = new(initialState: false);

            public override bool SingleWriter(ref int key, ref Input input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                // SingleWriter is shared with Read so needs the Reason
                if (input.doTest && reason == WriteReason.Upsert)
                    updateEvent.WaitOne();
                output = dst = src;
                return true;
            }

            public override void PostSingleWriter(ref int key, ref Input input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                // SingleWriter is shared with Read so needs the Reason
                base.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
                if (!input.doTest)
                    return;
                if (reason == WriteReason.Upsert)
                    readEvent.Set();
                else    // CopyToTail or CopyToReadCache
                    updateEvent.Set();
            }

            public override bool ConcurrentWriter(ref int key, ref Input input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            {
                // No Wait or Set operations are done here; that's all been done by the time we get to this point
                output = dst = src;
                return true;
            }

            public override bool CopyUpdater(ref int key, ref Input input, ref int oldValue, ref int newValue, ref int output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                if (input.doTest)
                    updateEvent.WaitOne();
                output = newValue = input.updatedValue;
                return true;
            }

            public override void PostCopyUpdater(ref int key, ref Input input, ref int oldValue, ref int newValue, ref int output, ref RMWInfo rmwInfo)
            {
                base.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
                if (input.doTest)
                    readEvent.Set();
            }

            public override bool InPlaceUpdater(ref int key, ref Input input, ref int value, ref int output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                // No Wait or Set operations are done here; that's all been done by the time we get to this point
                output = value = input.updatedValue;
                return true;
            }

            public override bool SingleReader(ref int key, ref Input input, ref int value, ref int output, ref ReadInfo readInfo)
            {
                // We are here if we are doing the initial read, before Upsert's insert-at-tail has taken place; this means we are testing that Insert waits for the
                // Read to Read and then CTT, and then signal the Insert to proceed (and update the newly-CTT'd record).
                Assert.IsTrue(input.doTest && input.expectSingleReader, $"SingleReader: Key = {key}");
                output = value;
                return true;
            }

            public override bool ConcurrentReader(ref int key, ref Input input, ref int value, ref int output, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            {
                // We are here if we are doing the read of the Upsert-inserted updated value; this means we are testing that Read waits for the
                // Upsert to complete and then signal the Read to proceed (and read the newly-inserted record).
                Assert.IsFalse(input.doTest && input.expectSingleReader, $"ConcurrentReader: Key = {key}");
                output = value;
                return true;
            }

            public void Dispose()
            {
                updateEvent.Dispose();
                readEvent.Dispose();
            }
        }

        internal class ChainComparer : ITsavoriteEqualityComparer<int>
        {
            readonly int mod;

            internal ChainComparer(int mod) => this.mod = mod;

            public bool Equals(ref int k1, ref int k2) => k1 == k2;

            public long GetHashCode64(ref int k) => k % mod;
        }

        private TsavoriteKV<int, int> store;
        private ClientSession<int, int, Input, int, Empty, Functions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(MethodTestDir + "/GenericStringTests.log", deleteOnClose: true);
            var readCacheSettings = new ReadCacheSettings { MemorySizeBits = 15, PageSizeBits = 9 };

            var concurrencyControlMode = ConcurrencyControlMode.None;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ConcurrencyControlMode ccm)
                {
                    concurrencyControlMode = ccm;
                    continue;
                }
            }

            store = new TsavoriteKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, ReadCacheSettings = readCacheSettings },
                comparer: new ChainComparer(mod), concurrencyControlMode: concurrencyControlMode);
            session = store.NewSession<Input, int, Empty, Functions>(new Functions());
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

        void Populate(bool evict = false)
        {
            using var session = store.NewSession<Input, int, Empty, Functions>(new Functions());

            for (int key = 0; key < numKeys; key++)
                session.Upsert(key, key + valueAdd);
            session.CompletePending(true);
            if (evict)
                store.Log.FlushAndEvict(wait: true);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(LockTestCategory)]
        //[Repeat(100)]
        public async ValueTask SameKeyInsertAndCTTTest([Values(ConcurrencyControlMode.None, ConcurrencyControlMode.RecordIsolation /* Standard will hang */)] ConcurrencyControlMode concurrencyControlMode,
                                                       [Values(ReadCopyTo.ReadCache, ReadCopyTo.MainLog)] ReadCopyTo readCopyTo, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");
            Populate(evict: true);
            using Functions functions = new();
            using var readSession = store.NewSession<Input, int, Empty, Functions>(functions);
            using var updateSession = store.NewSession<Input, int, Empty, Functions>(functions);
            var iter = 0;

            // This test ensures that we handle the two cases, which are timing-dependent so we use events to force the sequence. "Update" refers to either
            // Upsert ((Post)SingleWriter) or RMW ((Post)CopyUpdater). Note that "CTT" is loosely used here to refer to both copies to MainLog and to ReadCache.
            //  UpdateAfterCTT: an Update is in-flight when a Read is CTT'd to the tail or readcache; the Update's insert should fail CAS and retry successfully,
            //    and the updated value should be returned in its output; the Read completed first, so should still have the old value.
            //    - Wait on the updateEvent in Upsert/SingleWriter or CopyUpdater; Set the updateEvent in Read/PostSingleWriter as part of CopyToTail.
            //    - In this case, ContinuePendingRead calls SingleReader (on the record in the request, before calling CopyToTail to add it) so returns the old value.
            //  CTTAfterUpdate: a ReadCTT is in-flight when an Insert is added to the tail; the updated value should be returned in output from both Upsert/RMW and Read.
            //    Read must go pending before Upsert/RMW starts, then wait until Upsert/RMW is complete, so it will find the newly inserted record rather than doing CTT.
            //    - Stage 0: Wait on the updateEvent in Upsert/SingleWriter or CopyUpdater; the updateEvent is Set in "second action" after calling Read but before calling CompletePending.
            //    - Stage 1: Wait on the readEvent in the "second action"; the readEvent is Set in Upsert/PostSingleWriter or PostCopyUpdater after Upsert/RMW write to tail.
            //    - In this case, ContinuePendingRead calls ConcurrentReader (on the newly inserted record; it does *not* call SingleReader)
            // So both of these resolve to having the Functions Wait in Upsert/SingleWriter or CopyUpdater, and Set in (Upsert_or_RMW or Read, depending on testMode)/PostSingleWriter.

            Input createInput(int key, out LockTestMode testMode)
            {
                testMode = (key & 1) == 0 ? LockTestMode.UpdateAfterCTT : LockTestMode.CTTAfterUpdate;
                return new()
                {
                    doTest = true,
                    expectSingleReader = testMode == LockTestMode.UpdateAfterCTT,
                    updatedValue = key + valueAdd * 2
                };
            }

            await DoTwoThreadRandomKeyTest(numKeys, doRandom: false,
                key =>
                {
                    int output = -1;
                    Input input = createInput(key, out var testMode);
                    Status status;

                    bool expectedFound;
                    if (updateOp == UpdateOp.Upsert)
                    {
                        // Upsert of a record not in mutable log returns NOTFOUND because it creates a new record.
                        expectedFound = readCopyTo == ReadCopyTo.MainLog && testMode == LockTestMode.UpdateAfterCTT;
                        status = updateSession.Upsert(ref key, ref input, ref input.updatedValue, ref output);
                    }
                    else
                    {
                        // RMW will always find and update or copyupdate.
                        expectedFound = true;
                        status = updateSession.RMW(ref key, ref input, ref output);

                        // Unlike Upsert, RMW may go pending (depending whether CopyToTail|ReadCache completes first). We don't care about that here;
                        // the test is that we properly handle either the CAS at the tail by retrying, or invalidate the readcache record.
                        if (status.IsPending)
                        {
                            updateSession.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            (status, output) = GetSinglePendingResult(completedOutputs);
                        }
                    }

                    // Insert that replaces a ReadCache record returns NOTFOUND because it creates a new record
                    Assert.AreEqual(expectedFound, status.Found, $"First action: Key = {key}, status = {status}, testMode {testMode}");
                    Assert.AreEqual(input.updatedValue, output, $"First action: Key = {key}, iter = {iter}, testMode {testMode}");
                },
                key =>
                {
                    int output = -2;
                    Input input = createInput(key, out var testMode);
                    ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.Device, readCopyTo) };

                    // This will copy to ReadCache or MainLog tail, and the test is trying to cause a race with the above Upsert.
                    var status = readSession.Read(ref key, ref input, ref output, ref readOptions, out _);

                    // For this test to work deterministically, the read must go pending
                    Assert.IsTrue(status.IsPending, $"Second action: Key = {key}, status = {status}, testMode {testMode}");

                    var expectedOutput = key + valueAdd;
                    if (testMode == LockTestMode.CTTAfterUpdate)
                    {
                        // We've gone pending on Read; now tell Update to continue and write at tail.
                        functions.updateEvent.Set();

                        // We must wait here until Update is complete, so ContinuePendingRead finds the newly-inserted record.
                        functions.readEvent.WaitOne();

                        // We will do ConcurrentReader of the newly-inserted (and therefore updated) value.
                        expectedOutput += valueAdd;
                    }

                    readSession.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                    Assert.AreEqual(expectedOutput, output, $"Second action: Key = {key}, iter = {iter}, testMode {testMode}");
                },
                key =>
                {
                    int output = -3;
                    var status = readSession.Read(ref key, ref output);

                    // This should not have gone pending, since the Insert or Read updated at the log tail. There should be no valid readcache
                    // record; if there is, the test for updated value will fail.
                    Assert.IsTrue(status.Found, $"Verification: Key = {key}, status = {status}");
                    Assert.AreEqual(key + valueAdd * 2, output, $"Verification: Key = {key}, iter = {iter}");
                    functions.updateEvent.Reset();
                    functions.readEvent.Reset();
                    ++iter;
                }
            );
        }

        [TestFixture]
        class LockRecoveryTests
        {
            const int numKeys = 5000;

            string checkpointDir;

            private TsavoriteKV<int, int> store1;
            private TsavoriteKV<int, int> store2;
            private IDevice log;

            [SetUp]
            public void Setup()
            {
                DeleteDirectory(MethodTestDir, wait: true);
                checkpointDir = MethodTestDir + $"/checkpoints";
                log = Devices.CreateLogDevice(MethodTestDir + "/test.log", deleteOnClose: true);

                store1 = new TsavoriteKV<int, int>(128,
                    logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                    checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                    );

                store2 = new TsavoriteKV<int, int>(128,
                    logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                    checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                    );
            }

            [TearDown]
            public void TearDown()
            {
                store1?.Dispose();
                store1 = null;
                store2?.Dispose();
                store2 = null;
                log?.Dispose();
                log = null;

                DeleteDirectory(MethodTestDir);
            }
        }
    }
}