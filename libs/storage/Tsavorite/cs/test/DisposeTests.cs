#pragma warning disable IDE0055
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if false

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.core.Utility;
using static Tsavorite.test.TestUtils;

#pragma warning disable IDE0060 // Remove unused parameter; used for Setup only

namespace Tsavorite.test.Dispose
{
    [TestFixture]
    internal class DisposeTests
    {
        // MyKey and MyValue are classes; we want to be sure we are getting the right Keys and Values to Dispose().
        private TsavoriteKV<MyKey, MyValue> store;
        private IDevice log, objlog;

        // Events to coordinate forcing CAS failure (by appending a new item), etc.
        private SemaphoreSlim sutGate;      // Session Under Test
        private SemaphoreSlim otherGate;    // Other session that inserts a colliding value

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            sutGate = new(0);
            otherGate = new(0);

            log = Devices.CreateLogDevice(MethodTestDir + "/ObjectTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(MethodTestDir + "/ObjectTests.obj.log", deleteOnClose: true);

            LogSettings logSettings = new() { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 10 };
            var concurrencyControlMode = ConcurrencyControlMode.None;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCopyDestination dest)
                {
                    if (dest == ReadCopyDestination.ReadCache)
                        logSettings.ReadCacheSettings = new() { PageSizeBits = 12, MemorySizeBits = 22 };
                    continue;
                }
                if (arg is ConcurrencyControlMode ccm)
                {
                    concurrencyControlMode = ccm;
                    continue;
                }
            }

            store = new TsavoriteKV<MyKey, MyValue>(128, logSettings: logSettings, comparer: new MyKeyComparer(),
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() },
                concurrencyControlMode: concurrencyControlMode   // Warning: ConcurrencyControlMode.LockTable will deadlock with X locks as both keys map to the same keyHash
                );
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            DeleteDirectory(MethodTestDir);
        }

        // This is passed to the TsavoriteKV ctor to override the default one. This lets us use a different key for the colliding
        // CAS; we can't use the same key because Readonly-region handling in the first session Seals the to-be-transferred record,
        // so the second session would loop forever while the first session waits for the collision to be written.
        class MyKeyComparer : ITsavoriteEqualityComparer<MyKey>
        {
            public long GetHashCode64(ref MyKey key) => Utility.GetHashCode(key.key % TestKey);

            public bool Equals(ref MyKey k1, ref MyKey k2) => k1.key == k2.key;
        }

        const int TestKey = 111;
        const int TestCollidingKey = TestKey * 2;
        const int TestCollidingKey2 = TestKey * 3;
        const int TestInitialValue = 3333;
        const int TestUpdatedValue = 5555;
        const int TestCollidingValue = 7777;
        const int TestCollidingValue2 = 9999;

        internal enum DisposeHandler
        {
            None,
            SingleWriter,
            CopyUpdater,
            InitialUpdater,
            SingleDeleter,
            DeserializedFromDisk,
        }

        public class DisposeFunctions : FunctionsBase<MyKey, MyValue, MyInput, MyOutput, Empty>
        {
            private readonly DisposeTests tester;
            internal readonly bool isSUT; // IsSessionUnderTest
            internal Queue<DisposeHandler> handlerQueue = new();
            private bool isRetry;
            private bool isSplice;

            internal DisposeFunctions(DisposeTests tester, bool sut, bool splice = false)
            {
                this.tester = tester;
                isSUT = sut;
                isSplice = splice;
            }

            void WaitForEvent()
            {
                Assert.IsTrue(tester.store.epoch.ThisInstanceProtected(), "This should only be called from IFunctions methods, which are under epoch protection");
                if (isSUT)
                {
                    MyKey key = new() { key = TestKey };
                    tester.store.FindHashBucketEntryForKey(ref key, out var entry);
                    var address = entry.Address;
                    if (isSplice)
                    {
                        // Get the tail entry for this key's hash chain; there should be exactly one readcache entry for this test.
                        Assert.IsTrue(entry.ReadCache, "Expected readcache entry in WaitForEvent pt 1");
                        Assert.GreaterOrEqual(entry.AbsoluteAddress, tester.store.ReadCache.HeadAddress);
                        var physicalAddress = tester.store.readcache.GetPhysicalAddress(entry.AbsoluteAddress);
                        ref RecordInfo recordInfo = ref tester.store.readcache.GetInfo(physicalAddress);
                        address = recordInfo.PreviousAddress;

                        // There should be only one readcache entry for this test. The address we just got may have been kTempInvalidAddress,
                        // and if not then it should have been a pre-FlushAndEvict()ed record.
                        Assert.IsFalse(IsReadCache(address));

                        // Retry will have already inserted something post-FlushAndEvict.
                        Assert.IsTrue(isRetry || address < tester.store.hlog.HeadAddress);
                    }
                    tester.otherGate.Release();
                    tester.sutGate.Wait();

                    tester.store.FindHashBucketEntryForKey(ref key, out entry);

                    // There's a little race where the SUT session could still beat the other session to the CAS
                    if (!isRetry)
                    {
                        if (isSplice)
                        {
                            // If this is not Standard locking, then we use detach-and-reattach logic on the hash chain. That happens after SingleWriter,
                            // so 'other' thread may still be in progress . Wait for it.
                            while (!entry.ReadCache)
                            {
                                Assert.IsFalse(tester.store.LockTable.IsEnabled, "Standard locking should have spliced directly");
                                Thread.Yield();
                                tester.store.FindHashBucketEntryForKey(ref key, out entry);
                            }

                            // We're the thread awaiting the splice, so wait until the address in the last readcache record changes.
                            Assert.IsTrue(entry.ReadCache, "Expected readcache entry in WaitForEvent pt 2");
                            Assert.GreaterOrEqual(entry.AbsoluteAddress, tester.store.ReadCache.HeadAddress);
                            var physicalAddress = tester.store.readcache.GetPhysicalAddress(entry.AbsoluteAddress);
                            ref RecordInfo recordInfo = ref tester.store.readcache.GetInfo(physicalAddress);
                            while (recordInfo.PreviousAddress == address)
                            {
                                // Wait for the splice to happen
                                Thread.Yield();
                            }
                            Assert.IsFalse(IsReadCache(recordInfo.PreviousAddress));
                            Assert.IsTrue(recordInfo.PreviousAddress >= tester.store.hlog.HeadAddress);
                        }
                        else
                        {
                            // We're not the splice thread, so wait until the address in the hash entry changes.
                            while (entry.Address == address)
                            {
                                Thread.Yield();
                                tester.store.FindHashBucketEntryForKey(ref key, out entry);
                            }
                        }
                    }
                    isRetry = true;     // the next call will be from RETRY_NOW
                }
            }

            void SignalEvent()
            {
                // Let the SUT proceed, which will trigger a RETRY_NOW due to the failed CAS, so we need to release for the second wait as well.
                // Release with a count of 2 to handle the attempt it's currently blocked on and the subsequent retry.
                if (!isSUT)
                    tester.sutGate.Release(2);
            }

            public override bool SingleWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                WaitForEvent();
                dst = src;
                SignalEvent();
                return true;
            }

            public override bool InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo)
            {
                WaitForEvent();
                value = new MyValue { value = input.value };
                SignalEvent();
                return true;
            }

            public override bool CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RMWInfo rmwInfo)
            {
                WaitForEvent();
                newValue = new MyValue { value = oldValue.value + input.value };
                SignalEvent();
                return true;
            }

            public override bool SingleDeleter(ref MyKey key, ref MyValue value, ref DeleteInfo deleteInfo)
            {
                WaitForEvent();
                base.SingleDeleter(ref key, ref value, ref deleteInfo);
                SignalEvent();
                return true;
            }

            public override bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo)
            {
                value.value += input.value;
                return true;
            }

            public override bool NeedCopyUpdate(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyOutput output, ref RMWInfo rmwInfo) => true;

            public override bool ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo)
            {
                Assert.Fail("ConcurrentReader should not be called for this test");
                return true;
            }

            public override bool ConcurrentWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo)
            {
                dst.value = src.value;
                return true;
            }

            public override void RMWCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                if (isSUT)
                {
                    Assert.IsTrue(status.Found, status.ToString());
                    Assert.IsTrue(status.Record.CopyUpdated, status.ToString()); // InPlace due to RETRY_NOW after CAS failure
                }
                else
                {
                    Assert.IsTrue(status.NotFound, status.ToString());
                    Assert.IsTrue(status.Record.Created, status.ToString());
                }
            }

            public override bool SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo)
            {
                dst.value = value;
                return true;
            }

            public override void DisposeSingleWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                Assert.AreEqual(TestKey, key.key);
                Assert.AreEqual(TestInitialValue, src.value);
                Assert.AreEqual(TestInitialValue, dst.value);  // dst has been populated
                handlerQueue.Enqueue(DisposeHandler.SingleWriter);
            }

            public override void DisposeCopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RMWInfo rmwInfo)
            {
                Assert.AreEqual(TestKey, key.key);
                Assert.AreEqual(TestInitialValue, oldValue.value);
                Assert.AreEqual(TestInitialValue + TestUpdatedValue, newValue.value);
                handlerQueue.Enqueue(DisposeHandler.CopyUpdater);
            }

            public override void DisposeInitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo)
            {
                Assert.AreEqual(TestKey, key.key);
                Assert.AreEqual(TestInitialValue, value.value);
                handlerQueue.Enqueue(DisposeHandler.InitialUpdater);
            }

            public override void DisposeSingleDeleter(ref MyKey key, ref MyValue value, ref DeleteInfo deleteInfo)
            {
                Assert.AreEqual(TestKey, key.key);
                Assert.IsNull(value);   // This is the default value inserted for the Tombstoned record
                handlerQueue.Enqueue(DisposeHandler.SingleDeleter);
            }

            public override void DisposeDeserializedFromDisk(ref MyKey key, ref MyValue value)
            {
                VerifyKeyValueCombo(ref key, ref value);
                handlerQueue.Enqueue(DisposeHandler.DeserializedFromDisk);
            }
        }

        static void VerifyKeyValueCombo(ref MyKey key, ref MyValue value)
        {
            switch (key.key)
            {
                case TestKey:
                    Assert.AreEqual(TestInitialValue, value.value);
                    break;
                case TestCollidingKey:
                    Assert.AreEqual(TestCollidingValue, value.value);
                    break;
                case TestCollidingKey2:
                    Assert.AreEqual(TestCollidingValue2, value.value);
                    break;
                default:
                    Assert.Fail($"Unexpected key: {key.key}");
                    break;
            }
        }

        // Override some things from MyFunctions for our purposes here
        class DisposeFunctionsNoSync : MyFunctions
        {
            internal Queue<DisposeHandler> handlerQueue = new();

            public override bool CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RMWInfo rmwInfo)
            {
                newValue = new MyValue { value = oldValue.value + input.value };
                output.value = newValue;
                return true;
            }

            public override void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(status.Found);
            }

            public override void DisposeDeserializedFromDisk(ref MyKey key, ref MyValue value)
            {
                VerifyKeyValueCombo(ref key, ref value);
                handlerQueue.Enqueue(DisposeHandler.DeserializedFromDisk);
            }
        }

        void DoFlush(FlushMode flushMode)
        {
            switch (flushMode)
            {
                case FlushMode.NoFlush:
                    return;
                case FlushMode.ReadOnly:
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                    return;
                case FlushMode.OnDisk:
                    store.Log.FlushAndEvict(wait: true);
                    return;
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void DisposeSingleWriter2Threads([Values(ConcurrencyControlMode.RecordIsolation, ConcurrencyControlMode.None)] ConcurrencyControlMode concurrencyControlMode)
        {
            var functions1 = new DisposeFunctions(this, sut: true);
            var functions2 = new DisposeFunctions(this, sut: false);

            MyKey key = new() { key = TestKey };
            MyKey collidingKey = new() { key = TestCollidingKey };
            MyValue value = new() { value = TestInitialValue };
            MyValue collidingValue = new() { value = TestCollidingValue };

            void DoUpsert(DisposeFunctions functions)
            {
                using var innerSession = store.NewSession(functions);
                if (functions.isSUT)
                    innerSession.Upsert(ref key, ref value);
                else
                {
                    otherGate.Wait();
                    innerSession.Upsert(ref collidingKey, ref collidingValue);
                }
            }

            var tasks = new[]
            {
                Task.Factory.StartNew(() => DoUpsert(functions1)),
                Task.Factory.StartNew(() => DoUpsert(functions2))
            };

            Task.WaitAll(tasks);

            Assert.AreEqual(DisposeHandler.SingleWriter, functions1.handlerQueue.Dequeue());
            Assert.IsEmpty(functions1.handlerQueue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void DisposeInitialUpdater2Threads([Values(FlushMode.NoFlush, FlushMode.OnDisk)] FlushMode flushMode,
                                                  [Values(ConcurrencyControlMode.RecordIsolation, ConcurrencyControlMode.None)] ConcurrencyControlMode concurrencyControlMode)
        {
            var functions1 = new DisposeFunctions(this, sut: true);
            var functions2 = new DisposeFunctions(this, sut: false);

            MyKey key = new() { key = TestKey };
            MyKey collidingKey = new() { key = TestCollidingKey };
            MyInput input = new() { value = TestInitialValue };
            MyInput collidingInput = new() { value = TestCollidingValue };

            DoFlush(flushMode);

            void DoInsert(DisposeFunctions functions)
            {
                using var session = store.NewSession(functions);
                if (functions.isSUT)
                    session.RMW(ref key, ref input);
                else
                {
                    otherGate.Wait();
                    session.RMW(ref collidingKey, ref collidingInput);
                }
            }

            var tasks = new[]
            {
                Task.Factory.StartNew(() => DoInsert(functions1)),
                Task.Factory.StartNew(() => DoInsert(functions2))
            };
            Task.WaitAll(tasks);

            Assert.AreEqual(DisposeHandler.InitialUpdater, functions1.handlerQueue.Dequeue());
            Assert.IsEmpty(functions1.handlerQueue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void DisposeCopyUpdater2Threads([Values(FlushMode.ReadOnly, FlushMode.OnDisk)] FlushMode flushMode,
                                               [Values(ConcurrencyControlMode.RecordIsolation, ConcurrencyControlMode.None)] ConcurrencyControlMode concurrencyControlMode)
        {
            var functions1 = new DisposeFunctions(this, sut: true);
            var functions2 = new DisposeFunctions(this, sut: false);

            MyKey key = new() { key = TestKey };
            MyKey collidingKey = new() { key = TestCollidingKey };
            {
                using var session = store.NewSession(new DisposeFunctionsNoSync());
                MyValue value = new() { value = TestInitialValue };
                session.Upsert(ref key, ref value);
            }

            // Make it immutable so CopyUpdater is called.
            DoFlush(flushMode);

            void DoUpdate(DisposeFunctions functions)
            {
                using var session = store.NewSession(functions);
                MyInput input = new() { value = functions.isSUT ? TestUpdatedValue : TestCollidingValue };
                if (functions.isSUT)
                    session.RMW(ref key, ref input);
                else
                {
                    otherGate.Wait();
                    session.RMW(ref collidingKey, ref input);
                }
                session.CompletePending(true);
            }

            var tasks = new[]
            {
                Task.Factory.StartNew(() => DoUpdate(functions1)),
                Task.Factory.StartNew(() => DoUpdate(functions2))
            };
            Task.WaitAll(tasks);

            // The way this works for OnDisk is:
            //   SUT sees that the address in the hash entry is below HeadAddress (because everything has been flushed to disk)
            //      SUT records InitialEntryAddress with the original hash entry address
            //      SUT goes pending, gets to InternalContinuePendingRMW, calls CreateNewRecordRMW, which calls CopyUpdater
            //          SUT (in CopyUpdater) signals Other, then blocks
            //   Other calls InternalRMW and also sees that the address in the hash entry is below HeadAddress, so it goes pending
            //      Other gets to InternalContinuePendingRMW, sees its key does not exist, and calls InitialUpdater, which signals SUT
            //      Other returns from InternalContinuePendingRMW, which enqueues DeserializedFromDisk into functions2.handlerQueue
            //   SUT is now unblocked and returns from CopyUpdater. CAS fails due to Other's insertion
            //      SUT does the RETRY loop in InternalContinuePendingRMW
            //      This second loop iteration searches for the record in-memory down to InitialEntryAddress and does not find it.
            //          It verifies that the lower bound of the search guarantees we searched all in-memory records.
            //      Therefore SUT calls CreateNewRecordRMW again, which succeeds.
            //      SUT returns from InternalContinuePendingRMW, which enqueues DeserializedFromDisk into functions1.handlerQueue
            Assert.AreEqual(DisposeHandler.CopyUpdater, functions1.handlerQueue.Dequeue());
            if (flushMode == FlushMode.OnDisk)
            {
                Assert.AreEqual(DisposeHandler.DeserializedFromDisk, functions1.handlerQueue.Dequeue());
                Assert.AreEqual(DisposeHandler.DeserializedFromDisk, functions2.handlerQueue.Dequeue());
            }
            Assert.IsEmpty(functions1.handlerQueue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void DisposeSingleDeleter2Threads([Values(FlushMode.ReadOnly, FlushMode.OnDisk)] FlushMode flushMode,
                                                  [Values(ConcurrencyControlMode.RecordIsolation, ConcurrencyControlMode.None)] ConcurrencyControlMode concurrencyControlMode)
        {
            var functions1 = new DisposeFunctions(this, sut: true);
            var functions2 = new DisposeFunctions(this, sut: false);

            MyKey key = new() { key = TestKey };
            MyKey collidingKey = new() { key = TestCollidingKey };

            {
                using var session = store.NewSession(new DisposeFunctionsNoSync());
                MyValue value = new() { value = TestInitialValue };
                session.Upsert(ref key, ref value);
                MyValue collidingValue = new() { value = TestCollidingValue };
                session.Upsert(ref collidingKey, ref collidingValue);
            }

            // Make it immutable so we don't simply set Tombstone.
            DoFlush(flushMode);

            void DoDelete(DisposeFunctions functions)
            {
                using var innerSession = store.NewSession(functions);
                if (functions.isSUT)
                    innerSession.Delete(ref key);
                else
                {
                    otherGate.Wait();
                    innerSession.Delete(ref collidingKey);
                }
            }

            var tasks = new[]
            {
                Task.Factory.StartNew(() => DoDelete(functions1)),
                Task.Factory.StartNew(() => DoDelete(functions2))
            };

            Task.WaitAll(tasks);

            Assert.AreEqual(DisposeHandler.SingleDeleter, functions1.handlerQueue.Dequeue());
            Assert.IsEmpty(functions1.handlerQueue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void PendingRead([Values] ReadCopyDestination copyDest, [Values(ConcurrencyControlMode.RecordIsolation, ConcurrencyControlMode.None)] ConcurrencyControlMode concurrencyControlMode)

        {
            DoPendingReadInsertTest(copyDest, initialReadCacheInsert: false);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void CopyToTailWithInitialReadCache([Values(ReadCopyDestination.ReadCache)] ReadCopyDestination copyDest,
                                                   [Values(ConcurrencyControlMode.RecordIsolation, ConcurrencyControlMode.None)] ConcurrencyControlMode concurrencyControlMode)
        {
            // We use the ReadCopyDestination.ReadCache parameter so Setup() knows to set up the readcache, but
            // for the actual test it is used only for setup; we execute CopyToTail.
            DoPendingReadInsertTest(ReadCopyDestination.Tail, initialReadCacheInsert: true);
        }

        void DoPendingReadInsertTest(ReadCopyDestination copyDest, bool initialReadCacheInsert)
        {
            MyKey key = new() { key = TestKey };
            MyKey collidingKey2 = new() { key = TestCollidingKey2 };
            MyValue collidingValue2 = new() { value = TestCollidingValue2 };

            using var session = store.NewSession(new DisposeFunctionsNoSync());

            // Do initial insert(s) to set things up
            {
                MyValue value = new() { value = TestInitialValue };
                session.Upsert(ref key, ref value);
                if (initialReadCacheInsert)
                    session.Upsert(ref collidingKey2, ref collidingValue2);
            }

            // FlushAndEvict so we go pending
            DoFlush(FlushMode.OnDisk);

            MyOutput output = new();
            MyInput input = new();

            if (initialReadCacheInsert)
            {
                Assert.IsTrue(session.Read(ref collidingKey2, ref output).IsPending);
                session.CompletePending(wait: true);
            }

            ReadOptions readOptions = new() { CopyFrom = ReadCopyFrom.Device };
            if (copyDest == ReadCopyDestination.Tail)
                readOptions.CopyOptions.CopyTo = ReadCopyTo.MainLog;
            var status = session.Read(ref key, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending, status.ToString());
            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, output) = GetSinglePendingResult(completedOutputs);
            Assert.AreEqual(TestInitialValue, output.value.value);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void DisposePendingRead2Threads([Values] ReadCopyDestination copyDest, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            DoDisposePendingReadInsertTest2Threads(copyDest, initialReadCacheInsert: false);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void DisposeCopyToTailWithInitialReadCache2Threads([Values(ReadCopyDestination.ReadCache)] ReadCopyDestination copyDest, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            // We use the ReadCopyDestination.ReadCache parameter so Setup() knows to set up the readcache, but
            // for the actual test it is used only for setup; we execute CopyToTail.
            DoDisposePendingReadInsertTest2Threads(ReadCopyDestination.Tail, initialReadCacheInsert: true);
        }

        void DoDisposePendingReadInsertTest2Threads(ReadCopyDestination copyDest, bool initialReadCacheInsert)
        {
            var functions1 = new DisposeFunctions(this, sut: true, splice: initialReadCacheInsert);
            var functions2 = new DisposeFunctions(this, sut: false);

            MyKey key = new() { key = TestKey };
            MyKey collidingKey = new() { key = TestCollidingKey };
            MyValue collidingValue = new() { value = TestCollidingValue };
            MyKey collidingKey2 = new() { key = TestCollidingKey2 };
            MyValue collidingValue2 = new() { value = TestCollidingValue2 };

            // Do initial insert(s) to set things up
            {
                using var session = store.NewSession(new DisposeFunctionsNoSync());
                MyValue value = new() { value = TestInitialValue };
                session.Upsert(ref key, ref value);
                session.Upsert(ref collidingKey, ref collidingValue);
                if (initialReadCacheInsert)
                    session.Upsert(ref collidingKey2, ref collidingValue2);
            }

            // FlushAndEvict so we go pending
            DoFlush(FlushMode.OnDisk);

            if (initialReadCacheInsert)
            {
                using var session = store.NewSession(new DisposeFunctionsNoSync());
                MyOutput output = new();
                var status = session.Read(ref collidingKey2, ref output);
                Assert.IsTrue(status.IsPending, status.ToString());
                session.CompletePending(wait: true);
            }

            // We use Read() only here (not Upsert()), so we have only read locks and thus do not self-deadlock with an XLock on the colliding bucket.
            void DoRead(DisposeFunctions functions)
            {
                MyOutput output = new();
                MyInput input = new();
                ReadOptions readOptions = new() { CopyFrom = ReadCopyFrom.Device };
                if (copyDest == ReadCopyDestination.Tail)
                    readOptions.CopyOptions.CopyTo = ReadCopyTo.MainLog;

                using var session = store.NewSession(functions);
                if (functions.isSUT)
                {
                    var status = session.Read(ref key, ref input, ref output, ref readOptions, out _);
                    Assert.IsTrue(status.IsPending, status.ToString());
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                    Assert.IsTrue(status.Found, status.ToString());
                    Assert.AreEqual(TestInitialValue, output.value.value);
                }
                else
                {
                    otherGate.Wait();
                    var status = session.Read(ref collidingKey, ref input, ref output, ref readOptions, out _);
                    Assert.IsTrue(status.IsPending, status.ToString());
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                    Assert.IsTrue(status.Found, status.ToString());
                    Assert.AreEqual(TestCollidingValue, output.value.value);
                }
            }

            var tasks = new[]
            {
                Task.Factory.StartNew(() => DoRead(functions1)),
                Task.Factory.StartNew(() => DoRead(functions2))
            };
            Task.WaitAll(tasks);

            if (store.LockTable.IsEnabled || !initialReadCacheInsert)    // This allows true splice, so we generated a conflict.
                Assert.AreEqual(DisposeHandler.SingleWriter, functions1.handlerQueue.Dequeue());
            Assert.AreEqual(DisposeHandler.DeserializedFromDisk, functions1.handlerQueue.Dequeue());
            Assert.IsEmpty(functions1.handlerQueue);
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void DisposePendingReadWithNoInsertTest([Values(ConcurrencyControlMode.RecordIsolation, ConcurrencyControlMode.None)] ConcurrencyControlMode concurrencyControlMode)
        {
            var functions = new DisposeFunctionsNoSync();

            MyKey key = new() { key = TestKey };
            MyValue value = new() { value = TestInitialValue };

            // Do initial insert
            using var session = store.NewSession(functions);
            session.Upsert(ref key, ref value);

            // FlushAndEvict so we go pending
            DoFlush(FlushMode.OnDisk);

            MyOutput output = new();
            var status = session.Read(ref key, ref output);
            Assert.IsTrue(status.IsPending, status.ToString());
            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, output) = GetSinglePendingResult(completedOutputs);
            Assert.AreEqual(TestInitialValue, output.value.value);

            Assert.AreEqual(DisposeHandler.DeserializedFromDisk, functions.handlerQueue.Dequeue());
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void DisposePendingRmwWithNoConflictTest([Values(ConcurrencyControlMode.RecordIsolation, ConcurrencyControlMode.None)] ConcurrencyControlMode concurrencyControlMode)
        {
            var functions = new DisposeFunctionsNoSync();

            MyKey key = new() { key = TestKey };
            MyValue value = new() { value = TestInitialValue };

            // Do initial insert
            using var session = store.NewSession(functions);
            session.Upsert(ref key, ref value);

            // FlushAndEvict so we go pending
            DoFlush(FlushMode.OnDisk);

            MyInput input = new() { value = TestUpdatedValue };
            MyOutput output = new();
            var status = session.RMW(ref key, ref input, ref output);
            Assert.IsTrue(status.IsPending, status.ToString());
            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, output) = GetSinglePendingResult(completedOutputs);
            Assert.AreEqual(TestInitialValue + TestUpdatedValue, output.value.value);

            Assert.AreEqual(DisposeHandler.DeserializedFromDisk, functions.handlerQueue.Dequeue());
        }
    }
}
#endif
#pragma warning restore IDE0055