// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.LockContentionHandling
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordTriggers>;

    [TestFixture]
    internal class LockContentionHandlerTests : TestBase
    {
        const long LockedKey = 1;
        const long OriginalValue = 10;
        static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);
        static readonly long[] PinnedLockedKey = GC.AllocateArray<long>(1, pinned: true);

        public enum Operation { Read, Upsert, RMW, Delete }

        sealed class AbandonedException : Exception { }

        /// <summary>Records the contention it is handed, then waits it out or abandons the operation.</summary>
        sealed class RecordingHandler(bool abandon) : ILockContentionHandler
        {
            internal readonly ManualResetEventSlim Invoked = new();
            internal int Calls;
            internal long KeyHash;
            internal bool Exclusive;
            internal bool WasContended;
            internal bool EpochHeldInHandler;
            internal LightEpoch Epoch;

            public void OnLockContended(in LockContention contention)
            {
                _ = Interlocked.Increment(ref Calls);
                KeyHash = contention.KeyHash;
                Exclusive = contention.Exclusive;
                WasContended = contention.IsContended;
                EpochHeldInHandler = Epoch.ThisInstanceProtected();
                Invoked.Set();

                if (abandon)
                    throw new AbandonedException();
                while (contention.IsContended)
                    Thread.Sleep(1);
            }
        }

        private TsavoriteKV<LongStoreFunctions, LongAllocator> store;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                PageSize = 1L << 12,
                LogMemorySize = 1L << 22
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordTriggers.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            OnTearDown();
        }

        ClientSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions, LongStoreFunctions, LongAllocator> NewSession()
            => store.NewSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());

        static Status Upsert(BasicContext<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions, LongStoreFunctions, LongAllocator> context, long key, long value)
            => context.Upsert(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), SpanByte.FromPinnedVariable(ref value));

        static Status Read(BasicContext<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions, LongStoreFunctions, LongAllocator> context, long key, out long value)
        {
            value = 0;
            return context.Read(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), ref value);
        }

        static Status Run(BasicContext<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions, LongStoreFunctions, LongAllocator> context, Operation operation, long key, long value)
        {
            var keySpan = TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key));
            switch (operation)
            {
                case Operation.Read:
                    var output = 0L;
                    return context.Read(keySpan, ref output);
                case Operation.Upsert:
                    return context.Upsert(keySpan, SpanByte.FromPinnedVariable(ref value));
                case Operation.RMW:
                    return context.RMW(keySpan, ref value);
                default:
                    return context.Delete(keySpan);
            }
        }

        long HashOf(long key) => store.storeFunctions.GetKeyHashCode64(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)));

        /// <summary>Holds <see cref="LockedKey"/> as a transaction would, until disposed.</summary>
        sealed class HeldLock : IDisposable
        {
            readonly ClientSession<TestSpanByteKey, long, long, Empty, SimpleLongSimpleFunctions, LongStoreFunctions, LongAllocator> session;
            readonly FixedLengthTransactionalKeyStruct[] keys;
            bool released;

            internal HeldLock(LockContentionHandlerTests tests, LockType lockType)
            {
                session = tests.NewSession();
                var context = session.TransactionalContext;
                context.BeginTransaction();
                PinnedLockedKey[0] = LockedKey;
                keys = [new FixedLengthTransactionalKeyStruct(MemoryMarshal.AsBytes(PinnedLockedKey.AsSpan()), lockType, context)];
                context.Lock<FixedLengthTransactionalKeyStruct>(keys);
            }

            public void Dispose()
            {
                if (released)
                    return;
                released = true;
                var context = session.TransactionalContext;
                context.Unlock<FixedLengthTransactionalKeyStruct>(keys);
                context.EndTransaction();
                session.Dispose();
            }
        }

        /// <summary>
        /// Holds the store's epoch on a thread of its own until disposed, so that the log cannot finish making read-only
        /// the records it has just placed below <see cref="LogAccessor{TStoreFunctions, TAllocator}.ReadOnlyAddress"/>.
        /// An update of such a record retries, for a reason other than a latch, until the epoch is released.
        /// </summary>
        sealed class HeldEpoch : IDisposable
        {
            readonly ManualResetEventSlim release = new();
            readonly Thread thread;

            internal HeldEpoch(LightEpoch epoch)
            {
                using var held = new ManualResetEventSlim();
                thread = new Thread(() =>
                {
                    epoch.Resume();
                    held.Set();
                    release.Wait();
                    epoch.Suspend();
                });
                thread.Start();
                held.Wait();
            }

            public void Dispose()
            {
                release.Set();
                thread.Join();
                release.Dispose();
            }
        }

        /// <summary>
        /// Has the log make the record at <paramref name="address"/> read-only, returning with the epoch held so that the
        /// record stays where an update of it must retry.
        /// </summary>
        HeldEpoch HoldRecordWhereAnUpdateRetries(long address)
        {
            var heldEpoch = new HeldEpoch(store.epoch);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: false);
            ClassicAssert.LessOrEqual(store.Log.SafeReadOnlyAddress, address, "The log finished making the record read-only");
            ClassicAssert.Greater(store.Log.ReadOnlyAddress, address, "The log did not start making the record read-only");
            return heldEpoch;
        }

        [Test]
        [Category(LockTestCategory), Category(SmokeTestCategory)]
        public void AnOperationOnALatchedKeyIsHandedToTheHandlerAndRetriedWhenItReturns([Values] Operation operation, [Values(LockType.Shared, LockType.Exclusive)] LockType heldLock)
        {
            using var populate = NewSession();
            _ = Upsert(populate.BasicContext, LockedKey, OriginalValue);

            var handler = new RecordingHandler(abandon: false) { Epoch = store.epoch };
            using var session = NewSession();
            session.LockContentionHandler = handler;

            // A shared latch conflicts only with an operation that needs the bucket exclusively
            var conflicts = heldLock == LockType.Exclusive || operation != Operation.Read;

            var holder = new HeldLock(this, heldLock);
            Status status = default;
            var worker = new Thread(() => status = Run(session.BasicContext, operation, LockedKey, OriginalValue + 1));
            bool invoked;
            try
            {
                worker.Start();
                invoked = handler.Invoked.Wait(conflicts ? Patience : TimeSpan.FromMilliseconds(200));
            }
            finally
            {
                holder.Dispose();
                ClassicAssert.IsTrue(worker.Join(Patience), "The operation did not complete once the lock was released");
            }

            ClassicAssert.AreEqual(conflicts, invoked, "Whether the handler was invoked");
            ClassicAssert.IsFalse(status.IsPending);
            if (!conflicts)
                return;
            ClassicAssert.AreEqual(HashOf(LockedKey), handler.KeyHash);
            ClassicAssert.AreEqual(operation != Operation.Read, handler.Exclusive);
            ClassicAssert.IsTrue(handler.WasContended);
            ClassicAssert.IsFalse(handler.EpochHeldInHandler, "The handler ran with the epoch held");

            ClassicAssert.AreEqual(operation != Operation.Delete, Read(populate.BasicContext, LockedKey, out var value).Found);
            if (operation != Operation.Delete)
                ClassicAssert.AreEqual(operation == Operation.Read ? OriginalValue : OriginalValue + 1, value);
        }

        [Test]
        [Category(LockTestCategory), Category(SmokeTestCategory)]
        public void AHandlerThatThrowsAbandonsTheOperationWithTheEpochReleasedAndTheStoreUnchanged([Values(Operation.Upsert, Operation.RMW, Operation.Delete)] Operation operation)
        {
            using var populate = NewSession();
            _ = Upsert(populate.BasicContext, LockedKey, OriginalValue);

            var handler = new RecordingHandler(abandon: true) { Epoch = store.epoch };
            using var session = NewSession();
            session.LockContentionHandler = handler;

            var holder = new HeldLock(this, LockType.Exclusive);
            Exception thrown = null;
            var epochHeldAfterThrow = true;
            var worker = new Thread(() =>
            {
                try
                {
                    _ = Run(session.BasicContext, operation, LockedKey, OriginalValue + 1);
                }
                catch (Exception ex)
                {
                    thrown = ex;
                }
                epochHeldAfterThrow = store.epoch.ThisInstanceProtected();
            });
            bool abandoned;
            try
            {
                worker.Start();
                abandoned = worker.Join(Patience);
            }
            finally
            {
                holder.Dispose();
                ClassicAssert.IsTrue(worker.Join(Patience), "The operation did not complete once the lock was released");
            }

            ClassicAssert.IsTrue(abandoned, "The operation waited for the lock instead of abandoning");
            ClassicAssert.IsInstanceOf<AbandonedException>(thrown);
            ClassicAssert.IsFalse(epochHeldAfterThrow, "The epoch is still held after the operation was abandoned");
            ClassicAssert.AreEqual(1, handler.Calls);
            ClassicAssert.IsTrue(Read(populate.BasicContext, LockedKey, out var value).Found);
            ClassicAssert.AreEqual(OriginalValue, value, "An abandoned operation changed the store");

            // Uncontended, the same session completes the operation without consulting the handler
            ClassicAssert.IsFalse(Run(session.BasicContext, operation, LockedKey, OriginalValue + 1).IsPending);
            ClassicAssert.AreEqual(1, handler.Calls);
            ClassicAssert.AreEqual(operation != Operation.Delete, Read(populate.BasicContext, LockedKey, out value).Found);
            if (operation != Operation.Delete)
                ClassicAssert.AreEqual(OriginalValue + 1, value);
        }

        [Test]
        [Category(LockTestCategory), Category(SmokeTestCategory)]
        public void ATransactionalLockOnALatchedKeyWaitsInTheHandlerHoldingNoneOfItsKeys([Values] bool abandon, [Values(LockType.Shared, LockType.Exclusive)] LockType wantedLock)
        {
            const long FreeKey = 2;
            var handler = new RecordingHandler(abandon) { Epoch = store.epoch };
            using var session = NewSession();
            session.LockContentionHandler = handler;
            var context = session.TransactionalContext;

            var keyBytes = GC.AllocateArray<long>(2, pinned: true);
            keyBytes[0] = FreeKey;
            keyBytes[1] = LockedKey;
            context.BeginTransaction();
            FixedLengthTransactionalKeyStruct[] keys =
            [
                new(MemoryMarshal.AsBytes(keyBytes.AsSpan(0, 1)), wantedLock, context),
                new(MemoryMarshal.AsBytes(keyBytes.AsSpan(1, 1)), wantedLock, context),
            ];
            context.SortKeyHashes<FixedLengthTransactionalKeyStruct>(keys);

            var holder = new HeldLock(this, LockType.Exclusive);
            Exception thrown = null;
            var epochHeldAfterThrow = true;
            var worker = new Thread(() =>
            {
                try
                {
                    context.Lock<FixedLengthTransactionalKeyStruct>(keys);
                }
                catch (Exception ex)
                {
                    thrown = ex;
                }
                epochHeldAfterThrow = store.epoch.ThisInstanceProtected();
            });
            bool invoked;
            bool freeKeyLockable;
            try
            {
                worker.Start();
                invoked = handler.Invoked.Wait(Patience);

                // Whichever of the keys the lock reached first, the wait holds neither
                using var other = NewSession();
                var otherContext = other.TransactionalContext;
                otherContext.BeginTransaction();
                FixedLengthTransactionalKeyStruct[] freeKey = [new(MemoryMarshal.AsBytes(keyBytes.AsSpan(0, 1)), LockType.Exclusive, otherContext)];
                freeKeyLockable = otherContext.TryLock<FixedLengthTransactionalKeyStruct>(freeKey, TimeSpan.FromMilliseconds(100));
                if (freeKeyLockable)
                    otherContext.Unlock<FixedLengthTransactionalKeyStruct>(freeKey);
                otherContext.EndTransaction();
            }
            finally
            {
                holder.Dispose();
                ClassicAssert.IsTrue(worker.Join(Patience), "The lock was not taken once the held key was released");
            }

            ClassicAssert.IsTrue(invoked, "The lock waited for the held key without consulting the handler");
            ClassicAssert.IsTrue(freeKeyLockable, "The lock held one of its keys while it waited for the other");
            ClassicAssert.AreEqual(HashOf(LockedKey), handler.KeyHash);
            ClassicAssert.AreEqual(wantedLock == LockType.Exclusive, handler.Exclusive);
            ClassicAssert.IsTrue(handler.WasContended);
            ClassicAssert.IsFalse(handler.EpochHeldInHandler, "The handler ran with the epoch held");

            if (abandon)
            {
                ClassicAssert.IsInstanceOf<AbandonedException>(thrown);
                ClassicAssert.IsFalse(epochHeldAfterThrow, "The epoch is still held after the lock was abandoned");
                ClassicAssert.AreEqual(0UL, session.TotalLockCount, "An abandoned lock left keys locked");

                // Uncontended, the same session takes the locks without consulting the handler
                context.Lock<FixedLengthTransactionalKeyStruct>(keys);
                ClassicAssert.AreEqual(1, handler.Calls);
            }
            else
                ClassicAssert.IsNull(thrown);

            ClassicAssert.AreEqual(2UL, session.TotalLockCount);
            context.Unlock<FixedLengthTransactionalKeyStruct>(keys);
            context.EndTransaction();
        }

        [Test]
        [Category(LockTestCategory), Category(SmokeTestCategory)]
        public void AnUpdateRetriedForAReasonOtherThanALatchIsNotHandedToTheHandler([Values] bool transactional)
        {
            using var populate = NewSession();
            var address = store.Log.TailAddress;
            _ = Upsert(populate.BasicContext, LockedKey, OriginalValue);

            var handler = new RecordingHandler(abandon: true) { Epoch = store.epoch };
            using var session = NewSession();
            session.LockContentionHandler = handler;

            // A session that holds the key locked itself retries on its own latch
            var keyBytes = GC.AllocateArray<long>(1, pinned: true);
            keyBytes[0] = LockedKey;
            var context = session.TransactionalContext;
            FixedLengthTransactionalKeyStruct[] keys = [new(MemoryMarshal.AsBytes(keyBytes.AsSpan()), LockType.Exclusive, context)];
            if (transactional)
            {
                context.BeginTransaction();
                context.Lock<FixedLengthTransactionalKeyStruct>(keys);
            }

            var heldEpoch = HoldRecordWhereAnUpdateRetries(address);
            Exception thrown = null;
            var worker = new Thread(() =>
            {
                try
                {
                    var key = LockedKey;
                    var value = OriginalValue + 1;
                    var keySpan = TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key));
                    _ = transactional ? context.RMW(keySpan, ref value) : session.BasicContext.RMW(keySpan, ref value);
                }
                catch (Exception ex)
                {
                    thrown = ex;
                }
            });
            bool retriedInPlace;
            try
            {
                worker.Start();
                retriedInPlace = !worker.Join(TimeSpan.FromMilliseconds(200));
            }
            finally
            {
                heldEpoch.Dispose();
                ClassicAssert.IsTrue(worker.Join(Patience), "The update did not complete once the epoch was released");
            }

            ClassicAssert.IsTrue(retriedInPlace, "The update completed while the epoch was held, so it never retried");
            ClassicAssert.IsNull(thrown, "The update was handed to the handler");
            ClassicAssert.AreEqual(0, handler.Calls);

            if (transactional)
            {
                context.Unlock<FixedLengthTransactionalKeyStruct>(keys);
                context.EndTransaction();
            }

            ClassicAssert.IsTrue(Read(populate.BasicContext, LockedKey, out var updated).Found);
            ClassicAssert.AreEqual(OriginalValue + 1, updated);
        }

        [Test]
        [Category(LockTestCategory), Category(SmokeTestCategory)]
        public void ATransactionalLockTakenWhileHoldingOtherLocksIsNotHandedToTheHandler()
        {
            const long FreeKey = 2;
            var handler = new RecordingHandler(abandon: true) { Epoch = store.epoch };
            using var session = NewSession();
            session.LockContentionHandler = handler;
            var context = session.TransactionalContext;

            var keyBytes = GC.AllocateArray<long>(2, pinned: true);
            keyBytes[0] = FreeKey;
            keyBytes[1] = LockedKey;
            FixedLengthTransactionalKeyStruct[] freeKey = [new(MemoryMarshal.AsBytes(keyBytes.AsSpan(0, 1)), LockType.Exclusive, context)];
            FixedLengthTransactionalKeyStruct[] lockedKey = [new(MemoryMarshal.AsBytes(keyBytes.AsSpan(1, 1)), LockType.Exclusive, context)];
            context.BeginTransaction();
            context.Lock<FixedLengthTransactionalKeyStruct>(freeKey);

            var holder = new HeldLock(this, LockType.Exclusive);
            Exception thrown = null;
            var worker = new Thread(() =>
            {
                try
                {
                    context.Lock<FixedLengthTransactionalKeyStruct>(lockedKey);
                }
                catch (Exception ex)
                {
                    thrown = ex;
                }
            });
            bool waitedInPlace;
            try
            {
                worker.Start();
                waitedInPlace = !worker.Join(TimeSpan.FromMilliseconds(200));
            }
            finally
            {
                holder.Dispose();
                ClassicAssert.IsTrue(worker.Join(Patience), "The lock was not taken once the held key was released");
            }

            ClassicAssert.IsTrue(waitedInPlace, "The lock was taken while the key was held");
            ClassicAssert.IsNull(thrown, "The lock was handed to the handler while the session held another");
            ClassicAssert.AreEqual(0, handler.Calls);
            ClassicAssert.AreEqual(2UL, session.TotalLockCount);

            context.Unlock<FixedLengthTransactionalKeyStruct>(lockedKey);
            context.Unlock<FixedLengthTransactionalKeyStruct>(freeKey);
            context.EndTransaction();
        }
    }
}