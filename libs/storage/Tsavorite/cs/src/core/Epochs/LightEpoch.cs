// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Epoch protection
    /// </summary>
    public sealed unsafe class LightEpoch : IEpochAccessor
    {
        /// <summary>
        /// Store for thread-static metadata.
        /// </summary>
        private class Metadata
        {
            /// <summary>
            /// Managed thread id of this thread
            /// </summary>
            [ThreadStatic]
            internal static int threadId;

            /// <summary>
            /// Start offset to reserve entry in the epoch table
            /// </summary>
            [ThreadStatic]
            internal static ushort startOffset1;

            /// <summary>
            /// Alternate start offset to reserve entry in the epoch table (to reduce probing if <see cref="startOffset1"/> slot is already filled)
            /// </summary>
            [ThreadStatic]
            internal static ushort startOffset2;

            /// <summary>
            /// Per-instance entry in the epoch table, for this thread
            /// </summary>
            [ThreadStatic]
            internal static InstanceIndexBuffer Entries;
        }

        /// <summary>
        /// Buffer to track information for LightEpoch instances. This is used:
        /// (1) in AssignInstance, to assign a unique instance ID to each LightEpoch instance, and
        /// (2) in Metadata, to track per-thread epoch table entries for each LightEpoch instance.
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = MaxInstances * sizeof(int))]
        private struct InstanceIndexBuffer
        {
            /// <summary>
            /// Maximum number of concurrent instances of LightEpoch supported.
            /// </summary>
            internal const int MaxInstances = 16;

            /// <summary>
            /// Anchor field for the buffer.
            /// </summary>
            [FieldOffset(0)]
            int field0;

            /// <summary>
            /// Reference to the entry for the given instance ID.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal ref int GetRef(int instanceId)
            {
                Debug.Assert(instanceId >= 0 && instanceId < MaxInstances);
                return ref Unsafe.AsRef<int>((int*)Unsafe.AsPointer(ref field0) + instanceId);
            }
        }

        /// <summary>
        /// Size of cache line in bytes
        /// </summary>
        const int kCacheLineBytes = 64;

        /// <summary>
        /// Default invalid index entry.
        /// </summary>
        const int kInvalidIndex = 0;

        /// <summary>
        /// Default number of entries in the entries table
        /// </summary>
        static readonly ushort kTableSize = Math.Max((ushort)128, (ushort)(Environment.ProcessorCount * 2));

        /// <summary>
        /// Default drainlist size
        /// </summary>
        const int kDrainListSize = 16;

        /// <summary>
        /// Thread protection status entries.
        /// </summary>
        readonly Entry[] tableRaw;
        readonly Entry* tableAligned;

        /// <summary>
        /// Semaphore for threads waiting for an epoch table entry
        /// </summary>
        readonly SemaphoreSlim waiterSemaphore = new(0);

        /// <summary>
        /// Cancellation token source used to cancel threads waiting on the semaphore during Dispose.
        /// </summary>
        readonly CancellationTokenSource cts = new();

        /// <summary>
        /// Number of threads waiting for an epoch table entry.
        /// Used as a fast-path check to avoid semaphore overhead when no waiters.
        /// </summary>
        volatile int waiterCount = 0;

        /// <summary>
        /// List of action, epoch pairs containing actions to be performed when an epoch becomes safe to reclaim.
        /// Marked volatile to ensure latest value is seen by the last suspended thread.
        /// </summary>
        volatile int drainCount = 0;
        readonly EpochActionPair[] drainList = new EpochActionPair[kDrainListSize];

        /// <summary>
        /// Global current epoch value
        /// </summary>
        internal long CurrentEpoch;

        /// <summary>
        /// Cached value of latest epoch that is safe to reclaim
        /// </summary>
        internal long SafeToReclaimEpoch;

        /// <summary>
        /// ID of this LightEpoch instance
        /// </summary>
        readonly int instanceId;

        /// <summary>
        /// Buffer to track assigned LightEpoch instance IDs
        /// </summary>
        static InstanceIndexBuffer InstanceTracker = default;

        /// <summary>
        /// Instantiate the epoch table
        /// </summary>
        public LightEpoch()
        {
            instanceId = SelectInstance();

            long p;

            tableRaw = GC.AllocateArray<Entry>(kTableSize + 2, true);
            p = (long)Unsafe.AsPointer(ref tableRaw[0]);

            // Force the pointer to align to 64-byte boundaries
            long p2 = (p + (kCacheLineBytes - 1)) & ~(kCacheLineBytes - 1);
            tableAligned = (Entry*)p2;

            CurrentEpoch = 1;
            SafeToReclaimEpoch = 0;

            // Mark all epoch table entries as "available"
            for (int i = 0; i < kDrainListSize; i++)
                drainList[i].epoch = long.MaxValue;
            drainCount = 0;
        }

        int SelectInstance()
        {
            for (var i = 0; i < InstanceIndexBuffer.MaxInstances; i++)
            {
                ref var entry = ref InstanceTracker.GetRef(i);
                // Try to claim this instance ID (indicated as 1 in the entry)
                if (kInvalidIndex == Interlocked.CompareExchange(ref entry, 1, kInvalidIndex))
                    return i;
            }
            throw new InvalidOperationException("Exceeded maximum number of active LightEpoch instances");
        }

        /// <summary>
        /// Clean up epoch table
        /// </summary>
        public void Dispose()
        {
            // Cancel any threads waiting on the semaphore, then wait for
            // them to finish unwinding before disposing resources.
            cts.Cancel();
            while (waiterCount > 0)
                Thread.Yield();

            CurrentEpoch = 1;
            SafeToReclaimEpoch = 0;
            // Mark this instance ID as available
            InstanceTracker.GetRef(instanceId) = kInvalidIndex;

            cts.Dispose();
            waiterSemaphore.Dispose();
        }

        /// <summary>
        /// Check whether current epoch instance is protected on this thread
        /// </summary>
        /// <returns>Result of the check</returns>
        public bool ThisInstanceProtected()
        {
            ref var entry = ref Metadata.Entries.GetRef(instanceId);
            if (kInvalidIndex != entry)
            {
                if ((*(tableAligned + entry)).threadId == entry)
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Release epoch if held
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySuspend()
        {
            if (ThisInstanceProtected())
            {
                Suspend();
                return true;
            }
            return false;
        }

        /// <summary>
        /// Enter the thread into the protected code region
        /// </summary>
        /// <returns>Current epoch</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ProtectAndDrain()
        {
            ref var entry = ref Metadata.Entries.GetRef(instanceId);

            Debug.Assert(entry > 0, "Trying to refresh unacquired epoch");
            Debug.Assert((*(tableAligned + entry)).threadId > 0, "Epoch table entry missing threadId");

            // Protect CurrentEpoch by copying it to the instance-specific epoch table
            // so that ComputeNewSafeToReclaimEpoch() will see it.
            (*(tableAligned + entry)).localCurrentEpoch = CurrentEpoch;

            if (drainCount > 0)
            {
                Drain((*(tableAligned + entry)).localCurrentEpoch);
            }

            if (waiterCount > 0)
            {
                SuspendResume();
            }
        }

        /// <summary>
        /// Thread suspends, then resumes, to give waiting threads a fair chance of making progress.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void SuspendResume()
        {
            Suspend();
            Resume();
        }

        /// <summary>
        /// Thread suspends its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Suspend()
        {
            Release();
            if (drainCount > 0) SuspendDrain();
        }

        /// <summary>
        /// Thread resumes its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Resume()
        {
            Acquire();
            ProtectAndDrain();
        }

        /// <summary>
        /// Increment global current epoch
        /// </summary>
        /// <returns></returns>
        internal long BumpCurrentEpoch()
        {
            Debug.Assert(ThisInstanceProtected(), "BumpCurrentEpoch must be called on a protected thread");
            long nextEpoch = Interlocked.Increment(ref CurrentEpoch);

            if (drainCount > 0)
                Drain(nextEpoch);
            else
                ComputeNewSafeToReclaimEpoch(nextEpoch);

            return nextEpoch;
        }

        /// <summary>
        /// Increment current epoch and associate trigger action
        /// with the prior epoch
        /// </summary>
        /// <param name="onDrain">Trigger action</param>
        /// <returns></returns>
        public void BumpCurrentEpoch(Action onDrain)
        {
            long PriorEpoch = BumpCurrentEpoch() - 1;

            int i = 0;
            while (true)
            {
                if (drainList[i].epoch == long.MaxValue)
                {
                    // This was an empty slot. If it still is, assign this action/epoch to the slot.
                    if (Interlocked.CompareExchange(ref drainList[i].epoch, long.MaxValue - 1, long.MaxValue) == long.MaxValue)
                    {
                        drainList[i].action = onDrain;
                        drainList[i].epoch = PriorEpoch;
                        Interlocked.Increment(ref drainCount);
                        break;
                    }
                }
                else
                {
                    var triggerEpoch = drainList[i].epoch;

                    if (triggerEpoch <= SafeToReclaimEpoch)
                    {
                        // This was a slot with an epoch that was safe to reclaim. If it still is, execute its trigger, then assign this action/epoch to the slot.
                        if (Interlocked.CompareExchange(ref drainList[i].epoch, long.MaxValue - 1, triggerEpoch) == triggerEpoch)
                        {
                            var triggerAction = drainList[i].action;
                            drainList[i].action = onDrain;
                            drainList[i].epoch = PriorEpoch;
                            triggerAction();
                            break;
                        }
                    }
                }

                if (++i == kDrainListSize)
                {
                    // We are at the end of the drain list and found no empty or reclaimable slot. ProtectAndDrain, which should clear one or more slots.
                    ProtectAndDrain();
                    i = 0;
                    Thread.Yield();
                }
            }

            // Now ProtectAndDrain, which may execute the action we just added.
            ProtectAndDrain();
        }

        /// <summary>
        /// Looks at all threads and return the latest safe epoch
        /// </summary>
        /// <returns>Safe epoch</returns>
        internal long ComputeNewSafeToReclaimEpoch() => ComputeNewSafeToReclaimEpoch(CurrentEpoch);

        /// <summary>
        /// Looks at all threads and return the latest safe epoch
        /// </summary>
        /// <param name="currentEpoch">Current epoch</param>
        /// <returns>Safe epoch</returns>
        long ComputeNewSafeToReclaimEpoch(long currentEpoch)
        {
            long oldestOngoingCall = currentEpoch;

            for (int index = 1; index <= kTableSize; ++index)
            {
                long entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                if (0 != entry_epoch)
                {
                    if (entry_epoch < oldestOngoingCall)
                    {
                        oldestOngoingCall = entry_epoch;
                    }
                }
            }

            // The latest safe epoch is the one just before the earliest unsafe epoch.
            SafeToReclaimEpoch = oldestOngoingCall - 1;
            return SafeToReclaimEpoch;
        }

        /// <summary>
        /// Take care of pending drains after epoch suspend
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SuspendDrain()
        {
            while (drainCount > 0)
            {
                // Barrier ensures we see the latest epoch table entries. Ensures
                // that the last suspended thread drains all pending actions.
                Thread.MemoryBarrier();
                for (int index = 1; index <= kTableSize; ++index)
                {
                    long entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                    if (0 != entry_epoch)
                    {
                        return;
                    }
                }
                Resume();
                Release();
            }
        }

        /// <summary>
        /// Check and invoke trigger actions that are ready
        /// </summary>
        /// <param name="nextEpoch">Next epoch</param>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void Drain(long nextEpoch)
        {
            ComputeNewSafeToReclaimEpoch(nextEpoch);

            for (int i = 0; i < kDrainListSize; i++)
            {
                var trigger_epoch = drainList[i].epoch;

                if (trigger_epoch <= SafeToReclaimEpoch)
                {
                    if (Interlocked.CompareExchange(ref drainList[i].epoch, long.MaxValue - 1, trigger_epoch) == trigger_epoch)
                    {
                        // Store off the trigger action, then set epoch to int.MaxValue to mark this slot as "available for use".
                        var trigger_action = drainList[i].action;
                        drainList[i].action = null;
                        drainList[i].epoch = long.MaxValue;
                        Interlocked.Decrement(ref drainCount);

                        // Execute the action
                        trigger_action();
                        if (drainCount == 0)
                            break;
                    }
                }
            }
        }

        /// <summary>
        /// Thread acquires its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Acquire()
        {
            ref var entry = ref Metadata.Entries.GetRef(instanceId);
            Debug.Assert(entry == kInvalidIndex,
                "Trying to acquire protected epoch. Make sure you do not re-enter Tsavorite from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");
            ReserveEntryForThread(ref entry);

            Debug.Assert((*(tableAligned + entry)).localCurrentEpoch == 0,
                "Trying to acquire protected epoch. Make sure you do not re-enter Tsavorite from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");
        }

        /// <summary>
        /// Thread releases its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Release()
        {
            ref var entry = ref Metadata.Entries.GetRef(instanceId);

            Debug.Assert((*(tableAligned + entry)).localCurrentEpoch != 0,
                "Trying to release unprotected epoch. Make sure you do not re-enter Tsavorite from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");

            // Clear "ThisInstanceProtected()" (non-static epoch table)
            (*(tableAligned + entry)).localCurrentEpoch = 0;
            (*(tableAligned + entry)).threadId = 0;

            entry = kInvalidIndex;
            if (waiterCount > 0)
                waiterSemaphore.Release();
        }

        /// <summary>
        /// Try to acquire an entry by probing startOffset1, startOffset2, 
        /// then circling twice around the epoch table.
        /// </summary>
        /// <returns>True if entry was acquired, false if table is full</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryAcquireEntry(ref int entry)
        {
            // Try primary offset
            entry = Metadata.startOffset1;
            if (0 == (tableAligned + entry)->threadId)
            {
                if (0 == Interlocked.CompareExchange(
                    ref (tableAligned + entry)->threadId,
                    Metadata.threadId, 0))
                    return true;
            }

            // Try alternate offset
            entry = Metadata.startOffset2;
            if (0 == (tableAligned + entry)->threadId)
            {
                if (0 == Interlocked.CompareExchange(
                    ref (tableAligned + entry)->threadId,
                    Metadata.threadId, 0))
                    return true;
            }

            // Circle twice around the table looking for free entries
            for (var i = 0; i < 2 * kTableSize; i++)
            {
                Metadata.startOffset2++;
                if (Metadata.startOffset2 > kTableSize)
                    Metadata.startOffset2 -= kTableSize;

                entry = Metadata.startOffset2;
                if (0 == (tableAligned + entry)->threadId)
                {
                    if (0 == Interlocked.CompareExchange(
                        ref (tableAligned + entry)->threadId,
                        Metadata.threadId, 0))
                        return true;
                }
            }

            // Note: Metadata.startOffset2 should now be back to where it started because
            // we circled the entire table twice.
            entry = kInvalidIndex;
            return false;
        }

        /// <summary>
        /// Reserve entry for thread. This method relies on the fact that no
        /// thread will ever have ID 0. Fast path that probes the table without waiting.
        /// </summary>
        /// <returns>Reserved entry</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ReserveEntry(ref int entry)
        {
            if (TryAcquireEntry(ref entry))
                return;

            // Table is full, fall back to slow path with waiting
            ReserveEntryWait(ref entry);
        }

        /// <summary>
        /// Slow path for reserving an entry when the table is full.
        /// Waits on semaphore until an entry becomes available.
        /// </summary>
        /// <returns>Reserved entry</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void ReserveEntryWait(ref int entry)
        {
            _ = Interlocked.Increment(ref waiterCount);
            try
            {
                while (true)
                {
                    // Re-check for free slot after incrementing waiterCount. This avoids
                    // us waiting on the semaphore forever in case we increment waiterCount
                    // immediately after the epoch releaser sees a zero waiterCount (and
                    // therefore does not release the semaphore).
                    if (TryAcquireEntry(ref entry))
                        return;

                    // No slot available, wait for a signal from Release()
                    waiterSemaphore.Wait(cts.Token);
                }
            }
            finally
            {
                _ = Interlocked.Decrement(ref waiterCount);
            }
        }

        /// <summary>
        /// Allocate a new entry in epoch table. This is called 
        /// once for a thread.
        /// </summary>
        /// <returns>Reserved entry</returns>
        void ReserveEntryForThread(ref int entry)
        {
            if (Metadata.threadId == 0) // run once per thread for performance
            {
                Metadata.threadId = Environment.CurrentManagedThreadId;
                uint code = (uint)Utility.Murmur3(Metadata.threadId);
                Metadata.startOffset1 = (ushort)(1 + (code % kTableSize));
                Metadata.startOffset2 = (ushort)(1 + ((code >> 16) % kTableSize));
            }
            ReserveEntry(ref entry);
        }

        /// <inheritdoc/>
        public override string ToString() => $"Current {CurrentEpoch}, SafeToReclaim {SafeToReclaimEpoch}, drainCount {drainCount}";

        /// <summary>
        /// Epoch table entry (cache line size).
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = kCacheLineBytes)]
        struct Entry
        {
            /// <summary>
            /// Thread-local value of epoch
            /// </summary>
            [FieldOffset(0)]
            public long localCurrentEpoch;

            /// <summary>
            /// ID of thread associated with this entry.
            /// </summary>
            [FieldOffset(8)]
            public int threadId;

            [FieldOffset(12)]
            public int reentrant;

            [FieldOffset(16)]
            public fixed long padding[6]; // Padding to end of cache line

            public override string ToString() => $"lce = {localCurrentEpoch}, tid = {threadId}, re-ent {reentrant}";
        }

        /// <summary>
        /// Pair of epoch and action to be executed
        /// </summary>
        struct EpochActionPair
        {
            public long epoch;
            public Action action;

            public override string ToString() => $"epoch = {epoch}, action = {(action is null ? "n/a" : action.Method.ToString())}";
        }
    }
}