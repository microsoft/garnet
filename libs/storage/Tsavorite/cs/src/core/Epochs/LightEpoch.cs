// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
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
        /// Buffer to track information for LightEpoch instances. This is used:
        /// (1) in AssignInstance, to assign a unique instanceId to each LightEpoch instance, and
        /// (2) in Metadata, to track per-thread epoch table entries for each LightEpoch instance.
        /// </summary>
        [InlineArray(MaxInstances)]
        private struct InstanceIndexBuffer
        {
            /// <summary>
            /// Maximum number of concurrent instances of LightEpoch supported.
            /// </summary>
            internal const int MaxInstances = 1024;

            /// <summary>
            /// Anchor field for the buffer.
            /// </summary>
            int field0;

            /// <summary>
            /// Reference to the entry for the given instance ID.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [UnscopedRef]
            internal ref int GetRef(int instanceId)
            {
                Debug.Assert(instanceId >= 0 && instanceId < MaxInstances);
                return ref Unsafe.Add(ref field0, instanceId);
            }
        }

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
            /// This is the thread-static index for fast access to the tableAligned index 
            /// that is obtained when each LightEpoch instance calls ReserveEntry.
            /// The instanceId of the LightEpoch instance (assigned to the instance 
            /// at constructor time using InstanceTracker) is the lookup offset into 
            /// Entries.
            /// 
            /// Note that Entries effectively gives us ThreadLocal{T} semantics of 
            /// (instance, thread)-specific metadata, without the overhead of 
            /// ThreadLocal{T}.
            /// </summary>
            [ThreadStatic]
            internal static InstanceIndexBuffer Entries;
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
        /// Number of threads waiting for an epoch table entry (lower 31 bits).
        /// MSB is set during Dispose to prevent new waiters from entering.
        /// </summary>
        volatile int waiterCount = 0;

        /// <summary>
        /// Flag (MSB) used to mark the epoch as disposed in <see cref="waiterCount"/>.
        /// </summary>
        const int kDisposedFlag = unchecked((int)0x80000000);

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
        /// Maximum number of general-purpose per-thread <see cref="long"/> user-word slots that subsystems
        /// can claim via <see cref="AllocateUserWord(long)"/>. Bounded by the free space in <see cref="Entry"/>'s
        /// cache line (48 bytes = 6 longs).
        /// </summary>
        public const int MaxUserWords = 6;

        /// <summary>
        /// Bitmask of claimed user-word slots. Each set bit means that word index is in use by some
        /// subsystem. Managed exclusively via CAS in <see cref="AllocateUserWord"/> and
        /// <see cref="ReleaseUserWord"/>. Not read on the epoch Acquire/Release hot path.
        /// </summary>
        int userWordMask;

        /// <summary>
        /// This is the LightEpoch-level static buffer (array) of available instance slots.
        /// On LightEpoch instance creation, it is used by SelectInstance() to find an
        /// available slot in this array; this becomes the LightEpoch instance's instanceId,
        /// which is the lookup index into the thread-static Metadata.Entries.
        /// </summary>
        static InstanceIndexBuffer InstanceTracker;

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
            throw new InvalidOperationException($"Exceeded maximum number of active LightEpoch instances {ActiveInstanceCount()} {InstanceIndexBuffer.MaxInstances}");
        }

        /// <summary>
        /// Number of active LightEpoch instances. Used for testing and diagnostics.
        /// </summary>
        /// <returns></returns>
        public static int ActiveInstanceCount()
        {
            int count = 0;
            for (var i = 0; i < InstanceIndexBuffer.MaxInstances; i++)
            {
                if (kInvalidIndex != InstanceTracker.GetRef(i))
                    count++;
            }
            return count;
        }

        /// <summary>
        /// Reset all instances. Used for testing to reset static LightEpoch state for all instances.
        /// </summary>
        public static void ResetAllInstances()
        {
            for (var i = 0; i < InstanceIndexBuffer.MaxInstances; i++)
            {
                InstanceTracker.GetRef(i) = kInvalidIndex;
            }
        }

        /// <summary>
        /// Clean up epoch table
        /// </summary>
        public void Dispose()
        {
            // Cancel any threads currently waiting on the semaphore so they
            // unwind and decrement waiterCount.
            cts.Cancel();

            // Atomically set the disposed flag after all waiters are done.
            while (true)
            {
                if (Interlocked.CompareExchange(ref waiterCount, kDisposedFlag, 0) == 0)
                    break;
                Thread.Yield();
            }

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ThisInstanceProtected()
        {
            ref var entry = ref Metadata.Entries.GetRef(instanceId);
            return kInvalidIndex != entry && (*(tableAligned + entry)).threadId == Metadata.threadId;
        }

        /// <summary>
        /// Try to suspend the epoch, if it is currently held
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

            // Max epoch across all threads may have advanced, so check for pending drain actions to process
            if (drainCount > 0)
                Drain((*(tableAligned + entry)).localCurrentEpoch);

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
            if (drainCount > 0)
                SuspendDrain();
        }

        /// <summary>
        /// Thread resumes its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Resume()
        {
            Acquire();
        }

        /// <summary>
        /// Thread resumes its epoch entry if it has not already been acquired
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ResumeIfNotProtected()
        {
            if (ThisInstanceProtected())
                return false;
            Resume();
            return true;
        }

        /// <summary>
        /// Increment global current epoch
        /// </summary>
        /// <returns></returns>
        internal long BumpCurrentEpoch()
        {
            Debug.Assert(ThisInstanceProtected(), "BumpCurrentEpoch must be called on a protected thread");
            var nextEpoch = Interlocked.Increment(ref CurrentEpoch);

            if (drainCount > 0)
                Drain(nextEpoch);
            else
                _ = ComputeNewSafeToReclaimEpoch(nextEpoch);

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
            var PriorEpoch = BumpCurrentEpoch() - 1;

            var i = 0;
            while (true)
            {
                if (drainList[i].epoch == long.MaxValue)
                {
                    // This was an empty slot. If it still is, assign this action/epoch to the slot.
                    if (Interlocked.CompareExchange(ref drainList[i].epoch, long.MaxValue - 1, long.MaxValue) == long.MaxValue)
                    {
                        drainList[i].action = onDrain;
                        drainList[i].epoch = PriorEpoch;
                        _ = Interlocked.Increment(ref drainCount);
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
                    _ = Thread.Yield();
                }
            }

            // Now ProtectAndDrain, which may execute the action we just added.
            ProtectAndDrain();
        }

        /// <summary>
        /// Looks at all threads and return the latest safe epoch
        /// </summary>
        /// <param name="currentEpoch">Current epoch</param>
        /// <returns>Safe epoch</returns>
        long ComputeNewSafeToReclaimEpoch(long currentEpoch)
        {
            var oldestOngoingCall = currentEpoch;

            for (var index = 1; index <= kTableSize; index++)
            {
                var entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                if (0 != entry_epoch)
                {
                    if (entry_epoch < oldestOngoingCall)
                        oldestOngoingCall = entry_epoch;
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
                for (var index = 1; index <= kTableSize; index++)
                {
                    var entry_epoch = (*(tableAligned + index)).localCurrentEpoch;
                    if (0 != entry_epoch)
                        return;
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
            _ = ComputeNewSafeToReclaimEpoch(nextEpoch);

            for (var i = 0; i < kDrainListSize; i++)
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
                        _ = Interlocked.Decrement(ref drainCount);

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

            // Reserve an entry in the epoch table for this thread
            ReserveEntryForThread(ref entry);

            Debug.Assert((*(tableAligned + entry)).localCurrentEpoch == 0,
                "Trying to acquire protected epoch. Make sure you do not re-enter Tsavorite from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");
            Debug.Assert((*(tableAligned + entry)).threadId > 0, "Epoch table entry missing threadId");

            // Protect CurrentEpoch by copying it to the instance-specific epoch table
            // so that ComputeNewSafeToReclaimEpoch() will see it.
            (*(tableAligned + entry)).localCurrentEpoch = CurrentEpoch;

            // Max epoch across all threads may have advanced, so check for pending drain actions to process
            if (drainCount > 0)
            {
                Drain((*(tableAligned + entry)).localCurrentEpoch);
            }
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
        /// then circling twice around the epoch table. On a successful acquire, 
        /// startOffset1 contains the acquired offset so that the next acquire 
        /// can optimistically get the same slot. This method relies on the fact 
        /// that no thread will ever have ID 0.
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
            var tmp = Metadata.startOffset1;
            Metadata.startOffset1 = Metadata.startOffset2;
            Metadata.startOffset2 = tmp;

            entry = Metadata.startOffset1;
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
                Metadata.startOffset1++;
                if (Metadata.startOffset1 > kTableSize)
                    Metadata.startOffset1 -= kTableSize;

                entry = Metadata.startOffset1;
                if (0 == (tableAligned + entry)->threadId)
                {
                    if (0 == Interlocked.CompareExchange(
                        ref (tableAligned + entry)->threadId,
                        Metadata.threadId, 0))
                        return true;
                }
            }

            // Note: Metadata.startOffset1 should now be back to where it started because
            // we circled the entire table twice.
            entry = kInvalidIndex;
            return false;
        }

        /// <summary>
        /// Reserve entry for thread. First try synchronous acquire, then fall back to a SemaphoreSlim wait.
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
            int newCount = Interlocked.Increment(ref waiterCount);
            try
            {
                // If the MSB (disposed flag) is set, the epoch is being disposed.
                if ((newCount & kDisposedFlag) != 0)
                    throw new ObjectDisposedException(nameof(LightEpoch));

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
            catch (OperationCanceledException)
            {
                throw new ObjectDisposedException(nameof(LightEpoch));
            }
            finally
            {
                _ = Interlocked.Decrement(ref waiterCount);
            }
        }

        /// <summary>
        /// Allocate a new entry in epoch table
        /// </summary>
        /// <returns>Reserved entry</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ReserveEntryForThread(ref int entry)
        {
            if (Metadata.threadId == 0) // run once per thread for performance
            {
                Metadata.threadId = Environment.CurrentManagedThreadId;
                var code = (uint)Utility.Murmur3(Metadata.threadId);
                Metadata.startOffset1 = (ushort)(1 + (code % kTableSize));
                Metadata.startOffset2 = (ushort)(1 + ((code >> 16) % kTableSize));
            }
            ReserveEntry(ref entry);
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            var sb = new System.Text.StringBuilder();
            sb.AppendLine($"CurrentEpoch: {CurrentEpoch}, SafeToReclaimEpoch: {SafeToReclaimEpoch}");

            var wc = waiterCount;
            bool disposed = (wc & kDisposedFlag) != 0;
            sb.AppendLine($"Waiters: {wc & ~kDisposedFlag}, Disposed: {disposed}");

            // Active epoch table entries
            sb.Append("Threads: [");
            bool first = true;
            for (int i = 1; i <= kTableSize; i++)
            {
                var e = *(tableAligned + i);
                if (e.threadId != 0)
                {
                    if (!first) sb.Append(", ");
                    sb.Append($"tid={e.threadId} epoch={e.localCurrentEpoch}");
                    first = false;
                }
            }
            sb.AppendLine(first ? "none]" : "]");

            // Drain list entries
            sb.Append("DrainList: [");
            first = true;
            for (int i = 0; i < kDrainListSize; i++)
            {
                var d = drainList[i];
                if (d.epoch != long.MaxValue)
                {
                    if (!first) sb.Append(", ");
                    sb.Append($"epoch={d.epoch} action={(d.action is null ? "null" : d.action.Method.Name)}");
                    first = false;
                }
            }
            sb.Append(first ? "none]" : "]");

            return sb.ToString();
        }

        #region User-word API

        /// <summary>
        /// Number of entries in the epoch table.
        /// </summary>
        public int EntryCount => kTableSize;

        /// <summary>
        /// Claim a per-thread user-word slot. Returns the word index to pass to
        /// <see cref="ThisThreadUserWord(int)"/> and <see cref="GetMinUserWord(int)"/>.
        /// The column across all entries is initialized to <paramref name="initialValue"/>.
        /// After allocation, the application owns the slot contents — LightEpoch does not
        /// automatically reset slots on epoch Acquire/Release. Throws if all
        /// <see cref="MaxUserWords"/> slots are already claimed.
        /// </summary>
        /// <param name="initialValue">Value written to every entry's slot at allocation time.</param>
        /// <returns>Word index in the range <c>[0, <see cref="MaxUserWords"/>)</c>.</returns>
        public int AllocateUserWord(long initialValue)
        {
            while (true)
            {
                var mask = Volatile.Read(ref userWordMask);
                int idx = BitOperations.TrailingZeroCount(~mask);
                if (idx >= MaxUserWords)
                    throw new InvalidOperationException($"All {MaxUserWords} LightEpoch user-word slots are claimed.");

                // CAS to claim the slot. Only the winner proceeds to initialize.
                var newMask = mask | (1 << idx);
                if (Interlocked.CompareExchange(ref userWordMask, newMask, mask) != mask)
                    continue; // another thread modified the mask; retry

                // We exclusively own this slot — initialize the column across all entries.
                for (int i = 1; i <= kTableSize; i++)
                    Volatile.Write(ref UserWordRef(i, idx), initialValue);

                return idx;
            }
        }

        /// <summary>
        /// Release a previously claimed user-word slot. Caller is responsible for ensuring that no
        /// producer thread still holds or can still issue writes to the slot (e.g., by calling this
        /// only after subsystem quiescence / Dispose).
        /// </summary>
        public void ReleaseUserWord(int wordIndex)
        {
            if ((uint)wordIndex >= MaxUserWords)
                throw new ArgumentOutOfRangeException(nameof(wordIndex));
            while (true)
            {
                var mask = Volatile.Read(ref userWordMask);
                var newMask = mask & ~(1 << wordIndex);
                if (Interlocked.CompareExchange(ref userWordMask, newMask, mask) == mask)
                    return;
            }
        }

        /// <summary>
        /// Get a ref to the current thread's user-word slot. Caller MUST be inside epoch protection
        /// (<see cref="Resume"/> / before <see cref="Suspend"/>). Returns the same cache line that is
        /// already hot due to epoch Resume, so writes are essentially free.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref long ThisThreadUserWord(int wordIndex)
        {
            Debug.Assert((uint)wordIndex < MaxUserWords, "Invalid user-word index");
            Debug.Assert(ThisInstanceProtected(), "ThisThreadUserWord must be called while epoch is protected");
            int entryIndex = Metadata.Entries.GetRef(instanceId);
            return ref UserWordRef(entryIndex, wordIndex);
        }

        /// <summary>
        /// Compute the minimum value of the user-word at <paramref name="wordIndex"/> across all epoch
        /// table entries, using a direct unsafe pointer walk.
        /// </summary>
        /// <param name="wordIndex">User-word slot index (0-based).</param>
        /// <returns>The minimum value observed across all entries.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetMinUserWord(int wordIndex)
        {
            Debug.Assert((uint)wordIndex < MaxUserWords, "Invalid user-word index");

            // Derive the base address from the actual Entry field layout via UserWordRef,
            // rather than hardcoding byte offsets. Entries occupy indices 1..kTableSize
            // (index 0 is kInvalidIndex and unused). Stride between entries is kCacheLineBytes.
            long min = long.MaxValue;
            byte* basePtr = (byte*)Unsafe.AsPointer(ref UserWordRef(1, wordIndex));
            int stride = kCacheLineBytes;
            int count = kTableSize;

            for (int i = 0; i < count; i++)
            {
                long v = Volatile.Read(ref Unsafe.AsRef<long>(basePtr + (long)i * stride));
                if (v < min) min = v;
            }
            return min;
        }

        /// <summary>
        /// Get a ref to the user word at <paramref name="wordIndex"/> for entry <paramref name="entryIndex"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ref long UserWordRef(int entryIndex, int wordIndex)
            => ref Unsafe.Add(ref (*(tableAligned + entryIndex)).userWord0, wordIndex);

        #endregion

        /// <summary>
        /// Epoch table entry (cache line size).
        /// Existing epoch fields occupy the first 16 bytes (localCurrentEpoch + threadId + 4 bytes padding).
        /// The remaining 48 bytes host <see cref="MaxUserWords"/> general-purpose per-thread <see cref="long"/> slots that
        /// subsystems can claim via <see cref="AllocateUserWord(long)"/>. This reuses the cache line that is already
        /// hot from epoch Resume/Suspend, so user-word access is essentially free compared to touching a separate
        /// data structure.
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

            /// <summary>
            /// First user-word slot. Remaining <see cref="MaxUserWords"/> - 1 slots are contiguous after this
            /// field at 8-byte stride. Access via <c>Unsafe.Add(ref userWord0, wordIndex)</c>.
            /// </summary>
            [FieldOffset(16)]
            public long userWord0;

            [FieldOffset(24)]
            public long userWord1;

            [FieldOffset(32)]
            public long userWord2;

            [FieldOffset(40)]
            public long userWord3;

            [FieldOffset(48)]
            public long userWord4;

            [FieldOffset(56)]
            public long userWord5;

            public override string ToString() => $"lce = {localCurrentEpoch}, tid = {threadId}";
        }

        /// <summary>
        /// Pair of epoch and action to be executed
        /// </summary>
        struct EpochActionPair
        {
            public long epoch;
            public Action action;

            public override readonly string ToString() => $"epoch = {epoch}, action = {(action is null ? "n/a" : action.Method.ToString())}";
        }
    }
}