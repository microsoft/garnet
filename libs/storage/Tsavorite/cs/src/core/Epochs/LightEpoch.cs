// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    public interface IEpochAccessor
    {
        bool ReleaseIfHeld();
        void Resume();
    }

    /// <summary>
    /// Epoch protection
    /// </summary>
    public sealed unsafe class LightEpoch : IEpochAccessor
    {
        /// <summary>
        /// Store thread-static metadata separately from the LightEpoch class because LightEpoch has a static ctor,
        /// and this inhibits optimization of the .NET helper function that determines the base address of the static variables.
        /// This is expensive as it goes through multiple lookups, so lift these into a class that does not have a static ctor.
        /// TODO: This should be fixed in .NET 8; verify this and remove the Metadata class code when we no longer support pre-NET.8.
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
            /// A thread's entry in the epoch table.
            /// </summary>
            [ThreadStatic]
            internal static int threadEntryIndex;

            /// <summary>
            /// Number of instances using this entry
            /// </summary>
            [ThreadStatic]
            internal static int threadEntryIndexCount;
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

        static readonly Entry[] threadIndex;
        static readonly Entry* threadIndexAligned;

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
        /// Static constructor to setup shared cache-aligned space
        /// to store per-entry count of instances using that entry
        /// </summary>
        static LightEpoch()
        {
            long p;

            // Over-allocate to do cache-line alignment
            threadIndex = GC.AllocateArray<Entry>(kTableSize + 2, true);
            p = (long)Unsafe.AsPointer(ref threadIndex[0]);

            // Force the pointer to align to 64-byte boundaries
            long p2 = (p + (kCacheLineBytes - 1)) & ~(kCacheLineBytes - 1);
            threadIndexAligned = (Entry*)p2;
        }

        /// <summary>
        /// Instantiate the epoch table
        /// </summary>
        public LightEpoch()
        {
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

        /// <summary>
        /// Clean up epoch table
        /// </summary>
        public void Dispose()
        {
            CurrentEpoch = 1;
            SafeToReclaimEpoch = 0;
        }

        /// <summary>
        /// Check whether current epoch instance is protected on this thread
        /// </summary>
        /// <returns>Result of the check</returns>
        public bool ThisInstanceProtected()
        {
            int entry = Metadata.threadEntryIndex;
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
        public bool ReleaseIfHeld()
        {
            if (ThisInstanceProtected())
            {
                Release();
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
            int entry = Metadata.threadEntryIndex;

            // Protect CurrentEpoch by making an entry for it in the non-static epoch table so ComputeNewSafeToReclaimEpoch() will see it.
            (*(tableAligned + entry)).threadId = Metadata.threadEntryIndex;
            (*(tableAligned + entry)).localCurrentEpoch = CurrentEpoch;

            if (drainCount > 0)
            {
                Drain((*(tableAligned + entry)).localCurrentEpoch);
            }
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
            if (Metadata.threadEntryIndex == kInvalidIndex)
                Metadata.threadEntryIndex = ReserveEntryForThread();

            Debug.Assert((*(tableAligned + Metadata.threadEntryIndex)).localCurrentEpoch == 0,
                "Trying to acquire protected epoch. Make sure you do not re-enter Tsavorite from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");

            // This corresponds to AnyInstanceProtected(). We do not mark "ThisInstanceProtected" until ProtectAndDrain().
            Metadata.threadEntryIndexCount++;
        }

        /// <summary>
        /// Thread releases its epoch entry
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Release()
        {
            int entry = Metadata.threadEntryIndex;

            Debug.Assert((*(tableAligned + entry)).localCurrentEpoch != 0,
                "Trying to release unprotected epoch. Make sure you do not re-enter Tsavorite from callbacks or IDevice implementations. If using tasks, use TaskCreationOptions.RunContinuationsAsynchronously.");

            // Clear "ThisInstanceProtected()" (non-static epoch table)
            (*(tableAligned + entry)).localCurrentEpoch = 0;
            (*(tableAligned + entry)).threadId = 0;

            // Decrement "AnyInstanceProtected()" (static thread table)
            Metadata.threadEntryIndexCount--;
            if (Metadata.threadEntryIndexCount == 0)
            {
                (threadIndexAligned + Metadata.threadEntryIndex)->threadId = 0;
                Metadata.threadEntryIndex = kInvalidIndex;
            }
        }

        /// <summary>
        /// Reserve entry for thread. This method relies on the fact that no
        /// thread will ever have ID 0.
        /// </summary>
        /// <returns>Reserved entry</returns>
        static int ReserveEntry()
        {
            while (true)
            {
                // Try to acquire entry
                if (0 == (threadIndexAligned + Metadata.startOffset1)->threadId)
                {
                    if (0 == Interlocked.CompareExchange(
                        ref (threadIndexAligned + Metadata.startOffset1)->threadId,
                        Metadata.threadId, 0))
                        return Metadata.startOffset1;
                }

                if (Metadata.startOffset2 > 0)
                {
                    // Try alternate entry
                    Metadata.startOffset1 = Metadata.startOffset2;
                    Metadata.startOffset2 = 0;
                }
                else Metadata.startOffset1++; // Probe next sequential entry
                if (Metadata.startOffset1 > kTableSize)
                {
                    Metadata.startOffset1 -= kTableSize;
                    Thread.Yield();
                }
            }
        }

        /// <summary>
        /// Allocate a new entry in epoch table. This is called 
        /// once for a thread.
        /// </summary>
        /// <returns>Reserved entry</returns>
        static int ReserveEntryForThread()
        {
            if (Metadata.threadId == 0) // run once per thread for performance
            {
                Metadata.threadId = Environment.CurrentManagedThreadId;
                uint code = (uint)Utility.Murmur3(Metadata.threadId);
                Metadata.startOffset1 = (ushort)(1 + (code % kTableSize));
                Metadata.startOffset2 = (ushort)(1 + ((code >> 16) % kTableSize));
            }
            return ReserveEntry();
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