// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;
using System.Threading;
using Garnet.common;

namespace Garnet.server
{
    internal struct VirtualSublogReplayState
    {
        const int SketchSlotSize = 1 << 15;
        const int SketchSlotMask = SketchSlotSize - 1;

        /// <summary>
        /// Maximum number of spin iterations before falling back to the waiter queue.
        /// </summary>
        const int MaxSpinCount = 64;

        readonly long[] sketch = new long[SketchSlotSize];

        /// <summary>
        /// Explicit definition to minimize cache invalidation
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = 192)]
        sealed class SublogReplayMetadata
        {
            /// <summary>
            /// Lower bound of window used to trigger drift check for this sublog.
            /// </summary>
            [FieldOffset(64)] public long NextDriftCheckWindowLowerBoundSequenceNumber;

            /// <summary>
            /// Sublog max frontier value
            /// </summary>
            [FieldOffset(128)] public long Frontier;

            /// <summary>
            /// Smallest target sequence number among queued waiters (the head of the sorted waiter list).
            /// </summary>
            [FieldOffset(136)] public long MinSequenceNumberTarget;
        }
        readonly SublogReplayMetadata sublogReplayMetadata = new();

        /// <summary>
        /// Lock protecting the intrusive waiter list.
        /// </summary>
        readonly object @lock = new();

        /// <summary>
        /// Head of the intrusive sorted linked list of waiters (ascending by target sequence number).
        /// </summary>
        ReadSessionWaiter waiterHead;

        public readonly long Max => sublogReplayMetadata.Frontier;

        /// <summary>
        /// Reference to the max value for Volatile.Read access from external callers.
        /// </summary>
        public ref long MaxRef => ref sublogReplayMetadata.Frontier;

        /// <summary>
        /// Sequence number at or beyond which the owning replay thread runs its next replay-driven
        /// cross-sublog drift scan; long.MaxValue when the replay-driven check is disabled.
        /// Owner-private: accessed only by this sublog's replay thread.
        /// </summary>
        public long NextDriftCheckWindowLowerBoundSequenceNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => sublogReplayMetadata.NextDriftCheckWindowLowerBoundSequenceNumber;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => sublogReplayMetadata.NextDriftCheckWindowLowerBoundSequenceNumber = value;
        }

        /// <param name="nextDriftCheckSeed">
        /// Initial value for <see cref="NextDriftCheckWindowLowerBoundSequenceNumber"/>: the left edge of this sublog's
        /// first owned drift-check window, or long.MaxValue to disable the replay-driven scan.
        /// </param>
        public VirtualSublogReplayState(long nextDriftCheckSeed)
        {
            var size = SketchSlotSize;
            if ((size & (size - 1)) != 0)
                throw new InvalidOperationException($"Size ({SketchSlotSize}) must be a power of 2");
            Array.Clear(sketch);
            sublogReplayMetadata.Frontier = 0;
            sublogReplayMetadata.MinSequenceNumberTarget = long.MaxValue;
            sublogReplayMetadata.NextDriftCheckWindowLowerBoundSequenceNumber = nextDriftCheckSeed;
            waiterHead = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static long GetSketchSlot(long hash) => (hash >> 32) & SketchSlotMask;

        /// <summary>
        /// Gets the current frontier sequence number associated with the specified hash value.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetFrontierSequenceNumber(long hash)
            => Math.Max(Volatile.Read(ref Unsafe.AsRef(in sketch[GetSketchSlot(hash)])),
                        Volatile.Read(ref Unsafe.AsRef(in sublogReplayMetadata.Frontier)));

        /// <summary>
        /// Gets the sequence number associated with the specified hash key.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetKeySequenceNumber(long hash)
            => Volatile.Read(ref Unsafe.AsRef(in sketch[GetSketchSlot(hash)]));

        /// <summary>
        /// Issues a temporal prefetch of the sketch slot for the given hash so the post-read update
        /// finds it resident. The replay thread writes this slot, so an uncached read of it is a
        /// cross-core coherence miss on the post-read critical path; prefetching here overlaps that
        /// miss with the store read.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly unsafe void PrefetchKeySequenceNumber(long hash)
        {
            if (Sse.IsSupported)
                Sse.Prefetch0(Unsafe.AsPointer(ref sketch[GetSketchSlot(hash)]));
        }

        /// <summary>
        /// Updates the maximum observed sequence number.
        /// </summary>
        /// <remarks>Updates are thread-safe and guaranteed to be monotonically increasing.</remarks>
        public void UpdateMaxSequenceNumber(long sequenceNumber)
        {
            _ = Tsavorite.core.Utility.MonotonicUpdate(ref sublogReplayMetadata.Frontier, sequenceNumber, out _);
            SignalWaiters();
        }

        /// <summary>
        /// Updates the sequence number associated with the specified key hash.
        /// </summary>
        /// <remarks>Updates are thread-safe and guaranteed to be monotonically increasing.</remarks>
        public void UpdateKeySequenceNumber(long hash, long sequenceNumber)
        {
            _ = Tsavorite.core.Utility.MonotonicUpdate(ref sketch[GetSketchSlot(hash)], sequenceNumber, out _);
            SignalWaiters();
        }

        /// <summary>
        /// Signals waiters whose target sequence numbers have been reached.
        /// Walks from the head (lowest target) and signals all satisfied waiters via O(1) unlink.
        /// </summary>
        private void SignalWaiters()
        {
            // Make the max publish visible before the read of minT, so a decision to skip can never race ahead of the waiter's ability to see the new  max
            Interlocked.MemoryBarrier();
            if (Volatile.Read(ref sublogReplayMetadata.Frontier) <= Volatile.Read(ref sublogReplayMetadata.MinSequenceNumberTarget))
                return;

            lock (@lock)
            {
                var currentMax = Volatile.Read(ref sublogReplayMetadata.Frontier);
                while (waiterHead != null && waiterHead.TargetSequenceNumber < currentMax)
                {
                    var node = waiterHead;
                    waiterHead = node.Next;
                    _ = (waiterHead?.Prev = null);
                    node.Next = null;
                    node.Signal.Set();
                }
                UpdateMinWaiterTarget();
            }
        }

        /// <summary>
        /// Waits until the sublog's maximum sequence number exceeds the given session maximum.
        /// </summary>
        /// <param name="maximumSessionSequenceNumber">Sequence number the caller must observe replay past.</param>
        /// <param name="node">The calling session's reusable waiter (armed here, never allocated per wait).</param>
        /// <param name="timeout">Maximum time to block before failing the consistent read.</param>
        /// <param name="ct">Cancellation token for the read.</param>
        public void WaitForSequenceNumber(long maximumSessionSequenceNumber, ReadSessionWaiter node, TimeSpan timeout, CancellationToken ct)
        {
            // Phase 1: SpinWait — fast path when replay is keeping up
            var spinner = new SpinWait();
            for (var i = 0; i < MaxSpinCount; i++)
            {
                if (maximumSessionSequenceNumber < Volatile.Read(ref sublogReplayMetadata.Frontier))
                    return;
                spinner.SpinOnce(sleep1Threshold: -1);
            }

            // Phase 2: Arm the session's reusable waiter and block. The node is fully unlinked from
            // any list between waits and touched by only this session's thread, so re-arming (which
            // clears stale list pointers and resets the wakeup event) needs no synchronization.
            node.Reset(maximumSessionSequenceNumber);

            lock (@lock)
            {
                if (maximumSessionSequenceNumber < Volatile.Read(ref sublogReplayMetadata.Frontier))
                    return;

                // Insert first, then re-check: if an updater raced with us
                // (SignalWaiters saw waiterHead == null before we inserted),
                // we catch it here and unlink before blocking.
                InsertWaiter(node);
                UpdateMinWaiterTarget();
                // Make the enqueue visible before the recheck of  max , so a decision to block can never race ahead of the signaler's ability to see that you enqueued
                Interlocked.MemoryBarrier();
                if (maximumSessionSequenceNumber < Volatile.Read(ref sublogReplayMetadata.Frontier))
                {
                    // Unlink directly — we already hold the lock
                    if (node.Prev != null)
                        node.Prev.Next = node.Next;
                    else if (waiterHead == node)
                        waiterHead = node.Next;
                    _ = (node.Next?.Prev = node.Prev);
                    UpdateMinWaiterTarget();
                    return;
                }
            }

            try
            {
                if (!node.Signal.Wait(timeout, ct))
                {
                    RemoveWaiter(node);
                    ExceptionUtils.ThrowException(new TimeoutException("Consistent read timed out waiting for replay to catch up."));
                }
            }
            catch (OperationCanceledException)
            {
                RemoveWaiter(node);
                throw;
            }
        }

        /// <summary>
        /// Inserts a waiter node into the sorted linked list (ascending by target sequence number).
        /// Must be called under lock.
        /// </summary>
        private void InsertWaiter(ReadSessionWaiter node)
        {
            if (waiterHead == null || node.TargetSequenceNumber <= waiterHead.TargetSequenceNumber)
            {
                // Insert at head
                node.Next = waiterHead;
                _ = (waiterHead?.Prev = node);
                waiterHead = node;
                return;
            }

            // Walk to find insertion point
            var current = waiterHead;
            while (current.Next != null && current.Next.TargetSequenceNumber <= node.TargetSequenceNumber)
                current = current.Next;

            // Insert after current
            node.Next = current.Next;
            node.Prev = current;
            _ = (current.Next?.Prev = node);
            current.Next = node;
        }

        /// <summary>
        /// Removes a waiter node from the linked list in O(1). Used on timeout/cancellation.
        /// </summary>
        private void RemoveWaiter(ReadSessionWaiter node)
        {
            lock (@lock)
            {
                if (node.Prev != null)
                    node.Prev.Next = node.Next;
                else if (waiterHead == node)
                    waiterHead = node.Next;

                _ = (node.Next?.Prev = node.Prev);

                node.Prev = null;
                node.Next = null;
                UpdateMinWaiterTarget();
            }
        }

        /// <summary>
        /// Refreshes <see cref="SublogReplayMetadata.MinSequenceNumberTarget"/> to the current head's target
        /// (or <see cref="long.MaxValue"/> when the list is empty). Must be called under
        /// <see cref="@lock"/>; every mutation of <see cref="waiterHead"/> is followed by this so the
        /// lock-free skip in <see cref="SignalWaiters"/> observes an accurate smallest target.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateMinWaiterTarget()
            => Volatile.Write(ref sublogReplayMetadata.MinSequenceNumberTarget, waiterHead?.TargetSequenceNumber ?? long.MaxValue);
    }

    /// <summary>
    /// Reusable intrusive waiter node owned by a single reader session. Bundles the sorted-list
    /// pointers, the current wait target, and a persistent wakeup event. A session waits on at most
    /// one sublog at a time and processes reads sequentially, so a single node is reused across every
    /// wait — removing the per-wait allocation of a node plus <see cref="ManualResetEventSlim"/> on
    /// the blocking path. The node is always fully unlinked from any list before a wait returns.
    /// </summary>
    internal sealed class ReadSessionWaiter : IDisposable
    {
        /// <summary>
        /// Sequence number this waiter is currently blocked until. Set by <see cref="Reset"/> before
        /// each wait; read under the sublog lock while the node is linked.
        /// </summary>
        public long TargetSequenceNumber;

        /// <summary>
        /// Persistent wakeup event, allocated once and reset before each wait rather than per wait.
        /// </summary>
        public readonly ManualResetEventSlim Signal = new(false);

        public ReadSessionWaiter Prev;
        public ReadSessionWaiter Next;

        /// <summary>
        /// Prepares the node for a fresh wait: sets the target, clears stale list pointers left by a
        /// prior unlink, and resets the wakeup event (clearing a possible lingering signal from the
        /// previous wait). Safe without synchronization because the node is unlinked and single-owner
        /// between waits.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset(long targetSequenceNumber)
        {
            TargetSequenceNumber = targetSequenceNumber;
            Prev = null;
            Next = null;
            Signal.Reset();
        }

        public void Dispose() => Signal.Dispose();
    }
}