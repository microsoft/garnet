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
        [StructLayout(LayoutKind.Explicit, Size = 128)]
        sealed class SublogReplayStateMax
        {
            [FieldOffset(64)] public long Value;

            /// <summary>
            /// Smallest target sequence number among queued waiters (the head of the sorted waiter
            /// list), or <see cref="long.MaxValue"/> when the list is empty. Mirrored beside
            /// <see cref="Value"/> so the per-record signal pass can skip the waiter lock with a
            /// single lock-free compare: while max &lt;= this value no waiter can be satisfied.
            /// Refreshed under <see cref="VirtualSublogReplayState.@lock"/> at every list mutation.
            /// </summary>
            [FieldOffset(72)] public long MinWaiterTarget;
        }
        readonly SublogReplayStateMax sketchMax = new();

        /// <summary>
        /// Lock protecting the intrusive waiter list.
        /// </summary>
        readonly object @lock = new();

        /// <summary>
        /// Head of the intrusive sorted linked list of waiters (ascending by target sequence number).
        /// </summary>
        WaiterNode waiterHead;

        public readonly long Max => sketchMax.Value;

        /// <summary>
        /// Reference to the max value for Volatile.Read access from external callers.
        /// </summary>
        public ref long MaxRef => ref sketchMax.Value;

        public VirtualSublogReplayState()
        {
            var size = SketchSlotSize;
            if ((size & (size - 1)) != 0)
                throw new InvalidOperationException($"Size ({SketchSlotSize}) must be a power of 2");
            Array.Clear(sketch);
            sketchMax.Value = 0;
            sketchMax.MinWaiterTarget = long.MaxValue;
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
                        Volatile.Read(ref Unsafe.AsRef(in sketchMax.Value)));

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
            if (sequenceNumber > sketchMax.Value)
                Volatile.Write(ref sketchMax.Value, sequenceNumber);
            SignalWaiters();
        }

        /// <summary>
        /// Updates the sequence number associated with the specified key hash.
        /// </summary>
        /// <remarks>Updates are thread-safe and guaranteed to be monotonically increasing.</remarks>
        public void UpdateKeySequenceNumber(long hash, long sequenceNumber)
        {
            if (sequenceNumber > sketch[GetSketchSlot(hash)])
                Volatile.Write(ref sketch[GetSketchSlot(hash)], sequenceNumber);
            SignalWaiters();
        }

        /// <summary>
        /// Signals waiters whose target sequence numbers have been reached.
        /// Walks from the head (lowest target) and signals all satisfied waiters via O(1) unlink.
        /// </summary>
        private void SignalWaiters()
        {
            if (waiterHead == null)
                return;

            // Make the  max  publish visible before the read of  minT , so a decision to skip can never race ahead of the waiter's ability to see the new  max 
            Interlocked.MemoryBarrier();
            if (Volatile.Read(ref sketchMax.Value) <= Volatile.Read(ref sketchMax.MinWaiterTarget))
                return;

            lock (@lock)
            {
                var currentMax = Volatile.Read(ref sketchMax.Value);
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
        public void WaitForSequenceNumber(long maximumSessionSequenceNumber, TimeSpan timeout, CancellationToken ct)
        {
            // Phase 1: SpinWait — fast path when replay is keeping up
            var spinner = new SpinWait();
            for (var i = 0; i < MaxSpinCount; i++)
            {
                if (maximumSessionSequenceNumber < Volatile.Read(ref sketchMax.Value))
                    return;
                spinner.SpinOnce(sleep1Threshold: -1);
            }

            // Phase 2: Register in waiter list and block
            using var signal = new ManualResetEventSlim(false);
            var node = new WaiterNode(maximumSessionSequenceNumber, signal);

            lock (@lock)
            {
                if (maximumSessionSequenceNumber < Volatile.Read(ref sketchMax.Value))
                    return;

                // Insert first, then re-check: if an updater raced with us
                // (SignalWaiters saw waiterHead == null before we inserted),
                // we catch it here and unlink before blocking.
                InsertWaiter(node);
                UpdateMinWaiterTarget();
                // Make the enqueue visible before the recheck of  max , so a decision to block can never race ahead of the signaler's ability to see that you enqueued
                Interlocked.MemoryBarrier();
                if (maximumSessionSequenceNumber < Volatile.Read(ref sketchMax.Value))
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
                if (!signal.Wait(timeout, ct))
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
        private void InsertWaiter(WaiterNode node)
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
        private void RemoveWaiter(WaiterNode node)
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
        /// Refreshes <see cref="SublogReplayStateMax.MinWaiterTarget"/> to the current head's target
        /// (or <see cref="long.MaxValue"/> when the list is empty). Must be called under
        /// <see cref="@lock"/>; every mutation of <see cref="waiterHead"/> is followed by this so the
        /// lock-free skip in <see cref="SignalWaiters"/> observes an accurate smallest target.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateMinWaiterTarget()
            => Volatile.Write(ref sketchMax.MinWaiterTarget, waiterHead?.TargetSequenceNumber ?? long.MaxValue);

        /// <summary>
        /// Intrusive linked list node for a waiter. Holds prev/next pointers, the target
        /// sequence number, and a signal to wake the waiting thread.
        /// </summary>
        sealed class WaiterNode(long targetSequenceNumber, ManualResetEventSlim signal)
        {
            public readonly long TargetSequenceNumber = targetSequenceNumber;
            public readonly ManualResetEventSlim Signal = signal;
            public WaiterNode Prev;
            public WaiterNode Next;
        }
    }
}