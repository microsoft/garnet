// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    internal struct VirtualSublogReplayState
    {
        const int SketchSlotSize = 1 << 15;
        const int SketchSlotMask = SketchSlotSize - 1;

        readonly long[] sketch = new long[SketchSlotSize];
        long sketchMaxValue;
        readonly ConcurrentQueue<ReadSessionWaiter> waitQs = new();

        public long Max => sketchMaxValue;

        public VirtualSublogReplayState()
        {
            var size = SketchSlotSize;
            if ((size & (size - 1)) != 0)
                throw new InvalidOperationException($"Size ({SketchSlotSize}) must be a power of 2");
            Array.Clear(sketch);
            sketchMaxValue = 0;
        }

        public void AddWaiter(ReadSessionWaiter waiter)
            => waitQs.Enqueue(waiter);

        public long GetFrontierSequenceNumber(long hash)
            => Math.Max(sketch[hash & SketchSlotMask], Max);

        public long GetKeySequenceNumber(long hash)
            => sketch[hash & SketchSlotMask];

        public void UpdateMaxSequenceNumber(long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref sketchMaxValue, sequenceNumber, out _);
            SignalWaiters();
        }

        public void UpdateKeySequenceNumber(long hash, long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref sketch[hash & SketchSlotMask], sequenceNumber, out _);
            _ = Utility.MonotonicUpdate(ref sketchMaxValue, sequenceNumber, out _);
            SignalWaiters();
        }

        /// <summary>
        /// Signal any readers waiting for timestamp to progress
        /// </summary>        
        void SignalWaiters()
        {
            var waiterList = new List<ReadSessionWaiter>();
            while (waitQs.TryDequeue(out var waiter))
            {
                // If timestamp has not progressed enough will re-add this waiter to the waitQ
                if (waiter.rrsc.maximumSessionSequenceNumber > GetFrontierSequenceNumber(waiter.rrsc.lastHash))
                    waiterList.Add(waiter);
                else
                    // Signal for waiter to proceed
                    waiter.Set();
            }

            // Re-insert any waiters that have not been released yet
            foreach (var waiter in waiterList)
                waitQs.Enqueue(waiter);
        }
    }
}
