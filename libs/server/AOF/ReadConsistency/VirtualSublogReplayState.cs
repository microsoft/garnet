// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Tsavorite.core;

namespace Garnet.server
{
    internal struct VirtualSublogReplayState
    {
        const int SketchSlotSize = 1 << 15;
        const int SketchSlotMask = SketchSlotSize - 1;

        readonly long[] sketch = new long[SketchSlotSize];
        long sketchMaxValue;
        private readonly object _lock = new();
        TaskCompletionSource<bool> update = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public long Max => sketchMaxValue;

        public VirtualSublogReplayState()
        {
            var size = SketchSlotSize;
            if ((size & (size - 1)) != 0)
                throw new InvalidOperationException($"Size ({SketchSlotSize}) must be a power of 2");
            Array.Clear(sketch);
            sketchMaxValue = 0;
        }

        public long GetFrontierSequenceNumber(long hash)
            => Math.Max(sketch[hash & SketchSlotMask], Max);

        public long GetKeySequenceNumber(long hash)
            => sketch[hash & SketchSlotMask];

        public void UpdateMaxSequenceNumber(long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref sketchMaxValue, sequenceNumber, out _);
            SignalAdvanceTime();
        }

        public void UpdateKeySequenceNumber(long hash, long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref sketch[hash & SketchSlotMask], sequenceNumber, out _);
            _ = Utility.MonotonicUpdate(ref sketchMaxValue, sequenceNumber, out _);
            SignalAdvanceTime();
        }

        /// <summary>
        /// Signal time has advanced
        /// </summary>
        void SignalAdvanceTime()
        {
            TaskCompletionSource<bool> release;

            lock (_lock)
            {
                release = update;
                update = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            _ = release.TrySetResult(true);
        }

        /// <summary>
        /// Waits asynchronously until the session's frontier sequence number for the specified hash reaches or exceeds
        /// the given maximum sequence number.
        /// </summary>
        /// <param name="hash">The hash value identifying the session whose sequence number is being monitored.</param>
        /// <param name="maximumSessionSequenceNumber">The target sequence number to wait for.</param>
        /// <param name="ct">A cancellation token that can be used to cancel the wait operation.</param>
        /// <returns>A task that completes when the session's frontier sequence number for the specified hash reaches or exceeds
        /// the target value, or immediately if the condition is already met.</returns>
        public Task WaitForSequenceNumber(long hash, long maximumSessionSequenceNumber, CancellationToken ct)
        {
            lock (_lock)
            {
                if (maximumSessionSequenceNumber >= GetFrontierSequenceNumber(hash))
                    return update.Task.WaitAsync(ct);
                return Task.CompletedTask;
            }
        }
    }
}
