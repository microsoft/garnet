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
        object @lock = new();
        TaskCompletionSource<bool> update = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public readonly long Max => sketchMaxValue;

        public VirtualSublogReplayState()
        {
            var size = SketchSlotSize;
            if ((size & (size - 1)) != 0)
                throw new InvalidOperationException($"Size ({SketchSlotSize}) must be a power of 2");
            Array.Clear(sketch);
            sketchMaxValue = 0;
        }

        /// <summary>
        /// Gets the current frontier sequence number associated with the specified hash value.
        /// </summary>
        /// <param name="hash">The hash value for which to retrieve the frontier sequence number.</param>
        /// <returns>The frontier sequence number corresponding to the specified hash value.</returns>
        public long GetFrontierSequenceNumber(long hash)
            => Math.Max(sketch[hash & SketchSlotMask], Max);

        /// <summary>
        /// Gets the sequence number associated with the specified hash key.
        /// </summary>
        /// <param name="hash">The hash value for which to retrieve the sequence number.</param>
        /// <returns>The sequence number corresponding to the given hash key.</returns>
        public long GetKeySequenceNumber(long hash)
            => sketch[hash & SketchSlotMask];

        /// <summary>
        /// Updates the maximum observed sequence number.
        /// </summary>
        /// <remarks>Updates are thread-safe and guaranteed to be monotonically increasing.</remarks>
        /// <param name="sequenceNumber">The sequence number to compare against the current maximum.</param>
        public void UpdateMaxSequenceNumber(long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref sketchMaxValue, sequenceNumber, out _);
            SignalAdvanceTime();
        }

        /// <summary>
        /// Updates the sequence number associated with the specified key hash.
        /// </summary>
        /// <remarks>Updates are thread-safe and guaranteed to be monotonically increasing.</remarks>
        /// <param name="hash">The hash value identifying the key whose sequence number is to be updated.</param>
        /// <param name="sequenceNumber">The new sequence number to associate with the specified key hash. Must be greater than or equal to the
        /// current value to have an effect.</param>
        public void UpdateKeySequenceNumber(long hash, long sequenceNumber)
        {
            _ = Utility.MonotonicUpdate(ref sketch[hash & SketchSlotMask], sequenceNumber, out _);
            _ = Utility.MonotonicUpdate(ref sketchMaxValue, sequenceNumber, out _);
            SignalAdvanceTime();
        }

        /// <summary>
        /// Signals that time should advance, allowing any awaiting operations to proceed.
        /// </summary>
        void SignalAdvanceTime()
        {
            TaskCompletionSource<bool> release;

            lock (@lock)
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
            lock (@lock)
            {
                if (maximumSessionSequenceNumber >= GetFrontierSequenceNumber(hash))
                    return update.Task.WaitAsync(ct);
                return Task.CompletedTask;
            }
        }
    }
}
