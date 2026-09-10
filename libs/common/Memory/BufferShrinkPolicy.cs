// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Garnet.common
{
    /// <summary>
    /// Decides when a growable buffer that has ratcheted up to serve one unusually large request
    /// should be released back down to a baseline capacity.
    ///
    /// Per-session buffers in Garnet grow to the high-water mark of the largest request the session
    /// has ever seen and are typically pinned, so without a shrink policy a single large command
    /// permanently enlarges the session. Releasing on every reset instead would penalize a session
    /// that genuinely needs a large buffer on every batch, turning steady traffic into repeated
    /// pinned reallocation. This policy resolves both: the buffer is released only after
    /// <see cref="Hysteresis"/> consecutive reset points in which the workload did not actually use
    /// the extra capacity.
    ///
    /// Instances are single-threaded, matching the sessions that own the buffers.
    /// </summary>
    public struct BufferShrinkPolicy
    {
        /// <summary>
        /// Number of consecutive resets that must go by without needing the extra capacity before the
        /// buffer is released. Chosen to be generous: a session with recurring large payloads keeps
        /// its buffer through the burst and only gives it back after a genuinely long quiet stretch.
        /// </summary>
        public const int DefaultHysteresis = 64;

        /// <summary>
        /// Capacity value that disables shrinking entirely, preserving legacy grow-forever behavior.
        /// </summary>
        public const int Unbounded = int.MaxValue;

        /// <summary>
        /// Retained capacity, where zero means shrinking is disabled. Stored with zero as the disabled
        /// sentinel so that a default-initialized instance is inert rather than shrinking every buffer
        /// to nothing.
        /// </summary>
        readonly int maxRetainedCapacity;
        readonly int hysteresis;
        int idleResets;

        /// <summary>Capacity the buffer is allowed to retain indefinitely; only meaningful when <see cref="IsEnabled"/>.</summary>
        public readonly int MaxRetainedCapacity => maxRetainedCapacity > 0 ? maxRetainedCapacity : Unbounded;

        /// <summary>Consecutive resets required before an unused oversized buffer is released.</summary>
        public readonly int Hysteresis => hysteresis > 0 ? hysteresis : DefaultHysteresis;

        /// <summary>Whether this policy can ever shrink a buffer.</summary>
        public readonly bool IsEnabled => maxRetainedCapacity > 0;

        /// <summary>
        /// Creates a shrink policy.
        /// </summary>
        /// <param name="maxRetainedCapacity">Capacity retained indefinitely; <see cref="Unbounded"/> or zero disables shrinking.</param>
        /// <param name="hysteresis">Consecutive unused resets required before releasing.</param>
        public BufferShrinkPolicy(int maxRetainedCapacity = Unbounded, int hysteresis = DefaultHysteresis)
        {
            this.maxRetainedCapacity = maxRetainedCapacity <= 0 || maxRetainedCapacity == Unbounded ? 0 : maxRetainedCapacity;
            this.hysteresis = hysteresis < 1 ? 1 : hysteresis;
            this.idleResets = 0;
        }

        /// <summary>
        /// Evaluates one reset point and reports whether the buffer should now be released down to
        /// <see cref="MaxRetainedCapacity"/>.
        /// </summary>
        /// <param name="currentCapacity">Capacity currently held by the buffer.</param>
        /// <param name="batchHighWater">Largest number of bytes the completed batch actually used.</param>
        /// <returns>True when the caller should release the buffer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ShouldShrink(int currentCapacity, int batchHighWater)
        {
            // Disabled, or within budget. Read-mostly comparison first so the common case costs a
            // single predictable branch.
            if (maxRetainedCapacity <= 0 || currentCapacity <= maxRetainedCapacity)
            {
                idleResets = 0;
                return false;
            }

            // The batch genuinely needed the extra capacity, so keep it and restart the count.
            if (batchHighWater > maxRetainedCapacity)
            {
                idleResets = 0;
                return false;
            }

            if (++idleResets < Hysteresis)
                return false;

            idleResets = 0;
            return true;
        }

        /// <summary>
        /// Clears accumulated idle state, e.g. when the owning buffer is replaced by other means.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetIdleCount() => idleResets = 0;
    }
}