// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Tsavorite.core;

#pragma warning disable CS0169

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Methods for managing <see cref="VectorManager.ContextMetadata"/>, which tracks process wide 
    /// information about different contexts.
    /// 
    /// <see cref="ContextMetadata"/> is persisted to the log when modified, but a copy is kept in memory for rapid access.
    /// </summary>
    public sealed partial class VectorManager
    {
        // MUST BE A POWER OF 2
        public const ulong ContextStep = 8;

        /// <summary>
        /// Used for tracking which contexts are currently active.
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = Size)]
        internal struct ContextMetadata
        {
            [InlineArray(64)]
            private struct HashSlots
            {
                private ushort element0;
            }

            internal const int Size =
                (4 * sizeof(ulong)) +   // Bitmaps
                (64 * sizeof(ushort));  // HashSlots for assigned contexts

            [FieldOffset(0)]
            public ulong Version;

            [FieldOffset(8)]
            private ulong inUse;

            [FieldOffset(16)]
            private ulong cleaningUp;

            [FieldOffset(24)]
            private ulong migrating;

            [FieldOffset(32)]
            private HashSlots slots;

            public readonly bool IsInUse(ulong context)
            {
                Debug.Assert(context > 0, "Context 0 is reserved, should never queried");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert(context <= byte.MaxValue, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                return (inUse & mask) != 0;
            }

            public readonly bool IsMigrating(ulong context)
            {
                Debug.Assert(context > 0, "Context 0 is reserved, should never queried");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert(context <= byte.MaxValue, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                return (migrating & mask) != 0;
            }

            public readonly HashSet<ulong> GetNamespacesForHashSlots(HashSet<int> hashSlots)
            {
                HashSet<ulong> ret = null;

                var remaining = inUse;
                while (remaining != 0)
                {
                    var inUseIx = BitOperations.TrailingZeroCount(remaining);
                    var inUseMask = 1UL << inUseIx;

                    remaining &= ~inUseMask;

                    if ((cleaningUp & inUseMask) != 0)
                    {
                        // If something is being cleaned up, no reason to migrate it
                        continue;
                    }

                    var hashSlot = slots[inUseIx];
                    if (!hashSlots.Contains(hashSlot))
                    {
                        // Active, but not a target
                        continue;
                    }

                    ret ??= [];

                    var nsStart = ContextStep * (ulong)inUseIx;
                    for (var i = 0U; i < ContextStep; i++)
                    {
                        _ = ret.Add(nsStart + i);
                    }
                }

                return ret;
            }

            public readonly ulong NextNotInUse()
            {
                var ignoringZero = inUse | 1;

                var bit = (ulong)BitOperations.TrailingZeroCount(~ignoringZero & (ulong)-(long)(~ignoringZero));

                if (bit == 64)
                {
                    throw new GarnetException("All possible Vector Sets allocated");
                }

                var ret = bit * ContextStep;

                return ret;
            }

            public bool TryReserveForMigration(int count, out List<ulong> reserved)
            {
                var ignoringZero = inUse | 1;

                var available = BitOperations.PopCount(~ignoringZero);

                if (available < count)
                {
                    reserved = null;
                    return false;
                }

                reserved = new();
                for (var i = 0; i < count; i++)
                {
                    var ctx = NextNotInUse();
                    reserved.Add(ctx);

                    MarkInUse(ctx, ushort.MaxValue); // HashSlot isn't known yet, so use an invalid value
                    MarkMigrating(ctx);
                }

                return true;
            }

            public void MarkInUse(ulong context, ushort hashSlot)
            {
                Debug.Assert(context > 0, "Context 0 is reserved, should never queried");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert(context <= byte.MaxValue, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((inUse & mask) == 0, "About to mark context which is already in use");
                inUse |= mask;

                slots[(int)bitIx] = hashSlot;

                Version++;
            }

            public void MarkMigrating(ulong context)
            {
                Debug.Assert(context > 0, "Context 0 is reserved, should never queried");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert(context <= byte.MaxValue, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((inUse & mask) != 0, "About to mark migrating a context which is not in use");
                Debug.Assert((migrating & mask) == 0, "About to mark migrating a context which is already migrating");
                migrating |= mask;

                Version++;
            }

            public void MarkMigrationComplete(ulong context, ushort hashSlot)
            {
                Debug.Assert(context > 0, "Context 0 is reserved, should never queried");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert(context <= byte.MaxValue, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((inUse & mask) != 0, "Should already be in use");
                Debug.Assert((migrating & mask) != 0, "Should be migrating target");
                Debug.Assert(slots[(int)bitIx] == ushort.MaxValue, "Hash slot should not be known yet");

                migrating &= ~mask;

                slots[(int)bitIx] = hashSlot;

                Version++;
            }

            public void MarkCleaningUp(ulong context)
            {
                Debug.Assert(context > 0, "Context 0 is reserved, should never queried");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert(context <= byte.MaxValue, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((inUse & mask) != 0, "About to mark for cleanup when not actually in use");
                Debug.Assert((cleaningUp & mask) == 0, "About to mark for cleanup when already marked");
                cleaningUp |= mask;

                // If this slot were migrating, it isn't anymore
                migrating &= ~mask;

                // Leave the slot around, we need it

                Version++;
            }

            public void FinishedCleaningUp(ulong context)
            {
                Debug.Assert(context > 0, "Context 0 is reserved, should never queried");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert(context <= byte.MaxValue, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((inUse & mask) != 0, "Cleaned up context which isn't in use");
                Debug.Assert((cleaningUp & mask) != 0, "Cleaned up context not marked for it");
                cleaningUp &= ~mask;
                inUse &= ~mask;

                slots[(int)bitIx] = 0;

                Version++;
            }

            public readonly HashSet<ulong> GetNeedCleanup()
            {
                if (cleaningUp == 0)
                {
                    return null;
                }

                var ret = new HashSet<ulong>();

                var remaining = cleaningUp;
                while (remaining != 0UL)
                {
                    var ix = BitOperations.TrailingZeroCount(remaining);

                    _ = ret.Add((ulong)ix * ContextStep);

                    remaining &= ~(1UL << (byte)ix);
                }

                return ret;
            }

            public readonly HashSet<ulong> GetMigrating()
            {
                if (migrating == 0)
                {
                    return null;
                }

                var ret = new HashSet<ulong>();

                var remaining = migrating;
                while (remaining != 0UL)
                {
                    var ix = BitOperations.TrailingZeroCount(remaining);

                    _ = ret.Add((ulong)ix * ContextStep);

                    remaining &= ~(1UL << (byte)ix);
                }

                return ret;
            }

            /// <inheritdoc/>
            public override readonly string ToString()
            {
                // Just for debugging purposes

                var sb = new StringBuilder();
                sb.AppendLine();
                _ = sb.AppendLine($"Version: {Version}");
                var mask = 1UL;
                var ix = 0;
                while (mask != 0)
                {
                    var isInUse = (inUse & mask) != 0;
                    var isMigrating = (migrating & mask) != 0;
                    var cleanup = (cleaningUp & mask) != 0;

                    var hashSlot = this.slots[ix];

                    if (isInUse || isMigrating || cleanup)
                    {
                        var ctxStart = (ulong)ix * ContextStep;
                        var ctxEnd = ctxStart + ContextStep - 1;

                        sb.AppendLine($"[{ctxStart:00}-{ctxEnd:00}): {(isInUse ? "in-use " : "")}{(isMigrating ? "migrating " : "")}{(cleanup ? "cleanup" : "")}");
                    }

                    mask <<= 1;
                    ix++;
                }

                return sb.ToString();
            }
        }

        private ContextMetadata contextMetadata;

        /// <summary>
        /// Get a new unique context for a vector set.
        /// 
        /// This value is guaranteed to not be shared by any other vector set in the store.
        /// </summary>
        private ulong NextVectorSetContext(ushort hashSlot)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Obtain some number of contexts for migrating Vector Sets.
        /// 
        /// The return contexts are unavailable for other use, but are not yet "live" for visibility purposes.
        /// </summary>
        public bool TryReserveContextsForMigration(ref BasicContext<VectorInput, PinnedSpanByte, long, VectorSessionFunctions, StoreFunctions, StoreAllocator> ctx, int count, out List<ulong> contexts)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Called when an index creation succeeds to flush <see cref="contextMetadata"/> into the store.
        /// </summary>
        private void UpdateContextMetadata(ref BasicContext<VectorInput, PinnedSpanByte, long, VectorSessionFunctions, StoreFunctions, StoreAllocator> ctx)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Find all namespaces in use by vector sets that are logically members of the given hash slots.
        /// 
        /// Meant for use during migration.
        /// </summary>
        public HashSet<ulong> GetNamespacesForHashSlots(HashSet<int> hashSlots)
        {
            throw new NotImplementedException();
        }
    }
}