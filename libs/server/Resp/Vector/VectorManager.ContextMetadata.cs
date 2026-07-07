// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Methods for managing <see cref="VectorManager.ContextMetadata"/>, which tracks process wide 
    /// information about different contexts.
    /// 
    /// <see cref="ContextMetadata"/> is persisted to the log when modified, but a copy is kept in memory for rapid access.
    /// </summary>
    public sealed partial class VectorManager
    {
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


            public readonly bool IsEmpty
            => inUse == 0 && migrating == 0 && cleaningUp == 0;

            public readonly bool IsInUse(bool allowZero, ushort context)
            {
                Debug.Assert(allowZero || context != 0, "Zero context not permitted here");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert((context / ContextStep) < 64, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                return (inUse & mask) != 0;
            }

            public readonly bool IsMigrating(bool allowZero, ushort context)
            {
                Debug.Assert(allowZero || context != 0, "Zero context not permitted here");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert((context / ContextStep) < 64, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                return (migrating & mask) != 0;
            }

            public readonly bool IsCleaningUp(bool allowZero, ulong context)
            {
                Debug.Assert(allowZero || context != 0, "Zero context not permitted here");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert((context / ContextStep) < 64, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                return (cleaningUp & mask) == mask;
            }

            public readonly HashSet<ushort> GetNamespacesForHashSlots(HashSet<int> hashSlots)
            {
                HashSet<ushort> ret = null;

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

                    var nsStart = (ushort)(ContextStep * (ulong)inUseIx);
                    for (var i = 0U; i < ContextStep; i++)
                    {
                        _ = ret.Add((ushort)(nsStart + i));
                    }
                }

                return ret;
            }

            public readonly ushort? NextNotInUse(bool allowZero)
            {
                var ignoringUnusuable = inUse;

                if (!allowZero)
                {
                    // Overall context 0 is reserved, but blocks past the first can assign it
                    ignoringUnusuable |= 1;
                }

                var bit = BitOperations.TrailingZeroCount(~ignoringUnusuable & (ulong)-(long)~ignoringUnusuable);

                if (bit == 64)
                {
                    return null;
                }

                var ret = (ushort)(bit * (int)ContextStep);

                return ret;
            }

            public bool TryReserveForMigration(bool allowZero, int count, out List<ushort> reserved)
            {
                var availableMask = inUse;

                if (!allowZero)
                {
                    availableMask |= 1;
                }

                var available = BitOperations.PopCount(~availableMask);

                if (available < count)
                {
                    reserved = null;
                    return false;
                }

                reserved = [];
                for (var i = 0; i < count; i++)
                {
                    var ctx = NextNotInUse(allowZero);

                    Debug.Assert(ctx != null, "Context should be locked, so this should never fail");

                    reserved.Add(ctx.Value);

                    MarkInUse(allowZero, ctx.Value, ushort.MaxValue); // HashSlot isn't known yet, so use an invalid value
                    MarkMigrating(allowZero, ctx.Value);
                }

                return true;
            }

            public void MarkInUse(bool allowZero, ushort context, ushort hashSlot)
            {
                Debug.Assert(allowZero || context != 0, "Zero context not permitted here");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert((context / ContextStep) < 64, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((inUse & mask) == 0, "About to mark context which is already in use");
                inUse |= mask;

                slots[(int)bitIx] = hashSlot;

                Version++;
            }

            public void MarkMigrating(bool allowZero, ushort context)
            {
                Debug.Assert(allowZero || context != 0, "Zero context not permitted here");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert((context / ContextStep) < 64, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((inUse & mask) != 0, "About to mark migrating a context which is not in use");
                Debug.Assert((migrating & mask) == 0, "About to mark migrating a context which is already migrating");
                migrating |= mask;

                Version++;
            }

            public void MarkMigrationComplete(bool allowZero, ushort context, ushort hashSlot)
            {
                Debug.Assert(allowZero || context != 0, "Zero context not permitted here");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert((context / ContextStep) < 64, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((inUse & mask) != 0, "Should already be in use");
                Debug.Assert((migrating & mask) != 0, "Should be migrating target");
                Debug.Assert(slots[(int)bitIx] == ushort.MaxValue, "Hash slot should not be known yet");

                migrating &= ~mask;

                slots[(int)bitIx] = hashSlot;

                Version++;
            }

            public void MarkCleaningUp(bool allowZero, ushort context)
            {
                Debug.Assert(allowZero || context != 0, "Zero context not permitted here");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert((context / ContextStep) < 64, "Context larger than expected");

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

            public void ClearIsCleaningUp(bool allowZero, ushort context)
            {
                Debug.Assert(allowZero || context != 0, "Zero context not permitted here");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert((context / ContextStep) < 64, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((inUse & mask) != 0, "Should be in use if was marked for cleanup");
                Debug.Assert((cleaningUp & mask) != 0, "About to clear cleanup when not marked for cleanup");
                cleaningUp &= ~mask;

                Version++;
            }

            public void FinishedCleaningUp(bool allowZero, ushort context)
            {
                Debug.Assert(allowZero || context != 0, "Zero context not permitted here");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert((context / ContextStep) < 64, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((inUse & mask) != 0, "Cleaned up context which isn't in use");
                Debug.Assert((cleaningUp & mask) != 0, "Cleaned up context not marked for it");
                cleaningUp &= ~mask;
                inUse &= ~mask;

                slots[(int)bitIx] = 0;

                Version++;
            }

            public void UpdateHashSlot(bool allowZero, ushort context, ushort slot)
            {
                Debug.Assert(allowZero || context != 0, "Zero context not permitted here");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert((context / ContextStep) < 64, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((inUse & mask) != 0, "Updating hash slot for context not in use");
                Debug.Assert((cleaningUp & mask) == 0, "Updating hash slot for context being cleaned up");

                slots[(int)bitIx] = slot;

                Version++;
            }

            public readonly HashSet<ushort> GetNeedCleanup()
            {
                if (cleaningUp == 0)
                {
                    return null;
                }

                var ret = new HashSet<ushort>();

                var remaining = cleaningUp;
                while (remaining != 0UL)
                {
                    var ix = BitOperations.TrailingZeroCount(remaining);

                    _ = ret.Add((ushort)(ix * (int)ContextStep));

                    remaining &= ~(1UL << (byte)ix);
                }

                return ret;
            }

            public readonly HashSet<ushort> GetMigrating()
            {
                if (migrating == 0)
                {
                    return null;
                }

                var ret = new HashSet<ushort>();

                var remaining = migrating;
                while (remaining != 0UL)
                {
                    var ix = BitOperations.TrailingZeroCount(remaining);

                    _ = ret.Add((ushort)(ix * (int)ContextStep));

                    remaining &= ~(1UL << (byte)ix);
                }

                return ret;
            }

            /// <summary>
            /// Take a context stored outside a <see cref="ContextMetadata"/> and break it into:
            ///   - an index into <see cref="contextMetadatas"/>
            ///   - a value to pass to methods on <see cref="ContextMetadata"/> instances
            /// </summary>
            internal static (int ContextMetadataIndex, ushort Context) DecomposeContext(ulong context)
            => ((int)(context / (64 * ContextStep)), (ushort)(context % (64 * ContextStep)));

            /// <summary>
            /// Given an index into <see cref="contextMetadatas"/>, return the first context that would be stored in it.
            /// </summary>
            internal static ulong OffsetForContextMetadata(int contextMetadataIndex)
            => (ulong)contextMetadataIndex * 64UL * ContextStep;

            /// <inheritdoc/>
            public override readonly string ToString()
            {
                // Just for debugging purposes

                var sb = new StringBuilder();
                _ = sb.AppendLine();
                _ = sb.AppendLine($"Version: {Version}");
                var mask = 1UL;
                var ix = 0;
                while (mask != 0)
                {
                    var isInUse = (inUse & mask) != 0;
                    var isMigrating = (migrating & mask) != 0;
                    var cleanup = (cleaningUp & mask) != 0;

                    if (isInUse || isMigrating || cleanup)
                    {
                        var ctxStart = (ulong)ix * ContextStep;
                        var ctxEnd = ctxStart + ContextStep - 1;

                        var slot = slots[ix];

                        _ = sb.AppendLine($"[{ctxStart:00}-{ctxEnd:00}): {(isInUse ? "in-use " : "")}{(isMigrating ? "migrating " : "")}{(cleanup ? "cleanup" : "")}");
                        _ = sb.AppendLine($"  - hash slot: {slot}");
                    }

                    mask <<= 1;
                    ix++;
                }

                return sb.ToString();
            }
        }

        /// <summary>
        /// Used to prevent new contexts from being issued during a FLUSHDB / FLUSHALL, as well as any new Vector Set operations from starting.
        /// 
        /// Also updates and clears cached <see cref="ContextMetadata"/> upon disposal.
        /// </summary>
        internal readonly struct FlushGuard : IDisposable
        {
            private readonly VectorManager manager;
            private readonly ReadOptimizedLock.LockToken lockToken;

            internal FlushGuard(VectorManager manager)
            {
                this.manager = manager;

                // Stop other Vector Set operations
                this.manager.vectorSetLocks.AcquireAllExclusiveLock(out lockToken);

                // Acquire a lock that will block all other attempts to issue a new context
                Monitor.Enter(this.manager);
            }

            /// <inheritdoc/>
            public readonly void Dispose()
            {
                if (manager == null)
                {
                    // This is the default instance, ignore disposal
                    return;
                }

                // Clear out all context data
                manager.contextMetadatas = new ContextMetadata[1];

                // Allow Vector Set operations again
                manager.vectorSetLocks.ReleaseLock(lockToken);

                // Allow new contexts to be issued
                Monitor.Exit(manager);
            }
        }

        private ContextMetadata[] contextMetadatas;
        private HashSet<int> dirtyContextMetadatas;

        /// <summary>
        /// Get a new unique context for a vector set.
        /// 
        /// This value is guaranteed to not be shared by any other vector set in the store.
        /// </summary>
        private ulong NextVectorSetContext(ushort hashSlot)
        {
            lock (this)
            {
                var startFrom = 0;

                while (true)
                {
                    for (var i = startFrom; i < contextMetadatas.Length; i++)
                    {
                        var nextFreeInBlock = contextMetadatas[i].NextNotInUse(i != 0);
                        if (nextFreeInBlock != null)
                        {
                            contextMetadatas[i].MarkInUse(i != 0, nextFreeInBlock.Value, hashSlot);

                            var offset = ContextMetadata.OffsetForContextMetadata(i);
                            var contextToRet = nextFreeInBlock.Value + offset;

                            _ = dirtyContextMetadatas.Add(i);

                            return contextToRet;
                        }
                    }

                    // Today we limit ourselves to uint.MaxValue _contexts_ (ContextStep per Vector Set).
                    //
                    // If a new ContextMetadata would allow us to exceed that limit, fail.
                    //
                    // This is unlikely (~8.3M Vector Sets), so treated as an error.
                    //
                    // We could raise this to ulong.MaxValue by increasing reserved space on the DiskANN size, in which case
                    // the cause of failure would be the GC refusing to allocate a large enough contextMetadatas array.
                    var limitOfNewAllocation = ContextMetadata.OffsetForContextMetadata(contextMetadatas.Length) + (64 * ContextStep);
                    if (limitOfNewAllocation > uint.MaxValue)
                    {
                        throw new GarnetException("Maximum Vector Set allocations exceeded, cannot issue new context");
                    }

                    // All allocated contexts are full, allocate more space
                    var newContextMetadatas = new ContextMetadata[contextMetadatas.Length + 1];
                    contextMetadatas.AsSpan().CopyTo(newContextMetadatas);

                    contextMetadatas = newContextMetadatas;
                    startFrom = contextMetadatas.Length - 1;

                    _ = dirtyContextMetadatas.Add(startFrom);
                }
            }
        }

        /// <summary>
        /// For testing purposes, force a number of contexts to be allocated.
        /// 
        /// Contexts are not persisted at call time, but may be persisted after future operations.
        /// </summary>
        public void AllocateTestContexts(int count)
        {
            for (var i = 0; i < count; i++)
            {
                _ = NextVectorSetContext(0);
            }

            using var session = (RespServerSession)getTempSession();
            UpdateContextMetadata(ref session.storageSession.vectorBasicContext);
        }

        /// <summary>
        /// During a FLUSHDB (or FLUSHALL) we need to prevent new contexts and other updates to context metadata.
        /// 
        /// This method is called at the start of a flush and returns a guard instance which will block such
        /// new creations until it is disposed.
        /// 
        /// This is pretty expensive, but flush should be rare and is @slow anyway.
        /// </summary>
        internal FlushGuard BeginFlush()
        {
            if (!IsEnabled)
            {
                return default;
            }

            return new FlushGuard(this);
        }

        /// <summary>
        /// Obtain some number of contexts for migrating Vector Sets.
        /// 
        /// The return contexts are unavailable for other use, but are not yet "live" for visibility purposes.
        /// </summary>
        public List<ulong> ReserveContextsForMigration(ref VectorBasicContext ctx, int count)
        {
            // At most 64 contexts can fit in a single ContextMetadata (though only 63 in the first one).
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(count, 64);

            List<ulong> contexts;

            lock (this)
            {
                var startFrom = 0;

                while (true)
                {
                    for (var i = startFrom; i < contextMetadatas.Length; i++)
                    {
                        if (contextMetadatas[i].TryReserveForMigration(i != 0, count, out var subContexts))
                        {
                            var offset = ContextMetadata.OffsetForContextMetadata(i);
                            contexts = new(count);
                            foreach (var item in subContexts)
                            {
                                contexts.Add(offset + item);
                            }

                            _ = dirtyContextMetadatas.Add(i);

                            goto persistAndReturn;
                        }
                    }

                    // Today we limit ourselves to uint.MaxValue _contexts_ (ContextStep per Vector Set).
                    //
                    // If a new ContextMetadata would allow us to exceed that limit, fail.
                    //
                    // This is unlikely (~8.3M Vector Sets), so treated as an error.
                    //
                    // We could raise this to ulong.MaxValue by increasing reserved space on the DiskANN size, in which case
                    // the cause of failure would be the GC refusing to allocate a large enough contextMetadatas array.
                    var limitOfNewAllocation = ContextMetadata.OffsetForContextMetadata(contextMetadatas.Length) + (64 * ContextStep);
                    if (limitOfNewAllocation > uint.MaxValue)
                    {
                        throw new GarnetException("Maximum Vector Set allocations exceeded, cannot issue new context");
                    }

                    // All allocated contexts are full, allocate more space
                    var newContextMetadatas = new ContextMetadata[contextMetadatas.Length + 1];
                    contextMetadatas.AsSpan().CopyTo(newContextMetadatas);

                    contextMetadatas = newContextMetadatas;
                    startFrom = contextMetadatas.Length - 1;

                    _ = dirtyContextMetadatas.Add(startFrom);
                }
            }

        persistAndReturn:
            UpdateContextMetadata(ref ctx);

            return contexts;
        }

        /// <summary>
        /// Called when an index creation succeeds to flush <see cref="contextMetadatas"/> into the store.
        /// </summary>
        private void UpdateContextMetadata(ref VectorBasicContext ctx)
        {
            Span<byte> dataSpan = stackalloc byte[ContextMetadata.Size];

#pragma warning disable IDE0302 // [...]-style collection initialization doesn't actually _guarantee_ stackalloc (or inline arrays), which we need here
            ReadOnlySpan<byte> nsBytes = stackalloc byte[1] { MetadataNamespace };
#pragma warning restore IDE0302

            Span<byte> keyBytes = stackalloc byte[sizeof(int)];

            while (true)
            {
                int contextIndex;
                lock (this)
                {
                    if (dirtyContextMetadatas.Count == 0)
                    {
                        return;
                    }

                    contextIndex = dirtyContextMetadatas.First();
                    _ = dirtyContextMetadatas.Remove(contextIndex);

                    MemoryMarshal.Cast<byte, ContextMetadata>(dataSpan)[0] = contextMetadatas[contextIndex];
                }

                BinaryPrimitives.WriteInt32LittleEndian(keyBytes, contextIndex);

                // empty key is context metadata
                VectorElementKey key = new(nsBytes, keyBytes);

                VectorInput input = default;
                input.Callback = 0;
                input.WriteDesiredSize = ContextMetadata.Size;
                unsafe
                {
                    input.CallbackContext = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dataSpan));
                }

                var status = ctx.RMW(key, ref input);

                if (status.IsPending)
                {
                    VectorOutput ignored = new();
                    CompletePending(ref status, ref ignored, ref ctx);
                }
            }
        }

        /// <summary>
        /// Find all namespaces in use by vector sets that are logically members of the given hash slots.
        /// 
        /// Meant for use during migration.
        /// </summary>
        public HashSet<ulong> GetNamespacesForHashSlots(HashSet<int> hashSlots)
        {
            HashSet<ulong> ret = null;

            lock (this)
            {
                for (var i = 0; i < contextMetadatas.Length; i++)
                {
                    var offset = ContextMetadata.OffsetForContextMetadata(i);

                    var sub = contextMetadatas[i].GetNamespacesForHashSlots(hashSlots);
                    if (sub != null)
                    {
                        ret ??= [];

                        foreach (var item in sub)
                        {
                            _ = ret.Add(offset + item);
                        }
                    }
                }
            }

            return ret;
        }

        /// <summary>
        /// Given a namespace that contains a serialized context, extract the context.
        /// 
        /// Inverse of <see cref="StoreContextInNamespace"/>.
        /// </summary>
        public static ulong ExtractContextFromNamespaces(ReadOnlySpan<byte> namespaceBytes)
        {
            Debug.Assert(namespaceBytes.Length is (sizeof(byte) or sizeof(uint)), "Namespace size unexpected");

            ulong ns;
            if (namespaceBytes.Length == 1)
            {
                ns = namespaceBytes[0];
            }
            else
            {
                ns = BinaryPrimitives.ReadUInt32LittleEndian(namespaceBytes);
            }

            return ns;
        }

        /// <summary>
        /// Given a context, store it in a span of bytes.
        /// 
        /// This handles rules about byte maximum single byte namespace and record value alignment.
        /// 
        /// Inverse of <see cref="ExtractContextFromNamespaces"/>.
        /// </summary>
        public static void StoreContextInNamespace(ulong context, ref Span<byte> namespaceBytes)
        {
            Debug.Assert(namespaceBytes.Length >= sizeof(uint), "Insufficient space in provided Span");
            Debug.Assert(context is > 0 and <= uint.MaxValue, "Context must be (0, uint.MaxValue]");

            if (context <= RecordDataHeader.MaximumSingleByteNamespaceValue)
            {
                namespaceBytes = namespaceBytes[..1];
                namespaceBytes[0] = (byte)context;
            }
            else
            {
                namespaceBytes = namespaceBytes[0..sizeof(uint)];
                BinaryPrimitives.WriteUInt32LittleEndian(namespaceBytes, (uint)context);
            }
        }

        /// <summary>
        /// Given an old and new key and the value for the new key, update the associated hash slot in context metadata.
        /// 
        /// Assumes that old and new key are locked to prevent concurrent modification.
        /// </summary>
        public void UpdateHashSlot(ReadOnlySpan<byte> oldKey, ReadOnlySpan<byte> newKey, ReadOnlySpan<byte> value, ref VectorBasicContext vectorContext)
        {
            // Not a Vector Set index
            if (value.Length != Index.Size)
            {
                logger?.LogError("Updating hash slot for {oldKey} -> {newKey} failed due to incorrect index length {actualLength} != {expectedLength}", SpanByte.ToShortString(oldKey), SpanByte.ToShortString(newKey), value.Length, Index.Size);
                return;
            }

            var oldHashSlot = HashSlotUtils.HashSlot(oldKey);
            var newHashSlot = HashSlotUtils.HashSlot(newKey);

            if (oldHashSlot == newHashSlot)
            {
                // Nothing to do
                return;
            }

            ReadIndex(value, out var context, out _, out _, out _, out _, out _, out _, out _, out _);

            var (contextIndex, contextValue) = ContextMetadata.DecomposeContext(context);

            lock (this)
            {
                contextMetadatas[contextIndex].UpdateHashSlot(contextIndex != 0, contextValue, newHashSlot);

                _ = dirtyContextMetadatas.Add(contextIndex);
            }

            UpdateContextMetadata(ref vectorContext);
        }
    }
}