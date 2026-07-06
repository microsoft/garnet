// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Methods for managing <see cref="Index"/>, which is the information about an index created by DiskANN.
    /// 
    /// <see cref="Index"/> is stored under the "visible" key in the log, and thus is the common entry point
    /// for all operations.
    /// </summary>
    public sealed partial class VectorManager
    {
        public const int IndexSize = Index.Size;

        [StructLayout(LayoutKind.Explicit, Size = Size)]
        private struct Index
        {
            internal const int Size = 56;

            [FieldOffset(0)]
            public ulong Context;
            [FieldOffset(8)]
            public ulong IndexPtr;
            [FieldOffset(16)]
            public uint Dimensions;
            [FieldOffset(20)]
            public uint ReduceDims;
            [FieldOffset(24)]
            public uint NumLinks;
            [FieldOffset(28)]
            public uint BuildExplorationFactor;
            [FieldOffset(32)]
            public VectorQuantType QuantType;
            [FieldOffset(36)]
            public VectorDistanceMetricType DistanceMetric;
            [FieldOffset(40)]
            public VectorSetFlags Flags;

            // These used to be allocated for a GUID and can be reclaimed as necessary
            [FieldOffset(44)]
            private uint unused0;
            [FieldOffset(48)]
            private ulong unused1;
        }

        /// <summary>
        /// Construct a new index, and stash enough data to recover it with <see cref="ReadIndex"/>.
        /// </summary>
        internal void CreateIndex(
            uint dimensions,
            uint reduceDims,
            VectorQuantType quantType,
            uint buildExplorationFactor,
            uint numLinks,
            VectorDistanceMetricType distanceMetric,
            ulong newContext,
            nint newIndexPtr,
            Span<byte> indexSpan)
        {
            AssertHaveStorageSession();

            Debug.Assert((newContext % 8) == 0 && newContext != 0, "Illegal context provided");
            Debug.Assert(Unsafe.SizeOf<Index>() == Index.Size, "Constant index size is incorrect");

            if (indexSpan.Length != Index.Size)
            {
                logger?.LogCritical("Acquired space for vector set index does not match expectations, {Length} != {Size}", indexSpan.Length, Index.Size);
                throw new GarnetException($"Acquired space for vector set index does not match expectations, {indexSpan.Length} != {Index.Size}");
            }

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexSpan));
            asIndex.Context = newContext;
            asIndex.Dimensions = dimensions;
            asIndex.ReduceDims = reduceDims;
            asIndex.QuantType = quantType;
            asIndex.BuildExplorationFactor = buildExplorationFactor;
            asIndex.NumLinks = numLinks;
            asIndex.DistanceMetric = distanceMetric;
            asIndex.Flags = VectorSetFlags.None;
            asIndex.IndexPtr = (ulong)newIndexPtr;
        }

        /// <summary>
        /// Recreate an index that was created by a prior instance of Garnet.
        /// 
        /// This implies the index still has element data, but the pointer is garbage.
        /// </summary>
        internal void RecreateIndex(nint newIndexPtr, Span<byte> indexSpan)
        {
            AssertHaveStorageSession();

            if (indexSpan.Length != Index.Size)
            {
                logger?.LogCritical("Acquired space for vector set index does not match expectations, {Length} != {Size}", indexSpan.Length, Index.Size);
                throw new GarnetException($"Acquired space for vector set index does not match expectations, {indexSpan.Length} != {Index.Size}");
            }

            ReadIndex(indexSpan, out _, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);
            Debug.Assert(indexPtr == 0, "Shouldn't be recreating an index if we already have a pointer");

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexSpan));
            asIndex.IndexPtr = (ulong)newIndexPtr;
        }

        /// <summary>
        /// Drop an index previously constructed with <see cref="CreateIndex"/>.
        /// </summary>
        internal void DropIndex(ReadOnlySpan<byte> indexValue)
        {
            ReadIndex(indexValue, out var context, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

            if (indexPtr == 0)
            {
                // We never actually spun this index up, so nothing to drop
                return;
            }

            Service.DropIndex(context, indexPtr);
        }

        /// <summary>
        /// Deconstruct index stored in the value under a Vector Set index key.
        /// </summary>
        public static void ReadIndex(
            ReadOnlySpan<byte> indexValue,
            out ulong context,
            out uint dimensions,
            out uint reduceDims,
            out VectorQuantType quantType,
            out uint buildExplorationFactor,
            out uint numLinks,
            out VectorDistanceMetricType distanceMetric,
            out VectorSetFlags flags,
            out nint indexPtr
        )
        {
            Debug.Assert(indexValue.Length == Index.Size, $"Index size is incorrect ({indexValue.Length} != {Index.Size}), implies vector set index is probably corrupted");

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexValue));

            context = asIndex.Context;
            dimensions = asIndex.Dimensions;
            reduceDims = asIndex.ReduceDims;
            quantType = asIndex.QuantType;
            buildExplorationFactor = asIndex.BuildExplorationFactor;
            numLinks = asIndex.NumLinks;
            distanceMetric = asIndex.DistanceMetric;
            flags = asIndex.Flags;
            indexPtr = (nint)asIndex.IndexPtr;

            Debug.Assert((context % ContextStep) == 0, $"Context ({context}) not as expected (% 4 == {context % 4}), vector set index is probably corrupted");
        }

        /// <summary>
        /// Update the context (which defines a range of namespaces) stored in a given index.
        /// 
        /// Doing this also smashes the index pointer, so the destination node won't
        /// think it's already creating this index.
        /// </summary>
        public static void SetContextForMigration(Span<byte> indexValue, ulong newContext)
        {
            Debug.Assert(newContext != 0, "0 is special, should not be assigning to an index");
            Debug.Assert(indexValue.Length == Index.Size, $"Index size is incorrect ({indexValue.Length} != {Index.Size}), implies vector set index is probably corrupted");

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexValue));

            asIndex.Context = newContext;
            asIndex.IndexPtr = 0;
        }

        /// <summary>
        /// Issues an RMW to set the <see cref="VectorSetFlags.SuppressCleanup"/> flag on the given index.
        /// 
        /// Assumes that appropriate locking has been done to prevent concurrent modification to the index.
        /// </summary>
        internal static void MarkSuppressCleanup<TContext>(ReadOnlySpan<byte> key, ref TContext stringContext)
            where TContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions<GarnetKeyComparer, GarnetRecordTriggers>, ObjectAllocator<StoreFunctions<GarnetKeyComparer, GarnetRecordTriggers>>>
        {
            // Since we only have the one flag, setting it doesn't require a read first.
            SetFlags(key, VectorSetFlags.SuppressCleanup, ref stringContext);
        }

        /// <summary>
        /// Issues an RMW to clear the <see cref="VectorSetFlags.SuppressCleanup"/> flag on the given index.
        /// 
        /// Assumes that appropriate locking has been done to prevent concurrent modification to the index.
        /// </summary>
        internal static void ClearSuppressCleanup<TContext>(ReadOnlySpan<byte> key, ref TContext stringContext)
            where TContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions<GarnetKeyComparer, GarnetRecordTriggers>, ObjectAllocator<StoreFunctions<GarnetKeyComparer, GarnetRecordTriggers>>>
        {
            // Since we only have the one flag, clearing it is equivalent to setting to None
            SetFlags(key, VectorSetFlags.None, ref stringContext);
        }

        /// <summary>
        /// Issues an RMW to set <see cref="VectorSetFlags"/> on the given index.
        /// 
        /// Assumes that appropriate locking has been done to prevent concurrent modification to the index.
        /// </summary>
        private static void SetFlags<TContext>(ReadOnlySpan<byte> key, VectorSetFlags flags, ref TContext stringContext)
            where TContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions<GarnetKeyComparer, GarnetRecordTriggers>, ObjectAllocator<StoreFunctions<GarnetKeyComparer, GarnetRecordTriggers>>>
        {
            var input = new StringInput(RespCommand.VADD, arg1: VADDSetFlagsArg);

#pragma warning disable IDE0302 // Collection initializers don't _guarantee_ stackalloc, which is what we need here
            Span<VectorSetFlags> flagsArg = stackalloc VectorSetFlags[1] { flags };
#pragma warning restore IDE0302

            input.parseState.EnsureCapacity(1);
            input.parseState.SetArgument(0, PinnedSpanByte.FromPinnedSpan(MemoryMarshal.AsBytes(flagsArg)));

            var output = new StringOutput();

            var writeRes = stringContext.RMW((FixedSpanByteKey)key, ref input, ref output);
            if (writeRes.IsPending)
            {
                CompletePending(ref writeRes, ref stringContext);
            }

            if (!writeRes.IsCompletedSuccessfully)
            {
                throw new GarnetException("Marking existing Vector Set index flags should never fail");
            }
        }

        /// <summary>
        /// Update <see cref="Index.Flags"/> field stored in <paramref name="indexValue"/>.
        /// </summary>
        public static void SetIndexFlags(Span<byte> indexValue, VectorSetFlags flags)
        {
            Debug.Assert(indexValue.Length == Index.Size, $"Index size is incorrect ({indexValue.Length} != {Index.Size}), implies vector set index is probably corrupted");

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexValue));

            asIndex.Flags = flags;
        }
    }
}