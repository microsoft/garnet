// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Supported quantizations of vector data.
    /// 
    /// This controls the mapping of vector elements to how they're actually stored.
    /// </summary>
    public enum VectorQuantType
    {
        Invalid = 0,

        // Redis quantiziations

        /// <summary>
        /// Provided and stored as floats (FP32).
        /// </summary>
        NoQuant,
        /// <summary>
        /// Provided as FP32, stored as binary (1 bit).
        /// </summary>
        Bin,
        /// <summary>
        /// Provided as FP32, stored as bytes (8 bits).
        /// </summary>
        Q8,

        // Extended quantizations

        /// <summary>
        /// Provided and stored as bytes (8 bits).
        /// </summary>
        XPreQ8,
    }

    /// <summary>
    /// Supported formats for Vector value data.
    /// </summary>
    public enum VectorValueType : int
    {
        Invalid = 0,

        // Redis formats

        /// <summary>
        /// Floats (FP32).
        /// </summary>
        FP32,

        // Extended formats

        /// <summary>
        /// Bytes (8 bit).
        /// </summary>
        XB8,
    }

    /// <summary>
    /// How result ids are formatted in responses from DiskANN.
    /// </summary>
    public enum VectorIdFormat : int
    {
        Invalid = 0,

        /// <summary>
        /// Has 4 bytes of unsigned length before the data.
        /// </summary>
        I32LengthPrefixed,

        /// <summary>
        /// Ids are actually 4-byte ints, no prefix.
        /// </summary>
        FixedI32
    }

    /// <summary>
    /// Implementation of Vector Set operations.
    /// </summary>
    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// Implement Vector Set Add - this may also create a Vector Set if one does not already exist.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetAdd(SpanByte key, int reduceDims, VectorValueType valueType, ArgSlice values, ArgSlice element, VectorQuantType quantizer, int buildExplorationFactor, ArgSlice attributes, int numLinks, out VectorManagerResult result, out ReadOnlySpan<byte> errorMsg)
        {
            int dims;
            if (valueType == VectorValueType.FP32)
            {
                dims = values.ReadOnlySpan.Length / sizeof(float);
            }
            else if (valueType == VectorValueType.XB8)
            {
                dims = values.ReadOnlySpan.Length;
            }
            else
            {
                throw new NotImplementedException($"{valueType}");
            }

            var dimsArg = new ArgSlice((byte*)&dims, sizeof(uint));
            var reduceDimsArg = new ArgSlice((byte*)&reduceDims, sizeof(uint));
            var valueTypeArg = new ArgSlice((byte*)&valueType, sizeof(VectorValueType));
            var valuesArg = values;
            var elementArg = element;
            var quantizerArg = new ArgSlice((byte*)&quantizer, sizeof(VectorQuantType));
            var buildExplorationFactorArg = new ArgSlice((byte*)&buildExplorationFactor, sizeof(uint));
            var attributesArg = attributes;
            var numLinksArg = new ArgSlice((byte*)&numLinks, sizeof(uint));

            parseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg]);

            var input = new RawStringInput(RespCommand.VADD, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadOrCreateVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = VectorManagerResult.Invalid;
                    errorMsg = default;
                    return status;
                }

                // After a successful read we add the vector while holding a shared lock
                // That lock prevents deletion, but everything else can proceed in parallel
                result = vectorManager.TryAdd(indexSpan, element.ReadOnlySpan, valueType, values.ReadOnlySpan, attributes.ReadOnlySpan, (uint)reduceDims, quantizer, (uint)buildExplorationFactor, (uint)numLinks, out errorMsg);

                if (result == VectorManagerResult.OK)
                {
                    // On successful addition, we need to manually replicate the write
                    vectorManager.ReplicateVectorSetAdd(ref key, ref input, ref basicContext);
                }

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Perform a similarity search on an existing Vector Set given a vector as a bunch of floats.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetValueSimilarity(SpanByte key, VectorValueType valueType, ArgSlice values, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        {
            parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            // Get the index
            var input = new RawStringInput(RespCommand.VSIM, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = VectorManagerResult.Invalid;
                    outputIdFormat = VectorIdFormat.Invalid;
                    return status;
                }

                result = vectorManager.ValueSimilarity(indexSpan, valueType, values.ReadOnlySpan, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes);

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Perform a similarity search on an existing Vector Set given an element that is already in the Vector Set.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetElementSimilarity(SpanByte key, ReadOnlySpan<byte> element, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        {
            parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            var input = new RawStringInput(RespCommand.VSIM, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    result = VectorManagerResult.Invalid;
                    outputIdFormat = VectorIdFormat.Invalid;
                    return status;
                }

                result = vectorManager.ElementSimilarity(indexSpan, element, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes);
                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Get the approximate vector associated with an element, after (approximately) reversing any transformation.
        /// </summary>
        [SkipLocalsInit]
        public unsafe GarnetStatus VectorSetEmbedding(SpanByte key, ReadOnlySpan<byte> element, ref SpanByteAndMemory outputDistances)
        {
            parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            var input = new RawStringInput(RespCommand.VEMB, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    return status;
                }

                if (!vectorManager.TryGetEmbedding(indexSpan, element, ref outputDistances))
                {
                    return GarnetStatus.NOTFOUND;
                }

                return GarnetStatus.OK;
            }
        }

        [SkipLocalsInit]
        internal unsafe GarnetStatus VectorSetDimensions(SpanByte key, out int dimensions)
        {
            parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            var input = new RawStringInput(RespCommand.VDIM, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (vectorManager.ReadVectorIndex(this, ref key, ref input, indexSpan, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    dimensions = 0;
                    return status;
                }

                // No need to recreate, all of this data is available to Garnet alone

                // After a successful read we add the vector while holding a shared lock
                // That lock prevents deletion, but everything else can proceed in parallel
                VectorManager.ReadIndex(indexSpan, out _, out var dimensionsUS, out var reducedDimensionsUS, out _, out _, out _, out _, out _);

                dimensions = (int)(reducedDimensionsUS == 0 ? dimensionsUS : reducedDimensionsUS);

                return GarnetStatus.OK;
            }
        }

        /// <summary>
        /// Deletion of a Vector Set needs special handling.
        /// 
        /// This is called by DEL and UNLINK after a naive delete fails for us to _try_ and delete a Vector Set.
        /// </summary>
        [SkipLocalsInit]
        private unsafe Status TryDeleteVectorSet(ref SpanByte key)
        {
            parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            var input = new RawStringInput(RespCommand.VADD, ref parseState);

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            Span<TxnKeyEntry> exclusiveLocks = stackalloc TxnKeyEntry[vectorManager.readLockShardCount];

            using (vectorManager.ReadForDeleteVectorIndex(this, ref key, ref input, indexSpan, exclusiveLocks, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    // This can happen is something else successfully deleted before we acquired the lock
                    return Status.CreateNotFound();
                }

                vectorManager.DropIndex(indexSpan);

                // Update the index to be delete-able
                var updateToDroppableVectorSet = new RawStringInput();
                updateToDroppableVectorSet.arg1 = VectorManager.DeleteAfterDropArg;
                updateToDroppableVectorSet.header.cmd = RespCommand.VADD;

                var update = basicContext.RMW(ref key, ref updateToDroppableVectorSet);
                if (!update.IsCompletedSuccessfully)
                {
                    throw new GarnetException("Failed to make Vector Set delete-able, this should never happen but will leave vector sets corrupted");
                }

                // Actually delete the value
                var del = basicContext.Delete(ref key);
                if (!del.IsCompletedSuccessfully)
                {
                    throw new GarnetException("Failed to delete dropped Vector Set, this should never happen but will leave vector sets corrupted");
                }

                // Cleanup incidental additional state
                vectorManager.DropVectorSetReplicationKey(key, ref basicContext);

                // TODO: This doesn't clean up element data, we should do that...  or DiskANN should do that, we'll figure it out later

                return Status.CreateFound();
            }
        }
    }
}