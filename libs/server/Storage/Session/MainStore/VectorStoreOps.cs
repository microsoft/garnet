// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
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
        public GarnetStatus VectorSetAdd(SpanByte key, int reduceDims, VectorValueType valueType, ArgSlice values, ArgSlice element, VectorQuantType quantizer, int buildExplorationFactor, ArgSlice attributes, int numLinks, out VectorManagerResult result, out ReadOnlySpan<byte> errorMsg)
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

            var dimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref dims, 1)));
            var reduceDimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
            var valueTypeArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorValueType, byte>(MemoryMarshal.CreateSpan(ref valueType, 1)));
            var valuesArg = values;
            var elementArg = element;
            var quantizerArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantizer, 1)));
            var buildExplorationFactorArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
            var attributesArg = attributes;
            var numLinksArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));

            parseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg]);

            var input = new RawStringInput(RespCommand.VADD, ref parseState);

            Span<byte> resSpan = stackalloc byte[128];
            var indexConfig = SpanByteAndMemory.FromPinnedSpan(resSpan);

            TxnKeyEntry vectorLockEntry = new();
            vectorLockEntry.isObject = false;
            vectorLockEntry.keyHash = lockableContext.GetKeyHash(key);

            var lockCtx = objectStoreLockableContext;

            lockCtx.BeginLockable();

            try
            {
            tryAgain:
                vectorLockEntry.lockType = LockType.Shared;

                lockCtx.Lock([vectorLockEntry]);

                try
                {
                    using (vectorManager.Enter(this))
                    {
                        var readRes = Read_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
                        if (readRes == GarnetStatus.NOTFOUND)
                        {
                            if (!lockCtx.TryPromoteLock(vectorLockEntry))
                            {
                                goto tryAgain;
                            }

                            vectorLockEntry.lockType = LockType.Exclusive;

                            var writeRes = RMW_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
                            if (writeRes == GarnetStatus.OK)
                            {
                                // Try again so we don't hold an exclusive lock while adding a vector (which might be time consuming)
                                goto tryAgain;
                            }
                        }
                        else if (readRes != GarnetStatus.OK)
                        {
                            result = VectorManagerResult.Invalid;
                            errorMsg = default;
                            return readRes;
                        }

                        if (vectorLockEntry.lockType != LockType.Shared)
                        {
                            logger?.LogCritical("Held exclusive lock when adding to vector set, should never happen");
                            throw new GarnetException("Held exclusive lock when adding to vector set, should never happen");
                        }

                        // After a successful read we add the vector while holding a shared lock
                        // That lock prevents deletion, but everything else can proceed in parallel
                        result = vectorManager.TryAdd(indexConfig.AsReadOnlySpan(), element.ReadOnlySpan, valueType, values.ReadOnlySpan, attributes.ReadOnlySpan, (uint)reduceDims, quantizer, (uint)buildExplorationFactor, (uint)numLinks, out errorMsg);

                        if (result == VectorManagerResult.OK)
                        {
                            // On successful addition, we need to manually replicate the write
                            vectorManager.ReplicateVectorSetAdd(key, ref input, ref basicContext);
                        }

                        return GarnetStatus.OK;
                    }
                }
                finally
                {
                    lockCtx.Unlock([vectorLockEntry]);
                }
            }
            finally
            {
                lockCtx.EndLockable();
            }
        }

        /// <summary>
        /// Perform a similarity search on an existing Vector Set given a vector as a bunch of floats.
        /// </summary>
        public GarnetStatus VectorSetValueSimilarity(SpanByte key, VectorValueType valueType, ArgSlice values, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        {
            // Need to lock to prevent the index from being dropped while we read against it
            //
            // Note that this does not block adding vectors to the set, as that can also be done under
            // a shared lock
            var lockCtx = objectStoreLockableContext;

            lockCtx.BeginLockable();
            try
            {
                TxnKeyEntry vectorLockEntry = new();
                vectorLockEntry.isObject = false;
                vectorLockEntry.keyHash = lockableContext.GetKeyHash(key);
                vectorLockEntry.lockType = LockType.Shared;

                lockCtx.Lock([vectorLockEntry]);

                try
                {
                    parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

                    // Get the index
                    var input = new RawStringInput(RespCommand.VSIM, ref parseState);

                    Span<byte> resSpan = stackalloc byte[128];
                    var indexConfig = SpanByteAndMemory.FromPinnedSpan(resSpan);

                    var readRes = Read_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
                    if (readRes != GarnetStatus.OK)
                    {
                        result = VectorManagerResult.Invalid;
                        outputIdFormat = VectorIdFormat.Invalid;
                        return readRes;
                    }

                    // After a successful read we add the vector while holding a shared lock
                    // That lock prevents deletion, but everything else can proceed in parallel
                    using (vectorManager.Enter(this))
                    {
                        result = vectorManager.ValueSimilarity(indexConfig.AsReadOnlySpan(), valueType, values.ReadOnlySpan, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes);
                    }

                    return GarnetStatus.OK;
                }
                finally
                {
                    lockCtx.Unlock([vectorLockEntry]);
                }
            }
            finally
            {
                lockCtx.EndLockable();
            }
        }

        /// <summary>
        /// Perform a similarity search on an existing Vector Set given an element that is already in the Vector Set.
        /// </summary>
        public GarnetStatus VectorSetElementSimilarity(SpanByte key, ReadOnlySpan<byte> element, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, bool includeAttributes, ref SpanByteAndMemory outputIds, out VectorIdFormat outputIdFormat, ref SpanByteAndMemory outputDistances, ref SpanByteAndMemory outputAttributes, out VectorManagerResult result)
        {
            // Need to lock to prevent the index from being dropped while we read against it
            //
            // Note that this does not block adding vectors to the set, as that can also be done under
            // a shared lock
            var lockCtx = objectStoreLockableContext;

            lockCtx.BeginLockable();
            try
            {
                TxnKeyEntry vectorLockEntry = new();
                vectorLockEntry.isObject = false;
                vectorLockEntry.keyHash = lockableContext.GetKeyHash(key);
                vectorLockEntry.lockType = LockType.Shared;

                lockCtx.Lock([vectorLockEntry]);

                try
                {
                    parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

                    var input = new RawStringInput(RespCommand.VSIM, ref parseState);

                    Span<byte> resSpan = stackalloc byte[128];
                    var indexConfig = SpanByteAndMemory.FromPinnedSpan(resSpan);

                    var readRes = Read_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
                    if (readRes != GarnetStatus.OK)
                    {
                        result = VectorManagerResult.Invalid;
                        outputIdFormat = VectorIdFormat.Invalid;
                        return readRes;
                    }

                    // After a successful read we add the vector while holding a shared lock
                    // That lock prevents deletion, but everything else can proceed in parallel
                    using (vectorManager.Enter(this))
                    {
                        result = vectorManager.ElementSimilarity(indexConfig.AsReadOnlySpan(), element, count, delta, searchExplorationFactor, filter, maxFilteringEffort, includeAttributes, ref outputIds, out outputIdFormat, ref outputDistances, ref outputAttributes);
                    }

                    return GarnetStatus.OK;
                }
                finally
                {
                    lockCtx.Unlock([vectorLockEntry]);
                }
            }
            finally
            {
                lockCtx.EndLockable();
            }
        }

        /// <summary>
        /// Get the approximate vector associated with an element, after (approximately) reversing any transformation.
        /// </summary>
        public GarnetStatus VectorSetEmbedding(SpanByte key, ReadOnlySpan<byte> element, ref SpanByteAndMemory outputDistances)
        {
            // Need to lock to prevent the index from being dropped while we read against it
            //
            // Note that this does not block adding vectors to the set, as that can also be done under
            // a shared lock
            var lockCtx = objectStoreLockableContext;

            lockCtx.BeginLockable();
            try
            {
                TxnKeyEntry vectorLockEntry = new();
                vectorLockEntry.isObject = false;
                vectorLockEntry.keyHash = lockableContext.GetKeyHash(key);
                vectorLockEntry.lockType = LockType.Shared;

                lockCtx.Lock([vectorLockEntry]);

                try
                {
                    parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

                    var input = new RawStringInput(RespCommand.VEMB, ref parseState);

                    Span<byte> resSpan = stackalloc byte[128];
                    var indexConfig = SpanByteAndMemory.FromPinnedSpan(resSpan);

                    var readRes = Read_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
                    if (readRes != GarnetStatus.OK)
                    {
                        return readRes;
                    }

                    // After a successful read we add the vector while holding a shared lock
                    // That lock prevents deletion, but everything else can proceed in parallel
                    using (vectorManager.Enter(this))
                    {
                        if (!vectorManager.TryGetEmbedding(indexConfig.AsReadOnlySpan(), element, ref outputDistances))
                        {
                            return GarnetStatus.NOTFOUND;
                        }
                    }

                    return GarnetStatus.OK;
                }
                finally
                {
                    lockCtx.Unlock([vectorLockEntry]);
                }
            }
            finally
            {
                lockCtx.EndLockable();
            }
        }

        internal GarnetStatus VectorSetDimensions(SpanByte key, out int dimensions)
        {
            // Need to lock to prevent the index from being dropped while we read against it
            //
            // Note that this does not block adding vectors to the set, as that can also be done under
            // a shared lock
            var lockCtx = objectStoreLockableContext;

            lockCtx.BeginLockable();
            try
            {
                TxnKeyEntry vectorLockEntry = new();
                vectorLockEntry.isObject = false;
                vectorLockEntry.keyHash = lockableContext.GetKeyHash(key);
                vectorLockEntry.lockType = LockType.Shared;

                lockCtx.Lock([vectorLockEntry]);

                try
                {
                    parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

                    var input = new RawStringInput(RespCommand.VDIM, ref parseState);

                    Span<byte> resSpan = stackalloc byte[128];
                    var indexConfig = SpanByteAndMemory.FromPinnedSpan(resSpan);

                    var readRes = Read_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
                    if (readRes != GarnetStatus.OK)
                    {
                        dimensions = 0;
                        return readRes;
                    }

                    // After a successful read we add the vector while holding a shared lock
                    // That lock prevents deletion, but everything else can proceed in parallel
                    VectorManager.ReadIndex(indexConfig.AsReadOnlySpan(), out _, out var dimensionsUS, out var reducedDimensionsUS, out _, out _, out _, out _);

                    dimensions = (int)(reducedDimensionsUS == 0 ? dimensionsUS : reducedDimensionsUS);

                    return GarnetStatus.OK;
                }
                finally
                {
                    lockCtx.Unlock([vectorLockEntry]);
                }
            }
            finally
            {
                lockCtx.EndLockable();
            }
        }

        /// <summary>
        /// Deletion of a Vector Set needs special handling.
        /// 
        /// This is called by DEL and UNLINK after a naive delete fails for us to _try_ and delete a Vector Set.
        /// </summary>
        private Status TryDeleteVectorSet(ref SpanByte key)
        {
            var lockCtx = objectStoreLockableContext;

            lockCtx.BeginLockable();

            try
            {
                // An exclusive lock is needed to prevent any active readers while the Vector Set is deleted
                TxnKeyEntry vectorLockEntry = new();
                vectorLockEntry.isObject = false;
                vectorLockEntry.keyHash = lockableContext.GetKeyHash(key);
                vectorLockEntry.lockType = LockType.Exclusive;

                lockCtx.Lock([vectorLockEntry]);

                try
                {
                    Span<byte> resSpan = stackalloc byte[128];
                    var indexConfig = SpanByteAndMemory.FromPinnedSpan(resSpan);

                    parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

                    var input = new RawStringInput(RespCommand.VADD, ref parseState);

                    // Get the index
                    var readRes = Read_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
                    if (readRes != GarnetStatus.OK)
                    {
                        // This can happen is something else successfully deleted before we acquired the lock
                        return Status.CreateNotFound();
                    }

                    // We shouldn't read a non-Vector Set value if we read anything, so this is unconditional
                    using (vectorManager.Enter(this))
                    {
                        vectorManager.DropIndex(indexConfig.AsSpan());
                    }

                    // Update the index to be delete-able
                    var updateToDropableVectorSet = new RawStringInput();
                    updateToDropableVectorSet.arg1 = VectorManager.DeleteAfterDropArg;
                    updateToDropableVectorSet.header.cmd = RespCommand.VADD;

                    var update = basicContext.RMW(ref key, ref updateToDropableVectorSet);
                    if (!update.IsCompletedSuccessfully)
                    {
                        throw new GarnetException("Failed to make Vector Set delete-able, this should never happen but will leave vector sets corrupted");
                    }

                    // Actually delte the value
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
                finally
                {
                    lockCtx.Unlock([vectorLockEntry]);
                }
            }
            finally
            {
                lockCtx.EndLockable();
            }
        }

    }
}