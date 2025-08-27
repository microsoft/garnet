// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Garnet.server
{
    public enum VectorQuantType
    {
        Invalid = 0,
        NoQuant,

        Bin,
        Q8,
    }

    /// <summary>
    /// Implementation of Vector Set operations.
    /// </summary>
    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// Implement Vector Set Add - this may also create a Vector Set if one does not already exist.
        /// </summary>
        public GarnetStatus VectorSetAdd(SpanByte key, int reduceDims, ReadOnlySpan<float> values, ArgSlice element, VectorQuantType quantizer, int buildExplorationFactor, ArgSlice attributes, int numLinks, out VectorManagerResult result)
        {
            var dims = values.Length;

            var dimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref dims, 1)));
            var reduceDimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
            var valuesArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<float, byte>(values));
            var elementArg = element;
            var quantizerArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantizer, 1)));
            var buildExplorationFactorArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
            var attributesArg = attributes;
            var numLinksArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<int, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));

            parseState.InitializeWithArguments([dimsArg, reduceDimsArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg]);

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
                        return readRes;
                    }

                    Debug.Assert(vectorLockEntry.lockType == LockType.Shared, "Shouldn't hold exclusive lock while adding to vector set");

                    // After a successful read we add the vector while holding a shared lock
                    // That lock prevents deletion, but everything else can proceed in parallel
                    result = vectorManager.TryAdd(this, indexConfig.AsReadOnlySpan(), element.ReadOnlySpan, values, attributes.ReadOnlySpan, (uint)reduceDims, quantizer, (uint)buildExplorationFactor, (uint)numLinks);

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
        /// Perform a similarity search on an existing Vector Set given a vector as a bunch of floats.
        /// </summary>
        public GarnetStatus VectorSetValueSimilarity(SpanByte key, ReadOnlySpan<float> values, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, ref SpanByteAndMemory outputIds, ref SpanByteAndMemory outputDistances, out VectorManagerResult result)
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
                        return readRes;
                    }

                    // After a successful read we add the vector while holding a shared lock
                    // That lock prevents deletion, but everything else can proceed in parallel
                    result = vectorManager.ValueSimilarity(this, indexConfig.AsReadOnlySpan(), values, count, delta, searchExplorationFactor, filter, maxFilteringEffort, ref outputIds, ref outputDistances);

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
        public GarnetStatus VectorSetElementSimilarity(SpanByte key, ReadOnlySpan<byte> element, int count, float delta, int searchExplorationFactor, ReadOnlySpan<byte> filter, int maxFilteringEffort, ref SpanByteAndMemory outputIds, ref SpanByteAndMemory outputDistances, out VectorManagerResult result)
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
                        return readRes;
                    }

                    // After a successful read we add the vector while holding a shared lock
                    // That lock prevents deletion, but everything else can proceed in parallel
                    result = vectorManager.ElementSimilarity(this, indexConfig.AsReadOnlySpan(), element, count, delta, searchExplorationFactor, filter, maxFilteringEffort, ref outputIds, ref outputDistances);

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
                    if (!vectorManager.TryGetEmbedding(this, indexConfig.AsReadOnlySpan(), element, ref outputDistances))
                    {
                        return GarnetStatus.NOTFOUND;
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
                    vectorManager.DropIndex(this, indexConfig.AsSpan());

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
