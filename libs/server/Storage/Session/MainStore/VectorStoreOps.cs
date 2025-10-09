// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Used to scope some number of locks and contexts related to a Vector Set operation.
    /// 
    /// Disposing this ends the lockable context, releases all locks, and exits the storage session context on the current thread.
    /// </summary>
    internal readonly ref struct ReadVectorLock : IDisposable
    {
        private readonly ref LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> lockableCtx;
        private readonly TxnKeyEntry entry;

        internal ReadVectorLock(ref LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> lockableCtx, TxnKeyEntry entry)
        {
            this.entry = entry;
            this.lockableCtx = ref lockableCtx;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            VectorManager.ExitStorageSessionContext();

            if (Unsafe.IsNullRef(ref lockableCtx))
            {
                return;
            }

            lockableCtx.Unlock([entry]);
            lockableCtx.EndLockable();
        }
    }

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

            Span<byte> indexSpan = stackalloc byte[VectorManager.IndexSizeBytes];

            using (ReadOrCreateVectorIndex(ref key, ref input, indexSpan, out var status))
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
                    vectorManager.ReplicateVectorSetAdd(key, ref input, ref basicContext);
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

            using (ReadVectorIndex(ref key, ref input, indexSpan, out var status))
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

            using (ReadVectorIndex(ref key, ref input, indexSpan, out var status))
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

            using (ReadVectorIndex(ref key, ref input, indexSpan, out var status))
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

            using (ReadVectorIndex(ref key, ref input, indexSpan, out var status))
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

            using (ReadForDeleteVectorIndex(ref key, ref input, indexSpan, out var status))
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

        /// <summary>
        /// Utility method that will read an vector set index out but not create one.
        /// 
        /// It will however RECREATE one if needed.
        /// 
        /// Returns a disposable that prevents the index from being deleted while undisposed.
        /// </summary>
        private ReadVectorLock ReadVectorIndex(ref SpanByte key, ref RawStringInput input, scoped Span<byte> indexSpan, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == VectorManager.IndexSizeBytes, "Insufficient space for index");

            VectorManager.EnterStorageSessionContext(this);

            TxnKeyEntry vectorLockEntry = new();
            vectorLockEntry.isObject = false;
            vectorLockEntry.keyHash = lockableContext.GetKeyHash(key);

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            ref var lockCtx = ref objectStoreLockableContext;
            lockCtx.BeginLockable();

            while (true)
            {
                vectorLockEntry.lockType = LockType.Shared;
                input.arg1 = 0;

                lockCtx.Lock([vectorLockEntry]);

                GarnetStatus readRes;
                try
                {
                    readRes = Read_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
                    Debug.Assert(indexConfig.IsSpanByte, "Should never need to move index onto the heap");
                }
                catch
                {
                    lockCtx.Unlock([vectorLockEntry]);
                    lockCtx.EndLockable();

                    throw;
                }

                var needsRecreate = readRes == GarnetStatus.OK && vectorManager.NeedsRecreate(indexConfig.AsReadOnlySpan());

                if (needsRecreate)
                {
                    if (!lockCtx.TryPromoteLock(vectorLockEntry))
                    {
                        lockCtx.Unlock([vectorLockEntry]);
                        continue;
                    }

                    input.arg1 = VectorManager.RecreateIndexArg;
                    vectorLockEntry.lockType = LockType.Exclusive;

                    GarnetStatus writeRes;

                    try
                    {
                        writeRes = RMW_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
                    }
                    catch
                    {
                        lockCtx.Unlock([vectorLockEntry]);
                        lockCtx.EndLockable();

                        throw;
                    }

                    if (writeRes == GarnetStatus.OK)
                    {
                        // Try again so we don't hold an exclusive lock while performing a search
                        lockCtx.Unlock([vectorLockEntry]);
                        continue;
                    }
                    else
                    {
                        status = writeRes;
                        lockCtx.Unlock([vectorLockEntry]);
                        lockCtx.EndLockable();

                        return default;
                    }
                }
                else if (readRes != GarnetStatus.OK)
                {
                    status = readRes;
                    lockCtx.Unlock([vectorLockEntry]);
                    lockCtx.EndLockable();

                    return default;
                }

                if (vectorLockEntry.lockType != LockType.Shared)
                {
                    lockCtx.Unlock([vectorLockEntry]);
                    lockCtx.EndLockable();

                    throw new GarnetException("Held exclusive lock after reading vector set, should never happen");
                }

                status = GarnetStatus.OK;
                return new(ref lockCtx, vectorLockEntry);
            }
        }

        /// <summary>
        /// Utility method that will read vector set index out, create one if it doesn't exist, or RECREATE one if needed.
        /// 
        /// Returns a disposable that prevents the index from being deleted while undisposed.
        /// </summary>
        private ReadVectorLock ReadOrCreateVectorIndex(ref SpanByte key, ref RawStringInput input, scoped Span<byte> indexSpan, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == VectorManager.IndexSizeBytes, "Insufficient space for index");

            VectorManager.EnterStorageSessionContext(this);

            TxnKeyEntry vectorLockEntry = new();
            vectorLockEntry.isObject = false;
            vectorLockEntry.keyHash = lockableContext.GetKeyHash(key);

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            ref var lockCtx = ref objectStoreLockableContext;
            lockCtx.BeginLockable();

            while (true)
            {
                vectorLockEntry.lockType = LockType.Shared;
                input.arg1 = 0;

                lockCtx.Lock([vectorLockEntry]);

                GarnetStatus readRes;
                try
                {
                    readRes = Read_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
                    Debug.Assert(indexConfig.IsSpanByte, "Should never need to move index onto the heap");
                }
                catch
                {
                    lockCtx.Unlock([vectorLockEntry]);
                    lockCtx.EndLockable();

                    throw;
                }

                var needsRecreate = readRes == GarnetStatus.OK && vectorManager.NeedsRecreate(indexSpan);
                if (readRes == GarnetStatus.NOTFOUND || needsRecreate)
                {
                    if (!lockCtx.TryPromoteLock(vectorLockEntry))
                    {
                        lockCtx.Unlock([vectorLockEntry]);
                        continue;
                    }

                    vectorLockEntry.lockType = LockType.Exclusive;

                    if (needsRecreate)
                    {
                        input.arg1 = VectorManager.RecreateIndexArg;
                    }

                    GarnetStatus writeRes;

                    try
                    {
                        writeRes = RMW_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
                    }
                    catch
                    {
                        lockCtx.Unlock([vectorLockEntry]);
                        lockCtx.EndLockable();

                        throw;
                    }

                    if (writeRes == GarnetStatus.OK)
                    {
                        // Try again so we don't hold an exclusive lock while adding a vector (which might be time consuming)
                        lockCtx.Unlock([vectorLockEntry]);
                        continue;
                    }
                    else
                    {
                        status = writeRes;

                        lockCtx.Unlock([vectorLockEntry]);
                        lockCtx.EndLockable();

                        return default;
                    }
                }
                else if (readRes != GarnetStatus.OK)
                {
                    lockCtx.Unlock([vectorLockEntry]);
                    lockCtx.EndLockable();

                    status = readRes;
                    return default;
                }

                if (vectorLockEntry.lockType != LockType.Shared)
                {
                    lockCtx.Unlock([vectorLockEntry]);
                    lockCtx.EndLockable();

                    throw new GarnetException("Held exclusive lock when adding to vector set, should never happen");
                }

                status = GarnetStatus.OK;
                return new(ref lockCtx, vectorLockEntry);
            }
        }

        /// <summary>
        /// Utility method that will read vector set index out, and acquire exclusive locks to allow it to be deleted.
        /// </summary>
        private ReadVectorLock ReadForDeleteVectorIndex(ref SpanByte key, ref RawStringInput input, scoped Span<byte> indexSpan, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == VectorManager.IndexSizeBytes, "Insufficient space for index");

            VectorManager.EnterStorageSessionContext(this);

            TxnKeyEntry vectorLockEntry = new();
            vectorLockEntry.isObject = false;
            vectorLockEntry.keyHash = lockableContext.GetKeyHash(key);
            vectorLockEntry.lockType = LockType.Exclusive;

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            ref var lockCtx = ref objectStoreLockableContext;
            lockCtx.BeginLockable();

            lockCtx.Lock([vectorLockEntry]);

            // Get the index
            try
            {
                status = Read_MainStore(ref key, ref input, ref indexConfig, ref basicContext);
            }
            catch
            {
                lockCtx.Unlock([vectorLockEntry]);
                lockCtx.EndLockable();

                throw;
            }

            if (status != GarnetStatus.OK)
            {
                // This can happen is something else successfully deleted before we acquired the lock

                lockCtx.Unlock([vectorLockEntry]);
                lockCtx.EndLockable();
                return default;
            }

            return new(ref lockCtx, vectorLockEntry);
        }
    }
}