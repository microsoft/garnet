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
    /// <summary>
    /// Methods managing locking around Vector Sets.
    /// 
    /// Locking is bespoke because of read-like nature of most Vector Set operations, and the re-entrancy implied by DiskANN callbacks.
    /// </summary>
    public sealed partial class VectorManager
    {
        /// <summary>
        /// Used to scope a lock (shared or exclusive) related to a Vector Set operation.
        /// 
        /// Disposing this releases the lock and exits the storage session context on the current thread.
        /// </summary>
        internal readonly ref struct VectorSetLock : IDisposable
        {
            private readonly ref readonly ReadOptimizedLock lockableCtx;
            private readonly ReadOptimizedLock.LockToken lockToken;

            internal VectorSetLock(ref readonly ReadOptimizedLock lockableCtx, ReadOptimizedLock.LockToken lockToken)
            {
                this.lockToken = lockToken;
                this.lockableCtx = ref lockableCtx;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                Debug.Assert(ActiveThreadSession != null, "Shouldn't exit context when not in one");
                ActiveThreadSession = null;

                // Clear the per-index read geometry so a subsequent operation on a different vector set
                // (possibly with different dimensions / M) does not inherit stale sizes.
                ActiveReadGeometry = default;

                if (Unsafe.IsNullRef(in lockableCtx))
                {
                    return;
                }

                lockableCtx.ReleaseLock(lockToken);
            }
        }

        private readonly ReadOptimizedLock vectorSetLocks;

        /// <summary>
        /// Returns true for indexes that were created via a previous instance of <see cref="VectorManager"/>.
        /// 
        /// Such indexes still have element data, but the index pointer to the DiskANN bits are invalid.
        /// </summary>
        internal bool NeedsRecreate(ReadOnlySpan<byte> indexConfig)
        {
            ReadIndex(indexConfig, out _, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

            return indexPtr == 0;
        }

        /// <summary>
        /// Utility method that will read an vector set index out but not create one.
        /// 
        /// It will however RECREATE one if needed.
        /// 
        /// Returns a disposable that prevents the index from being deleted while undisposed.
        /// </summary>
        internal VectorSetLock ReadVectorIndex(StorageSession storageSession, ReadOnlySpan<byte> key, ref StringInput input, scoped Span<byte> indexSpan, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");

            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");
            ActiveThreadSession = storageSession;
            try
            {
                var keyHash = storageSession.stringBasicContext.GetKeyHash((FixedSpanByteKey)key);

                var indexConfigOutput = StringOutput.FromPinnedSpan(indexSpan);

                var readCmd = input.header.cmd;

                var takeExclusiveLock = false;

                while (true)
                {
                    input.header.cmd = readCmd;
                    input.arg1 = 0;

                    ReadOptimizedLock.LockToken lockToken;
                    if (takeExclusiveLock)
                    {
                        vectorSetLocks.AcquireExclusiveLock(keyHash, out lockToken);

                        // If we pass through _again_, don't start with an exclusive lock
                        takeExclusiveLock = false;
                    }
                    else
                    {
                        vectorSetLocks.AcquireSharedLock(keyHash, out lockToken);
                    }

                    GarnetStatus readRes;
                    try
                    {
                        readRes = storageSession.Read_MainStore(key, ref input, ref indexConfigOutput, ref storageSession.stringBasicContext);
                        Debug.Assert(indexConfigOutput.SpanByteAndMemory.IsSpanByte, "Should never need to move index onto the heap");
                    }
                    catch
                    {
                        vectorSetLocks.ReleaseLock(lockToken);

                        throw;
                    }

                    bool needsRecreate;
                    if (readRes == GarnetStatus.OK)
                    {
                        needsRecreate = NeedsRecreate(indexConfigOutput.SpanByteAndMemory.ReadOnlySpan);
                    }
                    else
                    {
                        needsRecreate = false;
                    }

                    if (lockToken.IsExclusive && !needsRecreate)
                    {
                        // Raised to recreate but don't need it, lower to shared and retry
                        vectorSetLocks.ReleaseLock(lockToken);

                        continue;
                    }

                    if (needsRecreate)
                    {
                        // If we need to recreate the index, BUT we haven't finished drop from the last time
                        // we need to spin (without holding a lock) until that happens
                        //
                        // This should be rare, but having two active DiskANN indexes for the same logical Vector Set
                        // will break inserts quite badly - so it's not optional.
                        if (DropRequested(key))
                        {
                            vectorSetLocks.ReleaseLock(lockToken);
                            takeExclusiveLock = false;

                            WaitForDiskANNIndexDrop(key);
                            continue;
                        }

                        if (!lockToken.IsExclusive)
                        {
                            // Try to promote
                            if (!vectorSetLocks.TryPromoteSharedLock(keyHash, ref lockToken))
                            {
                                // Release the SHARED lock if we can't promote and try again - but this time DEMAND an exclusive lock
                                vectorSetLocks.ReleaseLock(lockToken);
                                takeExclusiveLock = true;

                                continue;
                            }
                        }

                        ReadIndex(indexSpan, out var indexContext, out var dims, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var distanceMetric, out _, out _);

                        input.arg1 = RecreateIndexArg;

                        nint newlyAllocatedIndex;
                        bool requestQuantization;
                        unsafe
                        {
                            newlyAllocatedIndex = Service.RecreateIndex(indexContext, dims, reduceDims, quantType, buildExplorationFactor, numLinks, distanceMetric, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr, ReadModifyWriteCallbackPtr, InlineFilterCallbackPtr, out requestQuantization);
                        }

                        input.header.cmd = RespCommand.VADD;
                        input.arg1 = RecreateIndexArg;

                        input.parseState.EnsureCapacity(12);

                        // Save off for recreation
                        input.parseState.SetArgument(10, PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref indexContext, 1)))); // Strictly we don't _need_ this, but it keeps everything else aligned nicely
                        input.parseState.SetArgument(11, PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<nint, byte>(MemoryMarshal.CreateSpan(ref newlyAllocatedIndex, 1))));

                        GarnetStatus writeRes;
                        try
                        {
                            try
                            {
                                ExceptionInjectionHelper.ResetAndWait(ExceptionInjectionType.VectorSet_Pause_Before_Recreate_Rmw);

                                writeRes = storageSession.RMW_MainStore(key, ref input, ref indexConfigOutput, ref storageSession.stringBasicContext);

                                if (writeRes != GarnetStatus.OK)
                                {
                                    // If we didn't write, drop index so we don't leak it
                                    Service.DropIndex(indexContext, newlyAllocatedIndex);
                                }
                            }
                            catch
                            {
                                // Drop to avoid leak on error
                                Service.DropIndex(indexContext, newlyAllocatedIndex);
                                throw;
                            }
                        }
                        catch
                        {
                            vectorSetLocks.ReleaseLock(lockToken);

                            throw;
                        }

                        if (writeRes == GarnetStatus.OK)
                        {
                            // Post recreate the index might already need quantization - if so, queue it up
                            if (requestQuantization)
                            {
                                _ = quantizationChannel.Writer.TryWrite(new(key.ToArray(), QuantizationStep.BuildQuantizationTable, 0));
                            }

                            // Try again so we don't hold an exclusive lock while performing a search
                            vectorSetLocks.ReleaseLock(lockToken);

                            continue;
                        }
                        else
                        {
                            status = writeRes;
                            vectorSetLocks.ReleaseLock(lockToken);

                            return default;
                        }
                    }
                    else if (readRes != GarnetStatus.OK)
                    {
                        status = readRes;
                        vectorSetLocks.ReleaseLock(lockToken);

                        return default;
                    }

                    status = GarnetStatus.OK;
                    return new(in vectorSetLocks, lockToken);
                }
            }
            catch
            {
                // If we exit without returning a lock, we'll leave ActiveThreadSession set, this clears it and rethrows
                //
                // In the normal exit case, disposing ReadVectorLock will clear ActiveThreadSession
                ActiveThreadSession = null;

                throw;
            }
        }

        /// <summary>
        /// Utility method that will read vector set index out, create one if it doesn't exist, or RECREATE one if needed.
        /// 
        /// Returns a disposable that prevents the index from being deleted while undisposed.
        /// </summary>
        internal VectorSetLock ReadOrCreateVectorIndex(
            StorageSession storageSession,
            ReadOnlySpan<byte> key,
            ref StringInput input,
            scoped Span<byte> indexSpan,
            out GarnetStatus status
        )
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");

            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");
            ActiveThreadSession = storageSession;
            try
            {
                var keyHash = storageSession.stringBasicContext.GetKeyHash((FixedSpanByteKey)key);

                var indexConfigOutput = StringOutput.FromPinnedSpan(indexSpan);

                var takeExclusiveLock = false;

                while (true)
                {
                    input.arg1 = 0;

                    ReadOptimizedLock.LockToken lockToken;
                    if (takeExclusiveLock)
                    {
                        vectorSetLocks.AcquireExclusiveLock(keyHash, out lockToken);

                        // If we pass through _again_, don't start with an exclusive lock
                        takeExclusiveLock = false;
                    }
                    else
                    {
                        vectorSetLocks.AcquireSharedLock(keyHash, out lockToken);
                    }

                    GarnetStatus readRes;
                    try
                    {
                        readRes = storageSession.Read_MainStore(key, ref input, ref indexConfigOutput, ref storageSession.stringBasicContext);
                        Debug.Assert(indexConfigOutput.SpanByteAndMemory.IsSpanByte, "Should never need to move index onto the heap");
                    }
                    catch
                    {
                        vectorSetLocks.ReleaseLock(lockToken);

                        throw;
                    }

                    bool needsRecreate;
                    if (readRes == GarnetStatus.OK)
                    {
                        needsRecreate = NeedsRecreate(indexConfigOutput.SpanByteAndMemory.ReadOnlySpan);
                    }
                    else
                    {
                        needsRecreate = false;
                    }

                    // Don't need the exclusive lock, lower to shared immediately
                    if (lockToken.IsExclusive && !(readRes == GarnetStatus.NOTFOUND || needsRecreate))
                    {
                        vectorSetLocks.ReleaseLock(lockToken);
                        continue;
                    }

                    if (readRes == GarnetStatus.NOTFOUND || needsRecreate)
                    {
                        if (!lockToken.IsExclusive)
                        {
                            if (!vectorSetLocks.TryPromoteSharedLock(keyHash, ref lockToken))
                            {
                                // Release the SHARED lock if we can't promote and try again but DEMAND exclusive this time
                                vectorSetLocks.ReleaseLock(lockToken);
                                takeExclusiveLock = true;

                                continue;
                            }
                        }

                        ulong indexContext;
                        nint newlyAllocatedIndex;
                        bool requestQuantization;
                        if (needsRecreate)
                        {
                            // If we need to recreate the index, BUT we haven't finished drop from the last time
                            // we need to spin (without holding a lock) until that happens
                            //
                            // This should be rare, but having two active DiskANN indexes for the same logical Vector Set
                            // will break inserts quite badly - so it's not optional.
                            if (DropRequested(key))
                            {
                                vectorSetLocks.ReleaseLock(lockToken);
                                takeExclusiveLock = false;

                                WaitForDiskANNIndexDrop(key);
                                continue;
                            }

                            ReadIndex(indexSpan, out indexContext, out var dims, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var distanceMetric, out _, out _);

                            input.arg1 = RecreateIndexArg;

                            unsafe
                            {
                                newlyAllocatedIndex = Service.RecreateIndex(indexContext, dims, reduceDims, quantType, buildExplorationFactor, numLinks, distanceMetric, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr, ReadModifyWriteCallbackPtr, InlineFilterCallbackPtr, out requestQuantization);
                            }

                            input.parseState.EnsureCapacity(12);

                            // Save off for recreation
                            input.parseState.SetArgument(10, PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref indexContext, 1)))); // Strictly we don't _need_ this, but it keeps everything else aligned nicely
                            input.parseState.SetArgument(11, PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<nint, byte>(MemoryMarshal.CreateSpan(ref newlyAllocatedIndex, 1))));
                        }
                        else
                        {
                            // Create a new index, grab a new context
                            input.arg1 = CreateIndexArg;

                            // We must associate the index with a hash slot at creation time to enable future migrations
                            var slot = HashSlotUtils.HashSlot(key);

                            indexContext = NextVectorSetContext(slot);

                            var dims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(0).Span);
                            var reduceDims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(1).Span);
                            // ValueType is here, skipping during index creation
                            // Values is here, skipping during index creation
                            // Element is here, skipping during index creation
                            var quantizer = MemoryMarshal.Read<VectorQuantType>(input.parseState.GetArgSliceByRef(5).Span);
                            var buildExplorationFactor = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(6).Span);
                            // Attributes is here, skipping during index creation
                            var numLinks = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(8).Span);
                            var distanceMetric = MemoryMarshal.Read<VectorDistanceMetricType>(input.parseState.GetArgSliceByRef(9).Span);

                            unsafe
                            {
                                newlyAllocatedIndex = Service.CreateIndex(indexContext, dims, reduceDims, quantizer, buildExplorationFactor, numLinks, distanceMetric, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr, ReadModifyWriteCallbackPtr, InlineFilterCallbackPtr, out requestQuantization);
                            }

                            input.parseState.EnsureCapacity(12);

                            // Save off for insertion
                            input.parseState.SetArgument(10, PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref indexContext, 1))));
                            input.parseState.SetArgument(11, PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<nint, byte>(MemoryMarshal.CreateSpan(ref newlyAllocatedIndex, 1))));
                        }

                        GarnetStatus writeRes;
                        try
                        {
                            try
                            {
                                writeRes = storageSession.RMW_MainStore(key, ref input, ref indexConfigOutput, ref storageSession.stringBasicContext);

                                if (writeRes != GarnetStatus.OK)
                                {
                                    // Insertion failed, drop index
                                    Service.DropIndex(indexContext, newlyAllocatedIndex);
                                }
                            }
                            catch
                            {
                                if (newlyAllocatedIndex != 0)
                                {
                                    // Drop to avoid a leak on error
                                    Service.DropIndex(indexContext, newlyAllocatedIndex);
                                }

                                throw;
                            }

                            if (!needsRecreate)
                            {
                                UpdateContextMetadata(ref storageSession.vectorBasicContext);
                            }
                        }
                        catch
                        {
                            vectorSetLocks.ReleaseLock(lockToken);

                            throw;
                        }

                        if (writeRes == GarnetStatus.OK)
                        {
                            // Post (re)create the index might already need quantization - if so, queue it up
                            if (requestQuantization)
                            {
                                _ = quantizationChannel.Writer.TryWrite(new(key.ToArray(), QuantizationStep.BuildQuantizationTable, 0));
                            }

                            // Try again so we don't hold an exclusive lock while adding a vector (which might be time consuming)
                            vectorSetLocks.ReleaseLock(lockToken);
                            continue;
                        }
                        else
                        {
                            status = writeRes;
                            vectorSetLocks.ReleaseLock(lockToken);

                            return default;
                        }
                    }
                    else if (readRes != GarnetStatus.OK)
                    {
                        vectorSetLocks.ReleaseLock(lockToken);

                        status = readRes;
                        return default;
                    }

                    status = GarnetStatus.OK;
                    return new(in vectorSetLocks, lockToken);
                }
            }
            catch
            {
                // If we exit without returning a lock, we'll leave ActiveThreadSession set, this clears it and rethrows
                //
                // In the normal exit case, disposing ReadVectorLock will clear ActiveThreadSession
                ActiveThreadSession = null;

                throw;
            }
        }

        /// <summary>
        /// Acquire exclusive lock over a given key.
        /// </summary>
        private VectorSetLock AcquireExclusiveLocks(StorageSession storageSession, ReadOnlySpan<byte> key)
        {
            var keyHash = storageSession.stringTransactionalContext.GetKeyHash((FixedSpanByteKey)key);

            vectorSetLocks.AcquireExclusiveLock(keyHash, out var exclusiveLockToken);

            return new(in vectorSetLocks, exclusiveLockToken);
        }

        /// <summary>
        /// Utility method that will read vector set index out, and acquire exclusive locks to allow it to be deleted.
        /// </summary>
        internal VectorSetLock ReadForDeleteVectorIndex(StorageSession storageSession, ReadOnlySpan<byte> key, ref StringInput input, scoped Span<byte> indexSpan, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");

            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");
            ActiveThreadSession = storageSession;

            var indexConfigOutput = StringOutput.FromPinnedSpan(indexSpan);

            // Get the index
            var acquiredLock = AcquireExclusiveLocks(storageSession, key);
            try
            {
                status = storageSession.Read_MainStore(key, ref input, ref indexConfigOutput, ref storageSession.stringBasicContext);
                Debug.Assert(indexConfigOutput.SpanByteAndMemory.IsSpanByte, "Should never need to move index onto the heap");
            }
            catch
            {
                acquiredLock.Dispose();

                throw;
            }

            return acquiredLock;
        }
    }
}