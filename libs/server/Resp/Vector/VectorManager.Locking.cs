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
        /// Used to scope a shared lock related to a Vector Set operation.
        /// 
        /// Disposing this releases the lock and exits the storage session context on the current thread.
        /// </summary>
        internal readonly ref struct ReadVectorLock : IDisposable
        {
            private readonly ref readonly ReadOptimizedLock lockableCtx;
            private readonly int lockToken;

            internal ReadVectorLock(ref readonly ReadOptimizedLock lockableCtx, int lockToken)
            {
                this.lockToken = lockToken;
                this.lockableCtx = ref lockableCtx;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                Debug.Assert(ActiveThreadSession != null, "Shouldn't exit context when not in one");
                ActiveThreadSession = null;

                if (Unsafe.IsNullRef(in lockableCtx))
                {
                    return;
                }

                lockableCtx.ReleaseSharedLock(lockToken);
            }
        }

        /// <summary>
        /// Used to scope exclusive locks  to exclusive Vector Set operation (delete, migrate, etc.).
        /// 
        /// Disposing this releases the lock and exits the storage session context on the current thread.
        /// </summary>
        internal readonly ref struct ExclusiveVectorLock : IDisposable
        {
            private readonly ref readonly ReadOptimizedLock lockableCtx;
            private readonly int lockToken;

            internal ExclusiveVectorLock(ref readonly ReadOptimizedLock lockableCtx, int lockToken)
            {
                this.lockToken = lockToken;
                this.lockableCtx = ref lockableCtx;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                Debug.Assert(ActiveThreadSession != null, "Shouldn't exit context when not in one");
                ActiveThreadSession = null;

                if (Unsafe.IsNullRef(in lockableCtx))
                {
                    return;
                }

                lockableCtx.ReleaseExclusiveLock(lockToken);
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
            ReadIndex(indexConfig, out _, out _, out _, out _, out _, out _, out _, out var indexProcessInstanceId);

            return indexProcessInstanceId != processInstanceId;
        }

        /// <summary>
        /// Utility method that will read an vector set index out but not create one.
        /// 
        /// It will however RECREATE one if needed.
        /// 
        /// Returns a disposable that prevents the index from being deleted while undisposed.
        /// </summary>
        internal ReadVectorLock ReadVectorIndex(StorageSession storageSession, ref SpanByte key, ref RawStringInput input, scoped Span<byte> indexSpan, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");

            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");
            ActiveThreadSession = storageSession;

            var keyHash = storageSession.basicContext.GetKeyHash(ref key);

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            var readCmd = input.header.cmd;

            while (true)
            {
                input.header.cmd = readCmd;
                input.arg1 = 0;

                vectorSetLocks.AcquireSharedLock(keyHash, out var sharedLockToken);

                GarnetStatus readRes;
                try
                {
                    readRes = storageSession.Read_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
                    Debug.Assert(indexConfig.IsSpanByte, "Should never need to move index onto the heap");
                }
                catch
                {
                    vectorSetLocks.ReleaseSharedLock(sharedLockToken);

                    throw;
                }

                var needsRecreate = readRes == GarnetStatus.OK && NeedsRecreate(indexConfig.AsReadOnlySpan());

                if (needsRecreate)
                {
                    if (!vectorSetLocks.TryPromoteSharedLock(keyHash, sharedLockToken, out var exclusiveLockToken))
                    {
                        // Release the SHARED lock if we can't promote and try again
                        vectorSetLocks.ReleaseSharedLock(sharedLockToken);

                        continue;
                    }

                    ReadIndex(indexSpan, out var indexContext, out var dims, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out _, out _);

                    input.arg1 = RecreateIndexArg;

                    nint newlyAllocatedIndex;
                    unsafe
                    {
                        newlyAllocatedIndex = Service.RecreateIndex(indexContext, dims, reduceDims, quantType, buildExplorationFactor, numLinks, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr, ReadModifyWriteCallbackPtr);
                    }

                    input.header.cmd = RespCommand.VADD;
                    input.arg1 = RecreateIndexArg;

                    input.parseState.EnsureCapacity(11);

                    // Save off for recreation
                    input.parseState.SetArgument(9, ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref indexContext, 1)))); // Strictly we don't _need_ this, but it keeps everything else aligned nicely
                    input.parseState.SetArgument(10, ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<nint, byte>(MemoryMarshal.CreateSpan(ref newlyAllocatedIndex, 1))));

                    GarnetStatus writeRes;
                    try
                    {
                        try
                        {
                            writeRes = storageSession.RMW_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);

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
                        vectorSetLocks.ReleaseExclusiveLock(exclusiveLockToken);

                        throw;
                    }

                    if (writeRes == GarnetStatus.OK)
                    {
                        // Try again so we don't hold an exclusive lock while performing a search
                        vectorSetLocks.ReleaseExclusiveLock(exclusiveLockToken);
                        continue;
                    }
                    else
                    {
                        status = writeRes;
                        vectorSetLocks.ReleaseExclusiveLock(exclusiveLockToken);

                        return default;
                    }
                }
                else if (readRes != GarnetStatus.OK)
                {
                    status = readRes;
                    vectorSetLocks.ReleaseSharedLock(sharedLockToken);

                    return default;
                }

                status = GarnetStatus.OK;
                return new(in vectorSetLocks, sharedLockToken);
            }
        }

        /// <summary>
        /// Utility method that will read vector set index out, create one if it doesn't exist, or RECREATE one if needed.
        /// 
        /// Returns a disposable that prevents the index from being deleted while undisposed.
        /// </summary>
        internal ReadVectorLock ReadOrCreateVectorIndex(
            StorageSession storageSession,
            ref SpanByte key,
            ref RawStringInput input,
            scoped Span<byte> indexSpan,
            out GarnetStatus status
        )
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");

            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");
            ActiveThreadSession = storageSession;

            var keyHash = storageSession.basicContext.GetKeyHash(ref key);

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            while (true)
            {
                input.arg1 = 0;

                vectorSetLocks.AcquireSharedLock(keyHash, out var sharedLockToken);

                GarnetStatus readRes;
                try
                {
                    readRes = storageSession.Read_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
                    Debug.Assert(indexConfig.IsSpanByte, "Should never need to move index onto the heap");
                }
                catch
                {
                    vectorSetLocks.ReleaseSharedLock(sharedLockToken);

                    throw;
                }

                var needsRecreate = readRes == GarnetStatus.OK && storageSession.vectorManager.NeedsRecreate(indexSpan);
                if (readRes == GarnetStatus.NOTFOUND || needsRecreate)
                {
                    if (!vectorSetLocks.TryPromoteSharedLock(keyHash, sharedLockToken, out var exclusiveLockToken))
                    {
                        // Release the SHARED lock if we can't promote and try again
                        vectorSetLocks.ReleaseSharedLock(sharedLockToken);

                        continue;
                    }

                    ulong indexContext;
                    nint newlyAllocatedIndex;
                    if (needsRecreate)
                    {
                        ReadIndex(indexSpan, out indexContext, out var dims, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out _, out _);

                        input.arg1 = RecreateIndexArg;

                        unsafe
                        {
                            newlyAllocatedIndex = Service.RecreateIndex(indexContext, dims, reduceDims, quantType, buildExplorationFactor, numLinks, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr, ReadModifyWriteCallbackPtr);
                        }

                        input.parseState.EnsureCapacity(11);

                        // Save off for recreation
                        input.parseState.SetArgument(9, ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref indexContext, 1)))); // Strictly we don't _need_ this, but it keeps everything else aligned nicely
                        input.parseState.SetArgument(10, ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<nint, byte>(MemoryMarshal.CreateSpan(ref newlyAllocatedIndex, 1))));
                    }
                    else
                    {
                        // Create a new index, grab a new context

                        // We must associate the index with a hash slot at creation time to enable future migrations
                        // TODO: RENAME and friends need to also update this data
                        var slot = HashSlotUtils.HashSlot(ref key);

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

                        unsafe
                        {
                            newlyAllocatedIndex = Service.CreateIndex(indexContext, dims, reduceDims, quantizer, buildExplorationFactor, numLinks, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr, ReadModifyWriteCallbackPtr);
                        }

                        input.parseState.EnsureCapacity(11);

                        // Save off for insertion
                        input.parseState.SetArgument(9, ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref indexContext, 1))));
                        input.parseState.SetArgument(10, ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<nint, byte>(MemoryMarshal.CreateSpan(ref newlyAllocatedIndex, 1))));
                    }

                    GarnetStatus writeRes;
                    try
                    {
                        try
                        {
                            writeRes = storageSession.RMW_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);

                            if (writeRes != GarnetStatus.OK)
                            {
                                // Insertion failed, drop index
                                Service.DropIndex(indexContext, newlyAllocatedIndex);

                                // If the failure was for a brand new index, free up the context too
                                if (!needsRecreate)
                                {
                                    CleanupDroppedIndex(ref ActiveThreadSession.vectorContext, indexContext);
                                }
                            }
                        }
                        catch
                        {
                            if (newlyAllocatedIndex != 0)
                            {
                                // Drop to avoid a leak on error
                                Service.DropIndex(indexContext, newlyAllocatedIndex);

                                // If the failure was for a brand new index, free up the context too
                                if (!needsRecreate)
                                {
                                    CleanupDroppedIndex(ref ActiveThreadSession.vectorContext, indexContext);
                                }
                            }

                            throw;
                        }

                        if (!needsRecreate)
                        {
                            UpdateContextMetadata(ref storageSession.vectorContext);
                        }
                    }
                    catch
                    {
                        vectorSetLocks.ReleaseExclusiveLock(exclusiveLockToken);

                        throw;
                    }

                    if (writeRes == GarnetStatus.OK)
                    {
                        // Try again so we don't hold an exclusive lock while adding a vector (which might be time consuming)
                        vectorSetLocks.ReleaseExclusiveLock(exclusiveLockToken);
                        continue;
                    }
                    else
                    {
                        status = writeRes;
                        vectorSetLocks.ReleaseExclusiveLock(exclusiveLockToken);

                        return default;
                    }
                }
                else if (readRes != GarnetStatus.OK)
                {
                    vectorSetLocks.ReleaseSharedLock(sharedLockToken);

                    status = readRes;
                    return default;
                }

                status = GarnetStatus.OK;
                return new(in vectorSetLocks, sharedLockToken);
            }
        }

        /// <summary>
        /// Acquire exclusive lock over a given key.
        /// </summary>
        private ExclusiveVectorLock AcquireExclusiveLocks(StorageSession storageSession, ref SpanByte key)
        {
            var keyHash = storageSession.lockableContext.GetKeyHash(key);

            vectorSetLocks.AcquireExclusiveLock(keyHash, out var exclusiveLockToken);

            return new(in vectorSetLocks, exclusiveLockToken);
        }

        /// <summary>
        /// Utility method that will read vector set index out, and acquire exclusive locks to allow it to be deleted.
        /// </summary>
        internal ExclusiveVectorLock ReadForDeleteVectorIndex(StorageSession storageSession, ref SpanByte key, ref RawStringInput input, scoped Span<byte> indexSpan, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");

            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");
            ActiveThreadSession = storageSession;

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            // Get the index
            var acquiredLock = AcquireExclusiveLocks(storageSession, ref key);
            try
            {
                status = storageSession.Read_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
            }
            catch
            {
                acquiredLock.Dispose();

                throw;
            }

            if (status != GarnetStatus.OK)
            {
                // This can happen if something else successfully deleted before we acquired the lock

                acquiredLock.Dispose();
                return default;
            }

            return acquiredLock;
        }
    }
}