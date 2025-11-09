// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Methods managing locking around Vector Sets.
    /// 
    /// Locking is bespoke because of read-like nature of most Vector Set operations, and the re-entrancy implied by DiskANN callbacks.
    /// </summary>
    public sealed partial class VectorManager
    {
        // TODO: Object store is going away, need to move this to some other locking scheme

        /// <summary>
        /// Used to scope a shared lock and context related to a Vector Set operation.
        /// 
        /// Disposing this ends the lockable context, releases the lock, and exits the storage session context on the current thread.
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
                Debug.Assert(ActiveThreadSession != null, "Shouldn't exit context when not in one");
                ActiveThreadSession = null;

                if (Unsafe.IsNullRef(ref lockableCtx))
                {
                    return;
                }

                lockableCtx.Unlock([entry]);
                lockableCtx.EndLockable();
            }
        }

        /// <summary>
        /// Used to scope exclusive locks and a context related to exclusive Vector Set operation (delete, migrate, etc.).
        /// 
        /// Disposing this ends the lockable context, releases the locks, and exits the storage session context on the current thread.
        /// </summary>
        internal readonly ref struct ExclusiveVectorLock : IDisposable
        {
            private readonly ref LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> lockableCtx;
            private readonly ReadOnlySpan<TxnKeyEntry> entries;

            internal ExclusiveVectorLock(ref LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> lockableCtx, ReadOnlySpan<TxnKeyEntry> entries)
            {
                this.entries = entries;
                this.lockableCtx = ref lockableCtx;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                Debug.Assert(ActiveThreadSession != null, "Shouldn't exit context when not in one");
                ActiveThreadSession = null;

                if (Unsafe.IsNullRef(ref lockableCtx))
                {
                    return;
                }

                lockableCtx.Unlock(entries);
                lockableCtx.EndLockable();
            }
        }

        private readonly int readLockShardCount;
        private readonly long readLockShardMask;

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

            PrepareReadLockHash(storageSession, ref key, out var keyHash, out var readLockHash);

            Span<TxnKeyEntry> sharedLocks = stackalloc TxnKeyEntry[1];
            scoped Span<TxnKeyEntry> exclusiveLocks = default;

            ref var readLockEntry = ref sharedLocks[0];
            readLockEntry.isObject = false;
            readLockEntry.keyHash = readLockHash;
            readLockEntry.lockType = LockType.Shared;

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            ref var lockCtx = ref storageSession.objectStoreLockableContext;
            lockCtx.BeginLockable();

            var readCmd = input.header.cmd;

            while (true)
            {
                input.header.cmd = readCmd;
                input.arg1 = 0;

                lockCtx.Lock([readLockEntry]);

                GarnetStatus readRes;
                try
                {
                    readRes = storageSession.Read_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
                    Debug.Assert(indexConfig.IsSpanByte, "Should never need to move index onto the heap");
                }
                catch
                {
                    lockCtx.Unlock([readLockEntry]);
                    lockCtx.EndLockable();

                    throw;
                }

                var needsRecreate = readRes == GarnetStatus.OK && NeedsRecreate(indexConfig.AsReadOnlySpan());

                if (needsRecreate)
                {
                    if (exclusiveLocks.IsEmpty)
                    {
                        exclusiveLocks = stackalloc TxnKeyEntry[readLockShardCount];
                    }

                    if (!TryAcquireExclusiveLocks(storageSession, exclusiveLocks, keyHash, readLockHash))
                    {
                        // All locks will have been released by here
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
                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        lockCtx.EndLockable();

                        throw;
                    }

                    if (writeRes == GarnetStatus.OK)
                    {
                        // Try again so we don't hold an exclusive lock while performing a search
                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        continue;
                    }
                    else
                    {
                        status = writeRes;
                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        lockCtx.EndLockable();

                        return default;
                    }
                }
                else if (readRes != GarnetStatus.OK)
                {
                    status = readRes;
                    lockCtx.Unlock<TxnKeyEntry>(sharedLocks);
                    lockCtx.EndLockable();

                    return default;
                }

                status = GarnetStatus.OK;
                return new(ref lockCtx, readLockEntry);
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

            PrepareReadLockHash(storageSession, ref key, out var keyHash, out var readLockHash);

            Span<TxnKeyEntry> sharedLocks = stackalloc TxnKeyEntry[1];
            scoped Span<TxnKeyEntry> exclusiveLocks = default;

            ref var readLockEntry = ref sharedLocks[0];
            readLockEntry.isObject = false;
            readLockEntry.keyHash = readLockHash;
            readLockEntry.lockType = LockType.Shared;

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            ref var lockCtx = ref storageSession.objectStoreLockableContext;
            lockCtx.BeginLockable();

            while (true)
            {
                input.arg1 = 0;

                lockCtx.Lock<TxnKeyEntry>(sharedLocks);

                GarnetStatus readRes;
                try
                {
                    readRes = storageSession.Read_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
                    Debug.Assert(indexConfig.IsSpanByte, "Should never need to move index onto the heap");
                }
                catch
                {
                    lockCtx.Unlock<TxnKeyEntry>(sharedLocks);
                    lockCtx.EndLockable();

                    throw;
                }

                var needsRecreate = readRes == GarnetStatus.OK && storageSession.vectorManager.NeedsRecreate(indexSpan);
                if (readRes == GarnetStatus.NOTFOUND || needsRecreate)
                {
                    if (exclusiveLocks.IsEmpty)
                    {
                        exclusiveLocks = stackalloc TxnKeyEntry[readLockShardCount];
                    }

                    if (!TryAcquireExclusiveLocks(storageSession, exclusiveLocks, keyHash, readLockHash))
                    {
                        // All locks will have been released by here
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
                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        lockCtx.EndLockable();

                        throw;
                    }

                    if (writeRes == GarnetStatus.OK)
                    {
                        // Try again so we don't hold an exclusive lock while adding a vector (which might be time consuming)
                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        continue;
                    }
                    else
                    {
                        status = writeRes;

                        lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                        lockCtx.EndLockable();

                        return default;
                    }
                }
                else if (readRes != GarnetStatus.OK)
                {
                    lockCtx.Unlock<TxnKeyEntry>(sharedLocks);
                    lockCtx.EndLockable();

                    status = readRes;
                    return default;
                }

                status = GarnetStatus.OK;
                return new(ref lockCtx, readLockEntry);
            }
        }

        /// <summary>
        /// Acquire exclusive lock over a given key.
        /// </summary>
        private ExclusiveVectorLock AcquireExclusiveLocks(StorageSession storageSession, ref SpanByte key, Span<TxnKeyEntry> exclusiveLocks)
        {
            Debug.Assert(exclusiveLocks.Length == readLockShardCount, "Incorrect number of locks");

            var keyHash = storageSession.lockableContext.GetKeyHash(key);

            for (var i = 0; i < exclusiveLocks.Length; i++)
            {
                exclusiveLocks[i].isObject = false;
                exclusiveLocks[i].lockType = LockType.Exclusive;
                exclusiveLocks[i].keyHash = (keyHash & ~readLockShardMask) | (long)i;
            }

            ref var lockCtx = ref storageSession.objectStoreLockableContext;
            lockCtx.BeginLockable();

            lockCtx.Lock<TxnKeyEntry>(exclusiveLocks);

            return new(ref lockCtx, exclusiveLocks);
        }

        /// <summary>
        /// Utility method that will read vector set index out, and acquire exclusive locks to allow it to be deleted.
        /// </summary>
        internal ExclusiveVectorLock ReadForDeleteVectorIndex(StorageSession storageSession, ref SpanByte key, ref RawStringInput input, scoped Span<byte> indexSpan, Span<TxnKeyEntry> exclusiveLocks, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");
            Debug.Assert(exclusiveLocks.Length == readLockShardCount, "Insufficient space for exclusive locks");

            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");
            ActiveThreadSession = storageSession;

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            // Get the index
            var acquiredLock = AcquireExclusiveLocks(storageSession, ref key, exclusiveLocks);
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
                // This can happen is something else successfully deleted before we acquired the lock

                acquiredLock.Dispose();
                return default;
            }

            return acquiredLock;
        }

        /// <summary>
        /// Prepare a hash based on the given key and the currently active processor.
        /// 
        /// This can only be used for read locking, as it will block exclusive lock acquisition but not other readers.
        /// 
        /// Sharded for performance reasons.
        /// </summary>
        private void PrepareReadLockHash(StorageSession storageSession, ref SpanByte key, out long keyHash, out long readLockHash)
        {
            var id = Thread.GetCurrentProcessorId() & readLockShardMask;

            keyHash = storageSession.basicContext.GetKeyHash(ref key);
            readLockHash = (keyHash & ~readLockShardMask) | id;
        }

        /// <summary>
        /// Used to upgrade from one SHARED lock to all EXCLUSIVE locks.
        /// 
        /// Can fail, unlike <see cref="AcquireExclusiveLocks(StorageSession, ref SpanByte, Span{TxnKeyEntry})"/>.
        /// </summary>
        private bool TryAcquireExclusiveLocks(StorageSession storageSession, Span<TxnKeyEntry> exclusiveLocks, long keyHash, long readLockHash)
        {
            Debug.Assert(exclusiveLocks.Length == readLockShardCount, "Insufficient space for exclusive locks");

            // When we start, we still hold a SHARED lock on readLockHash

            for (var i = 0; i < exclusiveLocks.Length; i++)
            {
                exclusiveLocks[i].isObject = false;
                exclusiveLocks[i].lockType = LockType.Shared;
                exclusiveLocks[i].keyHash = (keyHash & ~readLockShardMask) | (long)i;
            }

            AssertSorted(exclusiveLocks);

            ref var lockCtx = ref storageSession.objectStoreLockableContext;

            TxnKeyEntry toUnlock = default;
            toUnlock.keyHash = readLockHash;
            toUnlock.isObject = false;
            toUnlock.lockType = LockType.Shared;

            if (!lockCtx.TryLock<TxnKeyEntry>(exclusiveLocks))
            {
                // We don't hold any new locks, but still have the old SHARED lock

                lockCtx.Unlock([toUnlock]);
                return false;
            }

            // Drop down to just 1 shared lock per id
            lockCtx.Unlock([toUnlock]);

            // Attempt to promote
            for (var i = 0; i < exclusiveLocks.Length; i++)
            {
                if (!lockCtx.TryPromoteLock(exclusiveLocks[i]))
                {
                    lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                    return false;
                }

                exclusiveLocks[i].lockType = LockType.Exclusive;
            }

            return true;

            [Conditional("DEBUG")]
            static void AssertSorted(ReadOnlySpan<TxnKeyEntry> locks)
            {
                for (var i = 1; i < locks.Length; i++)
                {
                    Debug.Assert(locks[i - 1].keyHash <= locks[i].keyHash, "Locks should be naturally sorted, but weren't");
                }
            }
        }
    }
}
