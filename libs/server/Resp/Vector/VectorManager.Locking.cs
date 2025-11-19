// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
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
        /// Holds a set of RW-esque locks for Vector Sets.
        /// 
        /// These are acquired and released as needed to prevent concurrent creation/deletion operations, or deletion concurrent with read operations.
        /// 
        /// These are outside of Tsavorite for correctness reasons.
        /// </summary>
        /// <remarks>
        /// This is a counter based r/w lock scheme, with a bit of biasing for cache line awareness.
        /// 
        /// Each "key" acquires locks based on its hash.
        /// Each hash is mapped to a range of indexes, each range is lockShardCount in length.
        /// When acquiring a shared lock, we take one index out of the keys range and acquire a read lock.
        ///   This will block exclusive locks, but not impact other readers.
        /// When acquiring an exclusive lock, we acquire write locks for all indexes in the key's range IN INCREASING _LOGICAL_ ORDER.
        ///   The order is necessary to avoid deadlocks.
        ///   By ensuring all exclusive locks walk "up" we guarantee no two exclusive lock acquisitions end up waiting for each other.
        /// 
        /// Locks themselves are just ints, where a negative value indicates an exclusive lock and a positive value is the number of active readers.
        /// Read locks are acquired optimistically, so actual lock values will fluctate above int.MinValue when an exclusive lock is held.
        /// 
        /// The last set of optimizations is around cache lines coherency:
        ///   We assume cache lines of 64-bytes (the x86 default, which is also true for some [but not all] ARM processors)
        ///   We access arrays via reference, to avoid thrashing cache lines due to length checks
        ///   Each shard is placed, in so much as is possible, into a different cache line rather than grouping a hash's counts physically near each other
        ///     This will tend to allow a core to retain ownership of the same cache lines even as it moves between different hashes
        /// </remarks>
        internal struct VectorSetLockContext
        {
            // This is true for all x86-derived processors and about 1/2 true for ARM-derived processors
            internal const int CacheLineSizeBytes = 64;

            // Beyond 4K bytes per core we're well past "this is worth the tradeoff", so cut off then
            internal const int MaxPerCoreContexts = 1_024;

            private readonly int[] lockCounts;
            private readonly int lockShardCount;
            private readonly int lockShardMask;
            private readonly int perCoreCounts;
            private readonly ulong perCoreCountsFastMod;
            private readonly byte perCoreCountsMultShift;

            internal VectorSetLockContext(int estimatedSimultaneousActiveVectorSets)
            {
                Debug.Assert(estimatedSimultaneousActiveVectorSets > 0);

                // ~1 per core
                lockShardCount = (int)BitOperations.RoundUpToPowerOf2((uint)Environment.ProcessorCount);
                lockShardMask = lockShardCount - 1;

                // Use estimatedSimultaneousActiveVectorSets to determine number of shards per lock.
                // 
                // We scale up to a whole multiple of CacheLineSizeBytes to reduce cache line thrashing.
                //
                // We scale to a power of 2 to avoid divisions (and some multiplies) in index calculation.
                perCoreCounts = estimatedSimultaneousActiveVectorSets;
                if (perCoreCounts % (CacheLineSizeBytes / sizeof(int)) != 0)
                {
                    perCoreCounts += (CacheLineSizeBytes / sizeof(int)) - (perCoreCounts % (CacheLineSizeBytes / sizeof(int)));
                }
                Debug.Assert(perCoreCounts % (CacheLineSizeBytes / sizeof(int)) == 0, "Each core should be whole cache lines of data");

                perCoreCounts = (int)BitOperations.RoundUpToPowerOf2((uint)perCoreCounts);

                // Put an upper bound of ~1 page worth of locks per core (which is still quite high).
                //
                // For the largest realistic machines out there (384 cores) this will put us at around ~2M of lock data, max.
                if (perCoreCounts is <= 0 or > MaxPerCoreContexts)
                {
                    perCoreCounts = MaxPerCoreContexts;
                }

                // Pre-calculate an alternative to %, as that division will be in the hot path
                perCoreCountsFastMod = (ulong.MaxValue / (uint)perCoreCounts) + 1;

                // Avoid two multiplies in the hot path
                perCoreCountsMultShift = (byte)BitOperations.Log2((uint)perCoreCounts);

                var size = lockShardCount * perCoreCounts;

                lockCounts = new int[size];
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal readonly int CalculateIndex(int hash, int currentProcessorHint)
            {
                // Hint might be out of range, so force it into the space we expect
                var currentProcessor = currentProcessorHint & lockShardMask;

                var startOfCoreCounts = currentProcessor << perCoreCountsMultShift;

                // Avoid doing a division in the hot path
                // Based on: https://github.com/dotnet/runtime/blob/3a95842304008b9ca84c14b4bec9ec99ed5802db/src/libraries/System.Private.CoreLib/src/System/Collections/HashHelpers.cs#L99
                var hashOffset = (uint)(((((perCoreCountsFastMod * (uint)hash) >> 32) + 1) << perCoreCountsMultShift) >> 32);

                Debug.Assert(hashOffset == ((uint)hash % perCoreCounts), "Replacing mod with multiplies failed");

                var ix = (int)(startOfCoreCounts + hashOffset);

                Debug.Assert(ix >= 0 && ix < lockCounts.Length, "About to do something out of bounds");

                return ix;
            }

            /// <summary>
            /// Attempt to acquire a shared lock for the given hash.
            /// 
            /// Will block exclusive locks until released.
            /// </summary>
            internal readonly bool TryAcquireSharedLock(int hash, out int lockToken)
            {
                var ix = CalculateIndex(hash, Thread.GetCurrentProcessorId());

                ref var acquireRef = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(lockCounts), ix);

                var res = Interlocked.Increment(ref acquireRef);
                if (res < 0)
                {
                    // Exclusively locked
                    _ = Interlocked.Decrement(ref acquireRef);
                    Unsafe.SkipInit(out lockToken);
                    return false;
                }

                lockToken = ix;
                return true;
            }

            /// <summary>
            /// Acquire a shared lock for the given hash, blocking until that succeeds.
            /// 
            /// Will block exclusive locks until released.
            /// </summary>
            internal readonly void AcquireSharedLock(int hash, out int lockToken)
            {
                var ix = CalculateIndex(hash, Thread.GetCurrentProcessorId());

                ref var acquireRef = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(lockCounts), ix);

                while (true)
                {
                    var res = Interlocked.Increment(ref acquireRef);
                    if (res < 0)
                    {
                        // Exclusively locked
                        _ = Interlocked.Decrement(ref acquireRef);

                        // Spin until we can grab this one
                        _ = Thread.Yield();
                    }
                    else
                    {
                        lockToken = ix;
                        return;
                    }
                }
            }

            /// <summary>
            /// Release a lock previously acquired with <see cref="TryAcquireSharedLock(int, out int)"/> or <see cref="AcquireSharedLock(int, out int)"/>.
            /// </summary>
            internal readonly void ReleaseSharedLock(int lockToken)
            {
                Debug.Assert(lockToken >= 0 && lockToken < lockCounts.Length, "Invalid lock token");

                ref var releaseRef = ref Unsafe.Add(ref MemoryMarshal.GetArrayDataReference(lockCounts), lockToken);

                _ = Interlocked.Decrement(ref releaseRef);
            }

            /// <summary>
            /// Attempt to acquire an exclusive lock for the given hash.
            /// 
            /// Will block all other locks until released.
            /// </summary>
            internal readonly bool TryAcquireExclusiveLock(int hash, out int lockToken)
            {
                ref var countRef = ref MemoryMarshal.GetArrayDataReference(lockCounts);

                for (var i = 0; i < lockShardCount; i++)
                {
                    var acquireIx = CalculateIndex(hash, i);
                    ref var acquireRef = ref Unsafe.Add(ref countRef, acquireIx);

                    if (Interlocked.CompareExchange(ref acquireRef, int.MinValue, 0) != 0)
                    {
                        // Failed, release previously acquired
                        for (var j = 0; j < i; j++)
                        {
                            var releaseIx = CalculateIndex(hash, j);

                            ref var releaseRef = ref Unsafe.Add(ref countRef, releaseIx);
                            while (Interlocked.CompareExchange(ref releaseRef, 0, int.MinValue) != int.MinValue)
                            {
                                // Optimistic shared lock got us, back off and try again
                                _ = Thread.Yield();
                            }
                        }

                        Unsafe.SkipInit(out lockToken);
                        return false;
                    }
                }

                // Successfully acquired all shards exclusively
                lockToken = hash;
                return true;
            }


            /// <summary>
            /// Acquire an exclusive lock for the given hash, blocking until that succeeds.
            /// 
            /// Will block all other locks until released.
            /// </summary>
            internal readonly void AcquireExclusiveLock(int hash, out int lockToken)
            {
                ref var countRef = ref MemoryMarshal.GetArrayDataReference(lockCounts);

                for (var i = 0; i < lockShardCount; i++)
                {
                    var acquireIx = CalculateIndex(hash, i);

                    ref var acquireRef = ref Unsafe.Add(ref countRef, acquireIx);
                    while (Interlocked.CompareExchange(ref acquireRef, int.MinValue, 0) != 0)
                    {
                        // Optimistic shared lock got us, or conflict with some other excluive lock acquisition
                        //
                        // Backoff and try again
                        _ = Thread.Yield();
                    }
                }

                lockToken = hash;
            }

            /// <summary>
            /// Release a lock previously acquired with <see cref="TryAcquireExclusiveLock(int, out int)"/>, <see cref="AcquireExclusiveLock(int, out int)"/>, or <see cref="TryPromoteSharedLock(int, int, out int)"/>.
            /// </summary>
            internal readonly void ReleaseExclusiveLock(int lockToken)
            {
                // The lockToken is a hash, so no range check here

                ref var countRef = ref MemoryMarshal.GetArrayDataReference(lockCounts);

                var hash = lockToken;

                for (var i = 0; i < lockShardCount; i++)
                {
                    var releaseIx = CalculateIndex(hash, i);

                    ref var releaseRef = ref Unsafe.Add(ref countRef, releaseIx);
                    while (Interlocked.CompareExchange(ref releaseRef, 0, int.MinValue) != int.MinValue)
                    {
                        // Optimistic shared lock got us, back off and try again
                        _ = Thread.Yield();
                    }
                }
            }

            /// <summary>
            /// Attempt to promote a shared lock previously acquired via <see cref="TryAcquireSharedLock(int, out int)"/> or <see cref="AcquireSharedLock(int, out int)"/> to an exclusive lock.
            /// 
            /// If successful, will block all other locks until released.
            /// 
            /// If successful, must be released with <see cref="ReleaseExclusiveLock(int)"/>.
            /// 
            /// If unsuccessful, shared lock will still be held and must be released with <see cref="ReleaseSharedLock(int)"/>.
            /// </summary>
            internal readonly bool TryPromoteSharedLock(int hash, int lockToken, out int newLockToken)
            {
                Debug.Assert(Interlocked.CompareExchange(ref lockCounts[lockToken], 0, 0) > 0, "Illegal call when not holding shard lock");

                Debug.Assert(lockToken >= 0 && lockToken < lockCounts.Length, "Invalid lock token");

                ref var countRef = ref MemoryMarshal.GetArrayDataReference(lockCounts);

                for (var i = 0; i < lockShardCount; i++)
                {
                    var acquireIx = CalculateIndex(hash, i);
                    ref var acquireRef = ref Unsafe.Add(ref countRef, acquireIx);

                    if (acquireIx == lockToken)
                    {
                        // Do the promote
                        if (Interlocked.CompareExchange(ref acquireRef, int.MinValue, 1) != 1)
                        {
                            // Failed, release previously acquired all of which are exclusive locks
                            for (var j = 0; j < i; j++)
                            {
                                var releaseIx = CalculateIndex(hash, j);

                                ref var releaseRef = ref Unsafe.Add(ref countRef, releaseIx);
                                while (Interlocked.CompareExchange(ref releaseRef, 0, int.MinValue) != int.MinValue)
                                {
                                    // Optimistic shared lock got us, back off and try again
                                    _ = Thread.Yield();
                                }
                            }

                            // Note we're still holding the shared lock here
                            Unsafe.SkipInit(out newLockToken);
                            return false;
                        }
                    }
                    else
                    {
                        // Otherwise attempt an exclusive acquire
                        if (Interlocked.CompareExchange(ref acquireRef, int.MinValue, 0) != 0)
                        {
                            // Failed, release previously acquired - one of which MIGHT be the shared lock
                            for (var j = 0; j < i; j++)
                            {
                                var releaseIx = CalculateIndex(hash, j);
                                var releaseTargetValue = releaseIx == lockToken ? 1 : 0;

                                ref var releaseRef = ref Unsafe.Add(ref countRef, releaseIx);
                                while (Interlocked.CompareExchange(ref releaseRef, releaseTargetValue, int.MinValue) != int.MinValue)
                                {
                                    // Optimistic shared lock got us, back off and try again
                                    _ = Thread.Yield();
                                }
                            }

                            // Note we're still holding the shared lock here
                            Unsafe.SkipInit(out newLockToken);
                            return false;
                        }
                    }
                }

                newLockToken = hash;
                return true;
            }
        }

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