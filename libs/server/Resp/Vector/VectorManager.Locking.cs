// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Garnet.common;
using Tsavorite.core;
using static Garnet.common.ReadOptimizedLock;

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
            private readonly LockToken lockToken;
            private readonly ref int elementLock;

            internal VectorSetLock(ref readonly ReadOptimizedLock lockableCtx, LockToken lockToken) : this(in lockableCtx, lockToken, ref Unsafe.NullRef<int>())
            {
                Debug.Assert(ActiveThreadSession != null, "Should never construct an actual lock without having set ActiveThreadSession first");
            }

            private VectorSetLock(ref readonly ReadOptimizedLock lockableCtx, LockToken lockToken, ref int elementLock)
            {
                this.lockToken = lockToken;
                this.lockableCtx = ref lockableCtx;
                this.elementLock = ref elementLock;
            }

            /// <summary>
            /// Add an acquired exclusive update lock on an element to this <see cref="VectorSetLock"/>.
            /// </summary>
            internal VectorSetLock WithElementLock(ref int elementLock)
            {
                Debug.Assert(!Unsafe.IsNullRef(ref elementLock), "Element lock must be non-null");
                Debug.Assert(elementLock == int.MinValue, "Element lock must already be acquired");
                Debug.Assert(Unsafe.IsNullRef(ref this.elementLock), "Cannot associate with second element lock");

                return new(in lockableCtx, lockToken, ref elementLock);
            }

            /// <summary>
            /// Combine two <see cref="VectorSetLock"/>s.
            /// 
            /// The original <see cref="VectorSetLock"/>s should not be used (including disposed) after this call.
            /// </summary>
            internal VectorSetPairLock PairWith(VectorSetLock otherLock)
            {
                Debug.Assert(Unsafe.IsNullRef(in otherLock.lockableCtx) || Unsafe.AreSame(in otherLock.lockableCtx, in lockableCtx), "Should never split lockable contexts");

                return new(in lockableCtx, lockToken, ref elementLock, otherLock.lockToken, ref otherLock.elementLock);
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                ClearActiveThreadSession();

                ReleaseLockImpl(in lockableCtx, lockToken, ref elementLock);
            }

            /// <summary>
            /// Common implemenation for releasing a <see cref="VectorSetLock"/> or <see cref="VectorSetPairLock"/>.
            /// </summary>
            internal static void ReleaseLockImpl(ref readonly ReadOptimizedLock lockableCtx, LockToken lockToken, ref int elementLock)
            {
                if (Unsafe.IsNullRef(in lockableCtx))
                {
                    return;
                }

                lockableCtx.ReleaseLock(lockToken);

                if (!Unsafe.IsNullRef(ref elementLock))
                {
                    var oldVal = Interlocked.Exchange(ref elementLock, 0);
                    Debug.Assert(oldVal == int.MinValue, "Unlock failed");
                }
            }
        }

        /// <summary>
        /// Used to scope a pair of <see cref="VectorSetLock"/>s.
        /// </summary>
        internal readonly ref struct VectorSetPairLock : IDisposable
        {
            private readonly ref readonly ReadOptimizedLock lockableCtx;

            private readonly LockToken lockToken0;
            private readonly ref int elementLock0;

            private readonly LockToken lockToken1;
            private readonly ref int elementLock1;

            internal VectorSetPairLock(ref readonly ReadOptimizedLock lockableCtx, LockToken lockToken0, ref int elementLock0, LockToken lockToken1, ref int elementLock1)
            {
                this.lockableCtx = ref lockableCtx;

                this.lockToken0 = lockToken0;
                this.elementLock0 = ref elementLock0;

                this.lockToken1 = lockToken1;
                this.elementLock1 = ref elementLock1;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                ClearActiveThreadSession();

                VectorSetLock.ReleaseLockImpl(in lockableCtx, lockToken0, ref elementLock0);
                VectorSetLock.ReleaseLockImpl(in lockableCtx, lockToken1, ref elementLock1);
            }
        }

        private readonly ReadOptimizedLock vectorSetLocks;

        private readonly int vectorSetElementSelectMask;
        private readonly int[] vectorSetElementUpdateLocks;

        /// <summary>
        /// Set <see cref="ActiveThreadSession"/> and <see cref="ActiveThreadShallowCopyBackReferences"/>.
        /// </summary>
        private static void SetActiveThreadSession(StorageSession storageSession, ReadOnlySpan<byte> indexSpan, ulong? knownContext)
        {
            const int DefaultLookbackArraySize = 16;

            Debug.Assert(knownContext is not null || indexSpan.Length == Index.Size, "Should have one or the other");
            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");

            ActiveThreadSession = storageSession;

            ulong shallowCopyOfContext;
            if (knownContext != null)
            {
                Debug.Assert((knownContext.Value % ContextStep) == 0, "Known context must be start context");
                if (!storageSession.vectorManager.shallowCopyBackReferences.TryGetValue(knownContext.Value, out shallowCopyOfContext))
                {
                    shallowCopyOfContext = 0;
                }
            }
            else
            {
                ReadIndex(indexSpan, out _, out _, out _, out _, out _, out _, out _, out _, out shallowCopyOfContext, out _);
            }

            if (shallowCopyOfContext != 0)
            {
                var lookupArr = ArrayPool<ulong>.Shared.Rent(DefaultLookbackArraySize);
                var lookupSpan = lookupArr.AsSpan();

                var curBackRef = shallowCopyOfContext;
                while (true)
                {
                    lookupSpan[0] = curBackRef;
                    lookupSpan = lookupSpan[1..];

                    if (!storageSession.vectorManager.shallowCopyBackReferences.TryGetValue(curBackRef, out curBackRef))
                    {
                        break;
                    }

                    if (lookupSpan.IsEmpty)
                    {
                        var newLookupArr = ArrayPool<ulong>.Shared.Rent(lookupArr.Length * 2);
                        lookupArr.CopyTo(newLookupArr);

                        lookupSpan = newLookupArr;
                        lookupSpan = lookupSpan[lookupArr.Length..];

                        ArrayPool<ulong>.Shared.Return(lookupArr);
                        lookupArr = newLookupArr;
                    }
                }

                ActiveThreadShallowCopyBackReferences = lookupArr.AsMemory()[..^lookupSpan.Length];
            }
        }

        /// <summary>
        /// Clear <see cref="ActiveThreadSession"/> and <see cref="ActiveThreadShallowCopyBackReferences"/>.
        /// </summary>
        private static void ClearActiveThreadSession()
        {
            ActiveThreadSession = null;

            if (!ActiveThreadShallowCopyBackReferences.IsEmpty)
            {
                if (MemoryMarshal.TryGetArray(ActiveThreadShallowCopyBackReferences, out var arrSeg) && arrSeg.Array != null && arrSeg.Array.Length > 0)
                {
                    ArrayPool<ulong>.Shared.Return(arrSeg.Array);
                }

                ActiveThreadShallowCopyBackReferences = default;
            }
        }

        /// <summary>
        /// Returns true for indexes that were created via a previous instance of <see cref="VectorManager"/>.
        /// 
        /// Such indexes still have element data, but the index pointer to the DiskANN bits are invalid.
        /// </summary>
        internal bool NeedsRecreate(ReadOnlySpan<byte> indexConfig)
        {
            ReadIndex(indexConfig, out _, out _, out _, out _, out _, out _, out _, out _, out _, out var indexPtr);

            return indexPtr == 0;
        }

        /// <summary>
        /// Acquire an exclusive lock based on an _element_ and a Vector Set key.
        /// 
        /// Should only be called after a <see cref="VectorSetLock"/> has been acquired for the Vector Set.
        /// </summary>
        private ref int GetElementSubLock(ReadOnlySpan<byte> key, ReadOnlySpan<byte> element)
        {
            var hash = SpanByteComparer.StaticGetHashCode64(key) ^ SpanByteComparer.StaticGetHashCode64(element);

            var vectorSetElementIndex = (int)(hash & vectorSetElementSelectMask);
            ref var vectorSetElementLock = ref vectorSetElementUpdateLocks[vectorSetElementIndex];

            while (Volatile.Read(ref vectorSetElementLock) != 0 || Interlocked.CompareExchange(ref vectorSetElementLock, int.MinValue, 0) != 0)
            {
                _ = Thread.Yield();
            }

            return ref vectorSetElementLock;
        }

        /// <summary>
        /// Equivalent of <see cref="ReadVectorIndex"/> but with a lock for updating a given element.
        /// 
        /// Used to guarantee sequencing on updates to element data.
        /// 
        /// Operations that do not _update_ element specific data should use <see cref="ReadVectorIndex"/>.
        /// </summary>
        internal VectorSetLock ReadVectorIndexWithElement(StorageSession storageSession, ReadOnlySpan<byte> key, ReadOnlySpan<byte> element, ref StringInput input, scoped Span<byte> indexSpan, out GarnetStatus status, out bool importPending)
        {
            var indexLock = ReadVectorIndex(storageSession, key, ref input, indexSpan, out status, out importPending);
            if (status != GarnetStatus.OK || importPending)
            {
                return indexLock;
            }

            return indexLock.WithElementLock(ref GetElementSubLock(key, element));
        }

        /// <summary>
        /// Utility method that will read an vector set index out but not create one.
        /// 
        /// It will however RECREATE one if needed.
        /// 
        /// Returns a disposable that prevents the index from being deleted while undisposed.
        /// 
        /// If locking operation will update an element, use <see cref="ReadVectorIndexWithElement"/> instead.
        /// </summary>
        internal VectorSetLock ReadVectorIndex(StorageSession storageSession, ReadOnlySpan<byte> key, ref StringInput input, scoped Span<byte> indexSpan, out GarnetStatus status, out bool importPending)
            => ReadVectorIndexCore(storageSession, key, ref input, indexSpan, nonBlocking: false, out status, out _, out importPending);

        /// <summary>
        /// Core of <see cref="ReadVectorIndex"/>. When <paramref name="nonBlocking"/> is <c>true</c>, the per-set
        /// lock is taken with the non-spinning <c>TryAcquire*</c> path: if it cannot be acquired immediately
        /// (another thread holds it — typically a network VADD recreating a disk-tiered index and blocked on a
        /// pending disk read), this returns a default (empty) lock with <paramref name="wouldBlock"/> set to
        /// <c>true</c> instead of spin-waiting. Callers that run on the thread pool use this so they can yield
        /// their pool thread and retry asynchronously rather than consuming the pool and starving the disk-IO
        /// completion that releases the lock.
        /// </summary>
        private VectorSetLock ReadVectorIndexCore(StorageSession storageSession, ReadOnlySpan<byte> key, ref StringInput input, scoped Span<byte> indexSpan, bool nonBlocking, out GarnetStatus status, out bool wouldBlock, out bool importPending)
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");

            var sessionSet = false;
            wouldBlock = false;
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

                    LockToken lockToken;
                    if (takeExclusiveLock)
                    {
                        if (nonBlocking)
                        {
                            if (!vectorSetLocks.TryAcquireExclusiveLock(keyHash, out lockToken))
                            {
                                status = default;
                                wouldBlock = true;
                                importPending = false;
                                return default;
                            }
                        }
                        else
                        {
                            vectorSetLocks.AcquireExclusiveLock(keyHash, out lockToken);
                        }

                        // If we pass through _again_, don't start with an exclusive lock
                        takeExclusiveLock = false;
                    }
                    else
                    {
                        if (nonBlocking)
                        {
                            if (!vectorSetLocks.TryAcquireSharedLock(keyHash, out lockToken))
                            {
                                status = default;
                                wouldBlock = true;
                                importPending = false;
                                return default;
                            }
                        }
                        else
                        {
                            vectorSetLocks.AcquireSharedLock(keyHash, out lockToken);
                        }
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
                        if (sessionSet)
                        {
                            ClearActiveThreadSession();
                            sessionSet = false;
                        }

                        SetActiveThreadSession(storageSession, indexSpan, null);
                        sessionSet = true;

                        needsRecreate = NeedsRecreate(indexConfigOutput.SpanByteAndMemory.ReadOnlySpan);

                        if (!needsRecreate)
                        {
                            if (readCmd == RespCommand.XVIMPORT && (IsImportCompleted(indexSpan) || IsImportFailed(indexSpan)))
                            {
                                status = GarnetStatus.OK;
                                importPending = false;
                                return new(in vectorSetLocks, lockToken);
                            }

                            if (IsImportPending(indexSpan))
                            {
                                if (readCmd == RespCommand.VINFO)
                                {
                                    status = GarnetStatus.OK;
                                    importPending = true;
                                    return new(in vectorSetLocks, lockToken);
                                }

                                if (readCmd != RespCommand.XVIMPORT)
                                {
                                    status = GarnetStatus.WRONGTYPE;
                                    importPending = true;
                                    vectorSetLocks.ReleaseLock(lockToken);
                                    return default;
                                }
                            }
                        }
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

                            if (nonBlocking)
                            {
                                // Don't spin on a pool thread waiting for the drop to finish; report contention so
                                // the caller yields its pool thread and retries.
                                status = default;
                                wouldBlock = true;
                                importPending = false;
                                return default;
                            }

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

                        if (!sessionSet)
                        {
                            SetActiveThreadSession(storageSession, indexSpan, null);
                            sessionSet = true;
                        }

                        ReadIndex(indexSpan, out var indexContext, out var dims, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var distanceMetric, out _, out _, out _);

                        input.arg1 = RecreateIndexArg;

                        nint newlyAllocatedIndex;
                        bool requestQuantization;
                        unsafe
                        {
                            newlyAllocatedIndex = Service.RecreateIndex(indexContext, dims, reduceDims, quantType, buildExplorationFactor, numLinks, distanceMetric, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr, ReadModifyWriteCallbackPtr, FilterCallbackPtr, LogCallbackPtr, ReadStartPointId(indexSpan), out requestQuantization);
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
                                EnqueueQuantization(new(key.ToArray(), QuantizationStep.BuildQuantizationTable, 0));
                            }

                            // Try again so we don't hold an exclusive lock while performing a search
                            vectorSetLocks.ReleaseLock(lockToken);

                            continue;
                        }
                        else
                        {
                            status = writeRes;
                            vectorSetLocks.ReleaseLock(lockToken);
                            importPending = false;

                            return default;
                        }
                    }
                    else if (readRes != GarnetStatus.OK)
                    {
                        status = readRes;
                        vectorSetLocks.ReleaseLock(lockToken);
                        importPending = false;

                        return default;
                    }

                    status = GarnetStatus.OK;
                    importPending = false;
                    return new(in vectorSetLocks, lockToken);
                }
            }
            catch
            {
                // If we exit without returning a lock, we'll leave ActiveThreadSession set, this clears it and rethrows
                //
                // In the normal exit case, disposing ReadVectorLock will clear ActiveThreadSession
                if (sessionSet)
                {
                    ClearActiveThreadSession();
                }

                throw;
            }
        }

        /// <summary>
        /// Equivalent of <see cref="ReadOrCreateVectorIndex"/> but with a lock for updating a given element.
        /// 
        /// Used to guarantee sequencing on updates to element data.
        /// 
        /// Operations that do not _update_ element specific data should use <see cref="ReadOrCreateVectorIndex"/>.
        /// </summary>
        internal VectorSetLock ReadOrCreateVectorIndexWithElement(StorageSession storageSession, ReadOnlySpan<byte> key, ReadOnlySpan<byte> element, ref StringInput input, scoped Span<byte> indexSpan, out GarnetStatus status, out bool importPending)
        {
            var indexLock = ReadOrCreateVectorIndex(storageSession, key, ref input, indexSpan, DefaultStartPointId, out status, out importPending);
            if (status != GarnetStatus.OK || importPending)
            {
                return indexLock;
            }

            return indexLock.WithElementLock(ref GetElementSubLock(key, element));
        }

        /// <summary>
        /// Utility method that will read vector set index out, create one if it doesn't exist, or RECREATE one if needed.
        /// 
        /// The <paramref name="demandCreate"/> allows fast failing if an index already exists under that key.
        /// 
        /// Returns a disposable that prevents the index from being deleted while undisposed.
        /// </summary>
        internal VectorSetLock ReadOrCreateVectorIndex(
            StorageSession storageSession,
            ReadOnlySpan<byte> key,
            ref StringInput input,
            scoped Span<byte> indexSpan,
            uint startPointId,
            out GarnetStatus status,
            out bool importPending,
            bool demandCreate = false
        )
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");

            var sessionSet = false;
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
                        if (demandCreate)
                        {
                            // WRONGTYPE is close enough - we wanted an empty key and we found a non-empty key
                            status = GarnetStatus.WRONGTYPE;
                            vectorSetLocks.ReleaseLock(lockToken);
                            importPending = false;
                            return default;
                        }

                        if (IsImportPending(indexSpan))
                        {
                            status = GarnetStatus.WRONGTYPE;
                            vectorSetLocks.ReleaseLock(lockToken);
                            importPending = true;
                            return default;
                        }

                        if (sessionSet)
                        {
                            ClearActiveThreadSession();
                            sessionSet = false;
                        }

                        SetActiveThreadSession(storageSession, indexSpan, null);
                        sessionSet = true;

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
                        long expirationTicks = 0;
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

                            if (!sessionSet)
                            {
                                SetActiveThreadSession(storageSession, indexSpan, null);
                                sessionSet = true;
                            }

                            ReadIndex(indexSpan, out indexContext, out var dims, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var distanceMetric, out _, out _, out _);

                            input.arg1 = RecreateIndexArg;

                            unsafe
                            {
                                newlyAllocatedIndex = Service.RecreateIndex(indexContext, dims, reduceDims, quantType, buildExplorationFactor, numLinks, distanceMetric, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr, ReadModifyWriteCallbackPtr, FilterCallbackPtr, LogCallbackPtr, ReadStartPointId(indexSpan), out requestQuantization);
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

                            if (!sessionSet)
                            {
                                SetActiveThreadSession(storageSession, default, indexContext);
                                sessionSet = true;
                            }

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
                                newlyAllocatedIndex = Service.CreateIndex(indexContext, dims, reduceDims, quantizer, buildExplorationFactor, numLinks, distanceMetric, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr, ReadModifyWriteCallbackPtr, FilterCallbackPtr, LogCallbackPtr, startPointId, out requestQuantization);
                            }

                            expirationTicks = input.parseState.Count >= 13 ? MemoryMarshal.Read<long>(input.parseState.GetArgSliceByRef(12).Span) : 0;
                            input.parseState.EnsureCapacity(14);

                            // Save off for insertion
                            input.parseState.SetArgument(10, PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<ulong, byte>(MemoryMarshal.CreateSpan(ref indexContext, 1))));
                            input.parseState.SetArgument(11, PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<nint, byte>(MemoryMarshal.CreateSpan(ref newlyAllocatedIndex, 1))));
                            input.parseState.SetArgument(12, PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<long, byte>(MemoryMarshal.CreateSpan(ref expirationTicks, 1))));
                            input.parseState.SetArgument(13, PinnedSpanByte.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref startPointId, 1))));
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
                                EnqueueQuantization(new(key.ToArray(), QuantizationStep.BuildQuantizationTable, 0));
                            }

                            // Try again so we don't hold an exclusive lock while adding a vector (which might be time consuming)
                            vectorSetLocks.ReleaseLock(lockToken);

                            // We created the index, so we no longer need to demand create
                            demandCreate = false;
                            continue;
                        }
                        else
                        {
                            status = writeRes;
                            vectorSetLocks.ReleaseLock(lockToken);
                            importPending = false;

                            return default;
                        }
                    }
                    else if (readRes != GarnetStatus.OK)
                    {
                        vectorSetLocks.ReleaseLock(lockToken);
                        importPending = false;
                        status = readRes;

                        return default;
                    }

                    status = GarnetStatus.OK;
                    importPending = false;
                    return new(in vectorSetLocks, lockToken);
                }
            }
            catch
            {
                // If we exit without returning a lock, we'll leave ActiveThreadSession set, this clears it and rethrows
                //
                // In the normal exit case, disposing ReadVectorLock will clear ActiveThreadSession
                if (sessionSet)
                {
                    ClearActiveThreadSession();
                }

                throw;
            }
        }

        /// <summary>
        /// Acquire exclusive lock over a given key.
        /// </summary>
        private VectorSetLock AcquireExclusiveLock(StorageSession storageSession, ReadOnlySpan<byte> key)
        {
            var token = AcquireExclusiveLockToken(storageSession, key);
            return new(in vectorSetLocks, token);
        }

        /// <summary>
        /// Acquire exclusive lock token over a given key.
        /// </summary>
        private LockToken AcquireExclusiveLockToken(StorageSession storageSession, ReadOnlySpan<byte> key, long? keyHashPrecalc = null)
        {
            var keyHash = keyHashPrecalc ?? storageSession.stringTransactionalContext.GetKeyHash((FixedSpanByteKey)key);

            vectorSetLocks.AcquireExclusiveLock(keyHash, out var exclusiveLockToken);

            return exclusiveLockToken;
        }

        /// <summary>
        /// Utility method that will read a Vector Set index out, and acquire TWO sets of exclusive locks to allow a shallow copy.
        /// </summary>
        internal VectorSetPairLock ReadForCopyVectorSetIndexes(StorageSession storageSession, ReadOnlySpan<byte> sourceKey, ReadOnlySpan<byte> destKey, ref StringInput input, scoped Span<byte> sourceIndexSpan, out bool importPending, out GarnetStatus status)
        {
            Debug.Assert(sourceIndexSpan.Length == IndexSizeBytes, "Insufficient space for index");
            Debug.Assert(!sourceKey.SequenceEqual(destKey), "Cannot copy source into itself");

            var indexConfigOutput = StringOutput.FromPinnedSpan(sourceIndexSpan);

            var sourceKeyHash = storageSession.stringTransactionalContext.GetKeyHash((FixedSpanByteKey)sourceKey);
            var destKeyHash = storageSession.stringTransactionalContext.GetKeyHash((FixedSpanByteKey)destKey);

            var sourceKeyAcquisitonIx = vectorSetLocks.CalculateIndex(sourceKeyHash, 0);
            var destKeyAcquisitonIx = vectorSetLocks.CalculateIndex(destKeyHash, 0);

            LockToken firstLock;
            LockToken? secondLock;

            // Ordering of lock acquisition and release matters
            if (sourceKeyAcquisitonIx < destKeyAcquisitonIx)
            {
                // Source key preceeds dest key
                firstLock = AcquireExclusiveLockToken(storageSession, sourceKey, sourceKeyHash);
                secondLock = AcquireExclusiveLockToken(storageSession, destKey, destKeyHash);
            }
            else if (sourceKeyAcquisitonIx > destKeyAcquisitonIx)
            {
                // Dest key preceeds source key
                firstLock = AcquireExclusiveLockToken(storageSession, destKey, destKeyHash);
                secondLock = AcquireExclusiveLockToken(storageSession, sourceKey, sourceKeyHash);
            }
            else
            {
                // Both keys are acquiring the same "slot" in lock set, so just acquire one
                firstLock = AcquireExclusiveLockToken(storageSession, sourceKey, sourceKeyHash);
                secondLock = null;
            }

            try
            {
                status = storageSession.Read_MainStore(sourceKey, ref input, ref indexConfigOutput, ref storageSession.stringBasicContext);
                Debug.Assert(indexConfigOutput.SpanByteAndMemory.IsSpanByte, "Should never need to move index onto the heap");
            }
            catch
            {
                vectorSetLocks.ReleaseLock(firstLock);

                if (secondLock != null)
                {
                    vectorSetLocks.ReleaseLock(secondLock.Value);
                }

                throw;
            }

            // Build the lock container now that we have index details
            VectorSetPairLock acquiredLockPair;
            if (status == GarnetStatus.OK)
            {
                SetActiveThreadSession(storageSession, sourceIndexSpan, null);

                acquiredLockPair = new VectorSetLock(in vectorSetLocks, firstLock).PairWith(secondLock != null ? new VectorSetLock(in vectorSetLocks, secondLock.Value) : default);

                // Can't copy a Vector Set which is being imported, we can't make it read only
                if (IsImportPending(sourceIndexSpan))
                {
                    importPending = true;

                    status = GarnetStatus.WRONGTYPE;

                    return acquiredLockPair;
                }
            }
            else
            {
                vectorSetLocks.ReleaseLock(firstLock);

                if (secondLock != null)
                {
                    vectorSetLocks.ReleaseLock(secondLock.Value);
                }

                acquiredLockPair = default;
            }

            importPending = false;
            return acquiredLockPair;
        }
    }
}