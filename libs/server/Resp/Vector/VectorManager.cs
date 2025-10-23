// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    public enum VectorManagerResult
    {
        Invalid = 0,

        OK,
        BadParams,
        Duplicate,
        MissingElement,
    }

    /// <summary>
    /// Methods for managing an implementation of various vector operations.
    /// </summary>
    public sealed class VectorManager : IDisposable
    {
        internal const int IndexSizeBytes = Index.Size;
        internal const long VADDAppendLogArg = long.MinValue;
        internal const long DeleteAfterDropArg = VADDAppendLogArg + 1;
        internal const long RecreateIndexArg = DeleteAfterDropArg + 1;
        internal const long VREMAppendLogArg = RecreateIndexArg + 1;

        public unsafe struct VectorReadBatch : IReadArgBatch<SpanByte, VectorInput, SpanByte>
        {
            public int Count { get; }

            private readonly ulong context;
            private readonly SpanByte lengthPrefixedKeys;

            public readonly unsafe delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void> callback;
            public readonly nint callbackContext;

            private int currentIndex;

            private int currentLen;
            private byte* currentPtr;

            private bool hasPending;

            public VectorReadBatch(delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void> callback, nint callbackContext, ulong context, uint keyCount, SpanByte lengthPrefixedKeys)
            {
                this.context = context;
                this.lengthPrefixedKeys = lengthPrefixedKeys;

                this.callback = callback;
                this.callbackContext = callbackContext;

                currentIndex = 0;
                Count = (int)keyCount;

                currentPtr = this.lengthPrefixedKeys.ToPointerWithMetadata();
                currentLen = *(int*)currentPtr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void AdvanceTo(int i)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                if (i == currentIndex)
                {
                    return;
                }

                // Undo namespace mutation
                *(int*)currentPtr = currentLen;

                // Most likely case, we're going one forward
                if (i == (currentIndex + 1))
                {
                    currentPtr += currentLen + sizeof(int); // Skip length prefix too

                    Debug.Assert(currentPtr < lengthPrefixedKeys.ToPointerWithMetadata() + lengthPrefixedKeys.Length, "About to access out of bounds data");

                    currentLen = *currentPtr;

                    currentIndex = i;

                    return;
                }

                // Next most likely case, we're going back to the start
                currentPtr = lengthPrefixedKeys.ToPointerWithMetadata();
                currentLen = *(int*)currentPtr;
                currentIndex = 0;

                if (i == 0)
                {
                    return;
                }

                SlowPath(ref this, i);

                // For the case where we're not just scanning or rolling back to 0, just iterate
                //
                // This should basically never happen
                [MethodImpl(MethodImplOptions.NoInlining)]
                static void SlowPath(ref VectorReadBatch self, int i)
                {
                    for (var subI = 1; subI <= i; subI++)
                    {
                        self.AdvanceTo(subI);
                    }
                }
            }

            /// <inheritdoc/>
            public void GetKey(int i, out SpanByte key)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                AdvanceTo(i);

                key = SpanByte.FromPinnedPointer(currentPtr + 3, currentLen + 1);
                key.MarkNamespace();
                key.SetNamespaceInPayload((byte)context);
            }

            /// <inheritdoc/>
            public readonly void GetInput(int i, out VectorInput input)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                input = default;
                input.CallbackContext = callbackContext;
                input.Callback = callback;
                input.Index = i;
            }

            /// <inheritdoc/>
            public readonly void GetOutput(int i, out SpanByte output)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                // Don't care, won't be used
                Unsafe.SkipInit(out output);
            }

            /// <inheritdoc/>
            public readonly void SetOutput(int i, SpanByte output)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");
            }

            /// <inheritdoc/>
            public void SetStatus(int i, Status status)
            {
                Debug.Assert(i >= 0 && i < Count, "Trying to advance out of bounds");

                hasPending |= status.IsPending;
            }

            internal readonly void CompletePending<TContext>(ref TContext objectContext)
                where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
            {
                // Undo mutations
                *(int*)currentPtr = currentLen;

                if (hasPending)
                {
                    _ = objectContext.CompletePending(wait: true);
                }
            }
        }

        [StructLayout(LayoutKind.Explicit, Size = Size)]
        private struct Index
        {
            internal const int Size = 52;

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
            public Guid ProcessInstanceId;
        }

        /// <summary>
        /// Used for tracking which contexts are currently active.
        /// </summary>
        [StructLayout(LayoutKind.Explicit, Size = Size)]
        internal struct ContextMetadata
        {
            internal const int Size = 3 * sizeof(ulong);

            // MUST BE A POWER OF 2
            internal const ulong ContextStep = 8;

            [FieldOffset(0)]
            public ulong Version;

            [FieldOffset(8)]
            public ulong InUse;

            [FieldOffset(16)]
            public ulong CleaningUp;

            public readonly bool IsInUse(ulong context)
            {
                Debug.Assert(context > 0, "Context 0 is reserved, should never queried");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert(context <= byte.MaxValue, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                return (InUse & mask) != 0;
            }

            public readonly ulong NextNotInUse()
            {
                var ignoringZero = InUse | 1;

                var bit = (ulong)BitOperations.TrailingZeroCount(~ignoringZero & (ulong)-(long)(~ignoringZero));

                if (bit == 64)
                {
                    throw new GarnetException("All possible Vector Sets allocated");
                }

                var ret = bit * ContextStep;

                return ret;
            }

            public void MarkInUse(ulong context)
            {
                Debug.Assert(context > 0, "Context 0 is reserved, should never queried");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert(context <= byte.MaxValue, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((InUse & mask) == 0, "About to mark context which is already in use");
                InUse |= mask;

                Version++;
            }

            public void MarkCleaningUp(ulong context)
            {
                Debug.Assert(context > 0, "Context 0 is reserved, should never queried");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert(context <= byte.MaxValue, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((InUse & mask) != 0, "About to mark for cleanup when not actually in use");
                Debug.Assert((CleaningUp & mask) == 0, "About to mark for cleanup when already marked");
                CleaningUp |= mask;

                Version++;
            }

            public void FinishedCleaningUp(ulong context)
            {
                Debug.Assert(context > 0, "Context 0 is reserved, should never queried");
                Debug.Assert((context % ContextStep) == 0, "Should only consider whole block of context, not a sub-bit");
                Debug.Assert(context <= byte.MaxValue, "Context larger than expected");

                var bitIx = context / ContextStep;
                var mask = 1UL << (byte)bitIx;

                Debug.Assert((InUse & mask) != 0, "Cleaned up context which isn't in use");
                Debug.Assert((CleaningUp & mask) != 0, "Cleaned up context not marked for it");
                CleaningUp &= ~mask;
                InUse &= ~mask;

                Version++;
            }

            public readonly HashSet<ulong> GetNeedCleanup()
            {
                if (CleaningUp == 0)
                {
                    return null;
                }

                var ret = new HashSet<ulong>();

                var remaining = CleaningUp;
                while (remaining != 0UL)
                {
                    var ix = BitOperations.TrailingZeroCount(remaining);

                    _ = ret.Add((ulong)ix * ContextStep);

                    remaining &= ~(1UL << (byte)ix);
                }

                return ret;
            }
        }

        private readonly record struct VADDReplicationState(Memory<byte> Key, uint Dims, uint ReduceDims, VectorValueType ValueType, Memory<byte> Values, Memory<byte> Element, VectorQuantType Quantizer, uint BuildExplorationFactor, Memory<byte> Attributes, uint NumLinks)
        {
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
        /// Used to scope exclusive locks and a context related to a Vector Set delete operation.
        /// 
        /// Disposing this ends the lockable context, releases the locks, and exits the storage session context on the current thread.
        /// </summary>
        internal readonly ref struct DeleteVectorLock : IDisposable
        {
            private readonly ref LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> lockableCtx;
            private readonly ReadOnlySpan<TxnKeyEntry> entries;

            internal DeleteVectorLock(ref LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> lockableCtx, ReadOnlySpan<TxnKeyEntry> entries)
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

        /// <summary>
        /// Used as part of scanning post-index-delete to cleanup abandoned data.
        /// </summary>
        private sealed class PostDropCleanupFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
        {
            private readonly StorageSession storageSession;
            private readonly FrozenSet<ulong> contexts;

            public PostDropCleanupFunctions(StorageSession storageSession, HashSet<ulong> contexts)
            {
                this.contexts = contexts.ToFrozenSet();
                this.storageSession = storageSession;
            }

            public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            public void OnException(Exception exception, long numberOfRecords) { }
            public bool OnStart(long beginAddress, long endAddress) => true;
            public void OnStop(bool completed, long numberOfRecords) { }

            public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                if (key.MetadataSize != 1)
                {
                    // Not Vector Set, ignore
                    cursorRecordResult = CursorRecordResult.Skip;
                    return true;
                }

                var ns = key.GetNamespaceInPayload();
                var pairedContext = (ulong)ns & ~0b11UL;
                if (!contexts.Contains(pairedContext))
                {
                    // Vector Set, but not one we're scanning for
                    cursorRecordResult = CursorRecordResult.Skip;
                    return true;
                }

                // Delete it
                var status = storageSession.vectorContext.Delete(ref key, 0);
                if (status.IsPending)
                {
                    SpanByte ignored = default;
                    CompletePending(ref status, ref ignored, ref storageSession.vectorContext);
                }

                cursorRecordResult = CursorRecordResult.Accept;
                return true;
            }
        }

        /// <summary>
        /// Minimum size of an id is assumed to be at least 4 bytes + a length prefix.
        /// </summary>
        private const int MinimumSpacePerId = sizeof(int) + 4;

        private unsafe delegate* unmanaged[Cdecl]<ulong, uint, nint, nuint, nint, nint, void> ReadCallbackPtr { get; } = &ReadCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, nint, nuint, byte> WriteCallbackPtr { get; } = &WriteCallbackUnmanaged;
        private unsafe delegate* unmanaged[Cdecl]<ulong, nint, nuint, byte> DeleteCallbackPtr { get; } = &DeleteCallbackUnmanaged;

        private DiskANNService Service { get; } = new DiskANNService();

        private readonly Guid processInstanceId = Guid.NewGuid();

        private ContextMetadata contextMetadata;

        private int replicationReplayStarted;
        private long replicationReplayPendingVAdds;
        private readonly ManualResetEventSlim replicationBlockEvent;
        private readonly Channel<VADDReplicationState> replicationReplayChannel;
        private readonly Task[] replicationReplayTasks;

        [ThreadStatic]
        private static StorageSession ActiveThreadSession;

        private readonly ILogger logger;

        internal readonly int readLockShardCount;
        private readonly long readLockShardMask;

        private Channel<object> cleanupTaskChannel;
        private readonly Task cleanupTask;
        private readonly Func<IMessageConsumer> getCleanupSession;

        public VectorManager(Func<IMessageConsumer> getCleanupSession, ILogger logger)
        {
            replicationBlockEvent = new(true);
            replicationReplayChannel = Channel.CreateUnbounded<VADDReplicationState>(new() { SingleWriter = true, SingleReader = false, AllowSynchronousContinuations = false });

            // TODO: Pull this off a config or something
            replicationReplayTasks = new Task[Environment.ProcessorCount];
            for (var i = 0; i < replicationReplayTasks.Length; i++)
            {
                replicationReplayTasks[i] = Task.CompletedTask;
            }

            this.logger = logger;

            // TODO: Probably configurable?
            // For now, nearest power of 2 >= process count;
            readLockShardCount = (int)BitOperations.RoundUpToPowerOf2((uint)Environment.ProcessorCount);
            readLockShardMask = readLockShardCount - 1;

            this.getCleanupSession = getCleanupSession;
            cleanupTaskChannel = Channel.CreateUnbounded<object>(new() { SingleWriter = false, SingleReader = true, AllowSynchronousContinuations = false });
            cleanupTask = RunCleanupTaskAsync();
        }

        /// <summary>
        /// Load state necessary for VectorManager from main store.
        /// </summary>
        public void Initialize()
        {
            using var session = (RespServerSession)getCleanupSession();

            Span<byte> keySpan = stackalloc byte[1];
            Span<byte> dataSpan = stackalloc byte[ContextMetadata.Size];

            var key = SpanByte.FromPinnedSpan(keySpan);

            key.MarkNamespace();
            key.SetNamespaceInPayload(0);

            var data = SpanByte.FromPinnedSpan(dataSpan);

            ref var ctx = ref session.storageSession.vectorContext;

            var status = ctx.Read(ref key, ref data);

            if (status.IsPending)
            {
                SpanByte ignored = default;
                CompletePending(ref status, ref ignored, ref ctx);
            }

            // Can be not found if we've never spun up a Vector Set
            if (status.Found)
            {
                contextMetadata = MemoryMarshal.Cast<byte, ContextMetadata>(dataSpan)[0];
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            // We must drain all these before disposing, otherwise we'll leave replicationBlockEvent unset
            replicationReplayChannel.Writer.Complete();
            replicationReplayChannel.Reader.Completion.Wait();

            Task.WhenAll(replicationReplayTasks).Wait();

            replicationBlockEvent.Dispose();

            // Wait for any in progress cleanup to finish
            cleanupTaskChannel.Writer.Complete();
            cleanupTaskChannel.Reader.Completion.Wait();
            cleanupTask.Wait();
        }

        /// <summary>
        /// Get a new unique context for a vector set.
        /// 
        /// This value is guaranteed to not be shared by any other vector set in the store.
        /// </summary>
        private ulong NextContext()
        {
            // TODO: This retry is no good, but will go away when namespaces >= 256 are possible
            while (true)
            {
                // Lock isn't amazing, but _new_ vector set creation should be rare
                // So just serializing it all is easier.
                try
                {
                    ulong nextFree;
                    lock (this)
                    {
                        nextFree = contextMetadata.NextNotInUse();

                        contextMetadata.MarkInUse(nextFree);
                    }

                    logger?.LogDebug("Allocated vector set with context {nextFree}", nextFree);
                    return nextFree;
                }
                catch (Exception e)
                {
                    logger?.LogError(e, "NextContext not available, delaying and retrying");
                }

                // HACK HACK HACK
                Thread.Sleep(1_000);
            }
        }

        /// <summary>
        /// Called when an index creation succeeds to flush <see cref="contextMetadata"/> into the store.
        /// </summary>
        private void UpdateContextMetadata<TContext>(ref TContext ctx)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            Span<byte> keySpan = stackalloc byte[1];
            Span<byte> dataSpan = stackalloc byte[ContextMetadata.Size];

            lock (this)
            {
                MemoryMarshal.Cast<byte, ContextMetadata>(dataSpan)[0] = contextMetadata;
            }

            var key = SpanByte.FromPinnedSpan(keySpan);

            key.MarkNamespace();
            key.SetNamespaceInPayload(0);

            VectorInput input = default;
            unsafe
            {
                input.CallbackContext = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dataSpan));
            }

            var data = SpanByte.FromPinnedSpan(dataSpan);

            var status = ctx.RMW(ref key, ref input);

            if (status.IsPending)
            {
                SpanByte ignored = default;
                CompletePending(ref status, ref ignored, ref ctx);
            }
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe void ReadCallbackUnmanaged(
            ulong context,
            uint numKeys,
            nint keysData,
            nuint keysLength,
            nint dataCallback,
            nint dataCallbackContext
        )
        {
            // Takes: index, dataCallbackContext, data pointer, data length, and returns nothing
            var dataCallbackDel = (delegate* unmanaged[Cdecl, SuppressGCTransition]<int, nint, nint, nuint, void>)dataCallback;

            var enumerable = new VectorReadBatch(dataCallbackDel, dataCallbackContext, context, numKeys, SpanByte.FromPinnedPointer((byte*)keysData, (int)keysLength));

            ref var ctx = ref ActiveThreadSession.vectorContext;

            ctx.ReadWithPrefetch(ref enumerable);

            enumerable.CompletePending(ref ctx);
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte WriteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength, nint writeData, nuint writeLength)
        {
            var keyWithNamespace = MarkDiskANNKeyWithNamespace(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorContext;
            VectorInput input = default;
            var valueSpan = SpanByte.FromPinnedPointer((byte*)writeData, (int)writeLength);
            SpanByte outputSpan = default;

            var status = ctx.Upsert(ref keyWithNamespace, ref input, ref valueSpan, ref outputSpan);
            if (status.IsPending)
            {
                CompletePending(ref status, ref outputSpan, ref ctx);
            }

            return status.IsCompletedSuccessfully ? (byte)1 : default;
        }

        [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
        private static unsafe byte DeleteCallbackUnmanaged(ulong context, nint keyData, nuint keyLength)
        {
            var keyWithNamespace = MarkDiskANNKeyWithNamespace(context, keyData, keyLength);

            ref var ctx = ref ActiveThreadSession.vectorContext;

            var status = ctx.Delete(ref keyWithNamespace);
            Debug.Assert(!status.IsPending, "Deletes should never go async");

            return status.IsCompletedSuccessfully && status.Found ? (byte)1 : default;
        }

        private static unsafe bool ReadSizeUnknown(ulong context, ReadOnlySpan<byte> key, ref SpanByteAndMemory value)
        {
            Span<byte> distinctKey = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(distinctKey);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload((byte)context);
            key.CopyTo(keyWithNamespace.AsSpan());

            ref var ctx = ref ActiveThreadSession.vectorContext;

        tryAgain:
            VectorInput input = new();
            input.ReadDesiredSize = -1;
            fixed (byte* ptr = value.AsSpan())
            {
                SpanByte asSpanByte = new(value.Length, (nint)ptr);

                var status = ctx.Read(ref keyWithNamespace, ref input, ref asSpanByte);
                if (status.IsPending)
                {
                    CompletePending(ref status, ref asSpanByte, ref ctx);
                }

                if (!status.Found)
                {
                    value.Length = 0;
                    return false;
                }

                if (input.ReadDesiredSize > asSpanByte.Length)
                {
                    value.Memory?.Dispose();
                    var newAlloc = MemoryPool<byte>.Shared.Rent(input.ReadDesiredSize);
                    value = new(newAlloc, newAlloc.Memory.Length);
                    goto tryAgain;
                }

                value.Length = asSpanByte.Length;
                return true;
            }
        }

        /// <summary>
        /// Get a <see cref="SpanByte"/> which covers (keyData, keyLength), but has a namespace component based on <paramref name="context"/>.
        /// 
        /// Attempts to do this in place.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe SpanByte MarkDiskANNKeyWithNamespace(ulong context, nint keyData, nuint keyLength)
        {
            // DiskANN guarantees we have 4-bytes worth of unused data right before the key
            var keyPtr = (byte*)keyData;
            var keyNamespaceByte = keyPtr - 1;

            // TODO: if/when namespace can be > 4-bytes, we'll need to copy here

            var keyWithNamespace = SpanByte.FromPinnedPointer(keyNamespaceByte, (int)(keyLength + 1));
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload((byte)context);

            return keyWithNamespace;
        }

        private static void CompletePending<TContext>(ref Status status, ref SpanByte output, ref TContext objectContext)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            objectContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            Debug.Assert(!completedOutputs.Next());
            completedOutputs.Dispose();
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
            ref SpanByte indexValue)
        {
            AssertHaveStorageSession();

            var context = NextContext();

            nint indexPtr;
            unsafe
            {
                // HACK HACK HACK
                // TODO: do something less awful here
                var threadCtx = ActiveThreadSession;

                Task<nint> offload = Task.Factory.StartNew(
                    () =>
                    {
                        ActiveThreadSession = threadCtx;
                        try
                        {
                            return Service.CreateIndex(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr);
                        }
                        finally
                        {
                            ActiveThreadSession = null;
                        }
                    },
                    TaskCreationOptions.RunContinuationsAsynchronously
                );

                //indexPtr = Service.CreateIndex(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr);
                indexPtr = offload.GetAwaiter().GetResult();

                ActiveThreadSession = threadCtx;
            }

            var indexSpan = indexValue.AsSpan();

            if (indexSpan.Length != Index.Size)
            {
                logger?.LogCritical("Acquired space for vector set index does not match expectations, {0} != {1}", indexSpan.Length, Index.Size);
                throw new GarnetException($"Acquired space for vector set index does not match expectations, {indexSpan.Length} != {Index.Size}");
            }

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexSpan));
            asIndex.Context = context;
            asIndex.Dimensions = dimensions;
            asIndex.ReduceDims = reduceDims;
            asIndex.QuantType = quantType;
            asIndex.BuildExplorationFactor = buildExplorationFactor;
            asIndex.NumLinks = numLinks;
            asIndex.IndexPtr = (ulong)indexPtr;
            asIndex.ProcessInstanceId = processInstanceId;
        }

        /// <summary>
        /// Recreate an index that was created by a prior instance of Garnet.
        /// 
        /// This implies the index still has element data, but the pointer is garbage.
        /// </summary>
        internal void RecreateIndex(ref SpanByte indexValue)
        {
            AssertHaveStorageSession();

            var indexSpan = indexValue.AsSpan();

            if (indexSpan.Length != Index.Size)
            {
                logger?.LogCritical("Acquired space for vector set index does not match expectations, {0} != {1}", indexSpan.Length, Index.Size);
                throw new GarnetException($"Acquired space for vector set index does not match expectations, {indexSpan.Length} != {Index.Size}");
            }

            ReadIndex(indexSpan, out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out _, out var indexProcessInstanceId);
            Debug.Assert(processInstanceId != indexProcessInstanceId, "Should be recreating an index that matched our instance id");

            nint indexPtr;
            unsafe
            {
                // HACK HACK HACK
                // TODO: do something less awful here
                var threadCtx = ActiveThreadSession;

                Task<nint> offload = Task.Factory.StartNew(
                    () =>
                    {
                        ActiveThreadSession = threadCtx;
                        try
                        {
                            return Service.RecreateIndex(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr);
                        }
                        finally
                        {
                            ActiveThreadSession = null;
                        }
                    },
                    TaskCreationOptions.RunContinuationsAsynchronously
                );

                //indexPtr = Service.RecreateIndex(context, dimensions, reduceDims, quantType, buildExplorationFactor, numLinks, ReadCallbackPtr, WriteCallbackPtr, DeleteCallbackPtr);
                indexPtr = offload.GetAwaiter().GetResult();

                ActiveThreadSession = threadCtx;
            }

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexSpan));
            asIndex.IndexPtr = (ulong)indexPtr;
            asIndex.ProcessInstanceId = processInstanceId;
        }

        /// <summary>
        /// Drop an index previously constructed with <see cref="CreateIndex"/>.
        /// </summary>
        internal void DropIndex(ReadOnlySpan<byte> indexValue)
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out _, out _, out _, out _, out _, out var indexPtr, out var indexProcessInstanceId);

            if (indexProcessInstanceId != processInstanceId)
            {
                // We never actually spun this index up, so nothing to drop
                return;
            }

            Service.DropIndex(context, indexPtr);
        }

        internal static void ReadIndex(
            ReadOnlySpan<byte> indexValue,
            out ulong context,
            out uint dimensions,
            out uint reduceDims,
            out VectorQuantType quantType,
            out uint buildExplorationFactor,
            out uint numLinks,
            out nint indexPtr,
            out Guid processInstanceId
        )
        {
            if (indexValue.Length != Index.Size)
            {
                throw new GarnetException($"Index size is incorrect ({indexValue.Length} != {Index.Size}), implies vector set index is probably corrupted");
            }

            ref var asIndex = ref Unsafe.As<byte, Index>(ref MemoryMarshal.GetReference(indexValue));

            context = asIndex.Context;
            dimensions = asIndex.Dimensions;
            reduceDims = asIndex.ReduceDims;
            quantType = asIndex.QuantType;
            buildExplorationFactor = asIndex.BuildExplorationFactor;
            numLinks = asIndex.NumLinks;
            indexPtr = (nint)asIndex.IndexPtr;
            processInstanceId = asIndex.ProcessInstanceId;

            Debug.Assert((context % ContextMetadata.ContextStep) == 0, $"Context ({context}) not as expected (% 4 == {context % 4}), vector set index is probably corrupted");
        }

        /// <summary>
        /// Add a vector to a vector set encoded by <paramref name="indexValue"/>.
        /// 
        /// Assumes that the index is locked in the Tsavorite store.
        /// </summary>
        /// <returns>Result of the operation.</returns>
        internal VectorManagerResult TryAdd(
            scoped ReadOnlySpan<byte> indexValue,
            ReadOnlySpan<byte> element,
            VectorValueType valueType,
            ReadOnlySpan<byte> values,
            ReadOnlySpan<byte> attributes,
            uint providedReduceDims,
            VectorQuantType providedQuantType,
            uint providedBuildExplorationFactor,
            uint providedNumLinks,
            out ReadOnlySpan<byte> errorMsg
        )
        {
            AssertHaveStorageSession();

            errorMsg = default;

            ReadIndex(indexValue, out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var indexPtr, out _);

            var valueDims = CalculateValueDimensions(valueType, values);

            if (dimensions != valueDims)
            {
                // Matching Redis behavior
                errorMsg = Encoding.ASCII.GetBytes($"ERR Vector dimension mismatch - got {valueDims} but set has {dimensions}");
                return VectorManagerResult.BadParams;
            }

            if (providedReduceDims == 0 && reduceDims != 0)
            {
                // Matching Redis behavior, which is definitely a bit weird here
                errorMsg = Encoding.ASCII.GetBytes($"ERR Vector dimension mismatch - got {valueDims} but set has {reduceDims}");
                return VectorManagerResult.BadParams;
            }
            else if (providedReduceDims != 0 && providedReduceDims != reduceDims)
            {
                return VectorManagerResult.BadParams;
            }

            if (providedQuantType != VectorQuantType.Invalid && providedQuantType != quantType)
            {
                return VectorManagerResult.BadParams;
            }

            if (providedNumLinks != numLinks)
            {
                // Matching Redis behavior
                errorMsg = "ERR asked M value mismatch with existing vector set"u8;
                return VectorManagerResult.BadParams;
            }

            if (quantType == VectorQuantType.XPreQ8 && element.Length != sizeof(uint))
            {
                errorMsg = "ERR XPREQ8 requires 4-byte element ids"u8;
                return VectorManagerResult.BadParams;
            }

            var insert =
                Service.Insert(
                    context,
                    indexPtr,
                    element,
                    valueType,
                    values,
                    attributes
                );

            if (insert)
            {
                return VectorManagerResult.OK;
            }

            return VectorManagerResult.Duplicate;
        }

        internal VectorManagerResult TryRemove(ReadOnlySpan<byte> indexValue, ReadOnlySpan<byte> element)
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out _, out _, out var quantType, out _, out _, out var indexPtr, out _);

            if (quantType == VectorQuantType.XPreQ8 && element.Length != sizeof(int))
            {
                // We know this element isn't present because of other validation constraints, bail
                return VectorManagerResult.MissingElement;
            }

            var del = Service.Remove(context, indexPtr, element);

            return del ? VectorManagerResult.OK : VectorManagerResult.MissingElement;
        }

        /// <summary>
        /// Deletion of a Vector Set needs special handling.
        /// 
        /// This is called by DEL and UNLINK after a naive delete fails for us to _try_ and delete a Vector Set.
        /// </summary>
        internal Status TryDeleteVectorSet(StorageSession storageSession, ref SpanByte key)
        {
            storageSession.parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(key.AsReadOnlySpan()));

            var input = new RawStringInput(RespCommand.VADD, ref storageSession.parseState);

            Span<byte> indexSpan = stackalloc byte[IndexSizeBytes];

            Span<TxnKeyEntry> exclusiveLocks = stackalloc TxnKeyEntry[readLockShardCount];

            using (ReadForDeleteVectorIndex(storageSession, ref key, ref input, indexSpan, exclusiveLocks, out var status))
            {
                if (status != GarnetStatus.OK)
                {
                    // This can happen is something else successfully deleted before we acquired the lock
                    return Status.CreateNotFound();
                }

                DropIndex(indexSpan);

                // Update the index to be delete-able
                var updateToDroppableVectorSet = new RawStringInput();
                updateToDroppableVectorSet.arg1 = DeleteAfterDropArg;
                updateToDroppableVectorSet.header.cmd = RespCommand.VADD;

                var update = storageSession.basicContext.RMW(ref key, ref updateToDroppableVectorSet);
                if (!update.IsCompletedSuccessfully)
                {
                    throw new GarnetException("Failed to make Vector Set delete-able, this should never happen but will leave vector sets corrupted");
                }

                // Actually delete the value
                var del = storageSession.basicContext.Delete(ref key);
                if (!del.IsCompletedSuccessfully)
                {
                    throw new GarnetException("Failed to delete dropped Vector Set, this should never happen but will leave vector sets corrupted");
                }

                // Cleanup incidental additional state
                DropVectorSetReplicationKey(key, ref storageSession.basicContext);

                CleanupDroppedIndex(ref storageSession.vectorContext, indexSpan);

                return Status.CreateFound();
            }
        }

        /// <summary>
        /// Perform a similarity search given a vector to compare against.
        /// </summary>
        internal VectorManagerResult ValueSimilarity(
            ReadOnlySpan<byte> indexValue,
            VectorValueType valueType,
            ReadOnlySpan<byte> values,
            int count,
            float delta,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            int maxFilteringEffort,
            bool includeAttributes,
            ref SpanByteAndMemory outputIds,
            out VectorIdFormat outputIdFormat,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes
        )
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out var dimensions, out var reduceDims, out var quantType, out var buildExplorationFactor, out var numLinks, out var indexPtr, out _);

            var valueDims = CalculateValueDimensions(valueType, values);
            if (dimensions != valueDims)
            {
                outputIdFormat = VectorIdFormat.Invalid;
                return VectorManagerResult.BadParams;
            }

            // No point in asking for more data than the effort we'll put in
            if (count > searchExplorationFactor)
            {
                count = searchExplorationFactor;
            }

            // Make sure enough space in distances for requested count
            if (count > outputDistances.Length)
            {
                if (!outputDistances.IsSpanByte)
                {
                    outputDistances.Memory.Dispose();
                }

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * sizeof(float)));
            }

            // Indicate requested # of matches
            outputDistances.Length = count * sizeof(float);

            // If we're fairly sure the ids won't fit, go ahead and grab more memory now
            //
            // If we're still wrong, we'll end up using continuation callbacks which have more overhead
            if (count * MinimumSpacePerId > outputIds.Length)
            {
                if (!outputIds.IsSpanByte)
                {
                    outputIds.Memory.Dispose();
                }

                outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * MinimumSpacePerId));
            }

            var found =
                Service.SearchVector(
                    context,
                    indexPtr,
                    valueType,
                    values,
                    delta,
                    searchExplorationFactor,
                    filter,
                    maxFilteringEffort,
                    outputIds.AsSpan(),
                    MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan()),
                    out var continuation
                );

            if (found < 0)
            {
                logger?.LogWarning("Error indicating response from vector service {0}", found);
                outputIdFormat = VectorIdFormat.Invalid;
                return VectorManagerResult.BadParams;
            }

            if (includeAttributes)
            {
                FetchVectorElementAttributes(context, found, outputIds, ref outputAttributes);
            }

            if (continuation != 0)
            {
                // TODO: paged results!
                throw new NotImplementedException();
            }

            outputDistances.Length = sizeof(float) * found;

            // Default assumption is length prefixed
            outputIdFormat = VectorIdFormat.I32LengthPrefixed;

            if (quantType == VectorQuantType.XPreQ8)
            {
                // But in this special case, we force them to be 4-byte ids
                //outputIdFormat = VectorIdFormat.FixedI32;
                outputIdFormat = VectorIdFormat.I32LengthPrefixed;
            }

            return VectorManagerResult.OK;
        }

        /// <summary>
        /// Perform a similarity search given a vector to compare against.
        /// </summary>
        internal VectorManagerResult ElementSimilarity(
            ReadOnlySpan<byte> indexValue,
            ReadOnlySpan<byte> element,
            int count,
            float delta,
            int searchExplorationFactor,
            ReadOnlySpan<byte> filter,
            int maxFilteringEffort,
            bool includeAttributes,
            ref SpanByteAndMemory outputIds,
            out VectorIdFormat outputIdFormat,
            ref SpanByteAndMemory outputDistances,
            ref SpanByteAndMemory outputAttributes
        )
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out _, out _, out var quantType, out _, out _, out var indexPtr, out _);

            // No point in asking for more data than the effort we'll put in
            if (count > searchExplorationFactor)
            {
                count = searchExplorationFactor;
            }

            // Make sure enough space in distances for requested count
            if (count * sizeof(float) > outputDistances.Length)
            {
                if (!outputDistances.IsSpanByte)
                {
                    outputDistances.Memory.Dispose();
                }

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * sizeof(float)));
            }

            // Indicate requested # of matches
            outputDistances.Length = count * sizeof(float);

            // If we're fairly sure the ids won't fit, go ahead and grab more memory now
            //
            // If we're still wrong, we'll end up using continuation callbacks which have more overhead
            if (count * MinimumSpacePerId > outputIds.Length)
            {
                if (!outputIds.IsSpanByte)
                {
                    outputIds.Memory.Dispose();
                }

                outputIds = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent(count * MinimumSpacePerId));
            }

            var found =
                Service.SearchElement(
                    context,
                    indexPtr,
                    element,
                    delta,
                    searchExplorationFactor,
                    filter,
                    maxFilteringEffort,
                    outputIds.AsSpan(),
                    MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan()),
                    out var continuation
                );

            if (found < 0)
            {
                logger?.LogWarning("Error indicating response from vector service {0}", found);
                outputIdFormat = VectorIdFormat.Invalid;
                return VectorManagerResult.BadParams;
            }

            if (includeAttributes)
            {
                FetchVectorElementAttributes(context, found, outputIds, ref outputAttributes);
            }

            if (continuation != 0)
            {
                // TODO: paged results!
                throw new NotImplementedException();
            }

            outputDistances.Length = sizeof(float) * found;

            // Default assumption is length prefixed
            outputIdFormat = VectorIdFormat.I32LengthPrefixed;

            if (quantType == VectorQuantType.XPreQ8)
            {
                // But in this special case, we force them to be 4-byte ids
                //outputIdFormat = VectorIdFormat.FixedI32;
                outputIdFormat = VectorIdFormat.I32LengthPrefixed;
            }

            return VectorManagerResult.OK;
        }


        /// <summary>
        /// Fetch attributes for a given set of element ids.
        /// 
        /// This must only be called while holding locks which prevent the Vector Set from being dropped.
        /// </summary>
        private void FetchVectorElementAttributes(ulong context, int numIds, SpanByteAndMemory ids, ref SpanByteAndMemory attributes)
        {
            var remainingIds = ids.AsReadOnlySpan();

            GCHandle idPin = default;
            byte[] idWithNamespaceArr = null;

            var attributesNextIx = 0;

            Span<byte> attributeFull = stackalloc byte[32];
            var attributeMem = SpanByteAndMemory.FromPinnedSpan(attributeFull);

            try
            {
                Span<byte> idWithNamespace = stackalloc byte[128];

                // TODO: we could scatter/gather this like MGET - doesn't matter when everything is in memory,
                //       but if anything is on disk it'd help perf
                for (var i = 0; i < numIds; i++)
                {
                    var idLen = BinaryPrimitives.ReadInt32LittleEndian(remainingIds);
                    if (idLen + sizeof(int) > remainingIds.Length)
                    {
                        throw new GarnetException($"Malformed ids, {idLen} + {sizeof(int)} > {remainingIds.Length}");
                    }

                    var id = remainingIds.Slice(sizeof(int), idLen);

                    // Make sure we've got enough space to query the element
                    if (id.Length + 1 > idWithNamespace.Length)
                    {
                        if (idWithNamespaceArr != null)
                        {
                            idPin.Free();
                            ArrayPool<byte>.Shared.Return(idWithNamespaceArr);
                        }

                        idWithNamespaceArr = ArrayPool<byte>.Shared.Rent(id.Length + 1);
                        idPin = GCHandle.Alloc(idWithNamespaceArr, GCHandleType.Pinned);
                        idWithNamespace = idWithNamespaceArr;
                    }

                    if (attributeMem.Memory != null)
                    {
                        attributeMem.Length = attributeMem.Memory.Memory.Length;
                    }
                    else
                    {
                        attributeMem.Length = attributeMem.SpanByte.Length;
                    }

                    var found = ReadSizeUnknown(context | DiskANNService.Attributes, id, ref attributeMem);

                    // Copy attribute into output buffer, length prefixed, resizing as necessary
                    var neededSpace = 4 + (found ? attributeMem.Length : 0);

                    var destSpan = attributes.AsSpan()[attributesNextIx..];
                    if (destSpan.Length < neededSpace)
                    {
                        var newAttrArr = MemoryPool<byte>.Shared.Rent(attributes.Length + neededSpace);
                        attributes.AsReadOnlySpan().CopyTo(newAttrArr.Memory.Span);

                        attributes.Memory?.Dispose();

                        attributes = new SpanByteAndMemory(newAttrArr, newAttrArr.Memory.Length);
                        destSpan = attributes.AsSpan()[attributesNextIx..];
                    }

                    BinaryPrimitives.WriteInt32LittleEndian(destSpan, attributeMem.Length);
                    attributeMem.AsReadOnlySpan().CopyTo(destSpan[sizeof(int)..]);

                    attributesNextIx += neededSpace;

                    remainingIds = remainingIds[(sizeof(int) + idLen)..];
                }

                attributes.Length = attributesNextIx;
            }
            finally
            {
                if (idWithNamespaceArr != null)
                {
                    idPin.Free();
                    ArrayPool<byte>.Shared.Return(idWithNamespaceArr);
                }

                attributeMem.Memory?.Dispose();
            }
        }

        internal bool TryGetEmbedding(ReadOnlySpan<byte> indexValue, ReadOnlySpan<byte> element, ref SpanByteAndMemory outputDistances)
        {
            AssertHaveStorageSession();

            ReadIndex(indexValue, out var context, out var dimensions, out _, out _, out _, out _, out var indexPtr, out _);

            // Make sure enough space in distances for requested count
            if (dimensions * sizeof(float) > outputDistances.Length)
            {
                if (!outputDistances.IsSpanByte)
                {
                    outputDistances.Memory.Dispose();
                }

                outputDistances = new SpanByteAndMemory(MemoryPool<byte>.Shared.Rent((int)dimensions * sizeof(float)), (int)dimensions * sizeof(float));
            }
            else
            {
                outputDistances.Length = (int)dimensions * sizeof(float);
            }

            Span<byte> asBytesSpan = stackalloc byte[(int)dimensions];
            var asBytes = SpanByteAndMemory.FromPinnedSpan(asBytesSpan);
            try
            {
                if (!ReadSizeUnknown(context | DiskANNService.FullVector, element, ref asBytes))
                {
                    return false;
                }

                var from = asBytes.AsReadOnlySpan();
                var into = MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan());

                for (var i = 0; i < asBytes.Length; i++)
                {
                    into[i] = from[i];
                }

                return true;
            }
            finally
            {
                asBytes.Memory?.Dispose();
            }

            // TODO: DiskANN will need to do this long term, since different quantizers may behave differently

            //return
            //    Service.TryGetEmbedding(
            //        context,
            //        indexPtr,
            //        element,
            //        MemoryMarshal.Cast<byte, float>(outputDistances.AsSpan())
            //    );
        }

        /// <summary>
        /// For replication purposes, we need a write against the main log.
        /// 
        /// But we don't actually want to do the (expensive) vector ops as part of a write.
        /// 
        /// So this fakes up a modify operation that we can then intercept as part of replication.
        /// 
        /// This the Primary part, on a Replica <see cref="HandleVectorSetAddReplication"/> runs.
        /// </summary>
        internal void ReplicateVectorSetAdd<TContext>(ref SpanByte key, ref RawStringInput input, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            Debug.Assert(input.header.cmd == RespCommand.VADD, "Shouldn't be called with anything but VADD inputs");

            var inputCopy = input;
            inputCopy.arg1 = VectorManager.VADDAppendLogArg;

            Span<byte> keyWithNamespaceBytes = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(keyWithNamespaceBytes);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload(0);
            key.AsReadOnlySpan().CopyTo(keyWithNamespace.AsSpan());

            Span<byte> dummyBytes = stackalloc byte[4];
            var dummy = SpanByteAndMemory.FromPinnedSpan(dummyBytes);

            var res = context.RMW(ref keyWithNamespace, ref inputCopy, ref dummy);

            if (res.IsPending)
            {
                CompletePending(ref res, ref dummy, ref context);
            }

            if (!res.IsCompletedSuccessfully)
            {
                logger?.LogCritical("Failed to inject replication write for VADD into log, result was {0}", res);
                throw new GarnetException("Couldn't synthesize Vector Set add operation for replication, data loss will occur");
            }

            // Helper to complete read/writes during vector set synthetic op goes async
            static void CompletePending(ref Status status, ref SpanByteAndMemory output, ref TContext context)
            {
                _ = context.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                var more = completedOutputs.Next();
                Debug.Assert(more);
                status = completedOutputs.Current.Status;
                output = completedOutputs.Current.Output;
                more = completedOutputs.Next();
                Debug.Assert(!more);
                completedOutputs.Dispose();
            }
        }

        /// <summary>
        /// For replication purposes, we need a write against the main log.
        /// 
        /// But we don't actually want to do the (expensive) vector ops as part of a write.
        /// 
        /// So this fakes up a modify operation that we can then intercept as part of replication.
        /// 
        /// This the Primary part, on a Replica <see cref="HandleVectorSetRemoveReplication"/> runs.
        /// </summary>
        internal void ReplicateVectorSetRemove<TContext>(ref SpanByte key, ref SpanByte element, ref RawStringInput input, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            Debug.Assert(input.header.cmd == RespCommand.VREM, "Shouldn't be called with anything but VREM inputs");

            var inputCopy = input;
            inputCopy.arg1 = VectorManager.VREMAppendLogArg;

            Span<byte> keyWithNamespaceBytes = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(keyWithNamespaceBytes);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload(0);
            key.AsReadOnlySpan().CopyTo(keyWithNamespace.AsSpan());

            Span<byte> dummyBytes = stackalloc byte[4];
            var dummy = SpanByteAndMemory.FromPinnedSpan(dummyBytes);

            inputCopy.parseState.InitializeWithArgument(ArgSlice.FromPinnedSpan(element.AsReadOnlySpan()));

            var res = context.RMW(ref keyWithNamespace, ref inputCopy, ref dummy);

            if (res.IsPending)
            {
                CompletePending(ref res, ref dummy, ref context);
            }

            if (!res.IsCompletedSuccessfully)
            {
                logger?.LogCritical("Failed to inject replication write for VREM into log, result was {res}", res);
                throw new GarnetException("Couldn't synthesize Vector Set remove operation for replication, data loss will occur");
            }

            // Helper to complete read/writes during vector set synthetic op goes async
            static void CompletePending(ref Status status, ref SpanByteAndMemory output, ref TContext context)
            {
                _ = context.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                var more = completedOutputs.Next();
                Debug.Assert(more);
                status = completedOutputs.Current.Status;
                output = completedOutputs.Current.Output;
                more = completedOutputs.Next();
                Debug.Assert(!more);
                completedOutputs.Dispose();
            }
        }

        /// <summary>
        /// After an index is dropped, called to cleanup state injected by <see cref="ReplicateVectorSetAdd"/>
        /// 
        /// Amounts to delete a synthetic key in namespace 0.
        /// </summary>
        internal void DropVectorSetReplicationKey<TContext>(SpanByte key, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            Span<byte> keyWithNamespaceBytes = stackalloc byte[key.Length + 1];
            var keyWithNamespace = SpanByte.FromPinnedSpan(keyWithNamespaceBytes);
            keyWithNamespace.MarkNamespace();
            keyWithNamespace.SetNamespaceInPayload(0);
            key.AsReadOnlySpan().CopyTo(keyWithNamespace.AsSpan());

            Span<byte> dummyBytes = stackalloc byte[4];
            var dummy = SpanByteAndMemory.FromPinnedSpan(dummyBytes);

            var res = context.Delete(ref keyWithNamespace);

            if (res.IsPending)
            {
                CompletePending(ref res, ref context);
            }

            if (!res.IsCompletedSuccessfully)
            {
                throw new GarnetException("Couldn't synthesize Vector Set add operation for replication, data loss will occur");
            }

            // Helper to complete read/writes during vector set synthetic op goes async
            static void CompletePending(ref Status status, ref TContext context)
            {
                _ = context.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                var more = completedOutputs.Next();
                Debug.Assert(more);
                status = completedOutputs.Current.Status;
                more = completedOutputs.Next();
                Debug.Assert(!more);
                completedOutputs.Dispose();
            }
        }

        /// <summary>
        /// After an index is dropped, called to start the process of removing ancillary data (elements, neighbor lists, attributes, etc.).
        /// </summary>
        internal void CleanupDroppedIndex<TContext>(ref TContext ctx, ReadOnlySpan<byte> index)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            ReadIndex(index, out var context, out _, out _, out _, out _, out _, out _, out _);

            lock (this)
            {
                contextMetadata.MarkCleaningUp(context);
            }

            UpdateContextMetadata(ref ctx);

            // Wake up cleanup task
            var writeRes = cleanupTaskChannel.Writer.TryWrite(null);
            Debug.Assert(writeRes, "Request for cleanup failed, this should never happen");
        }

        /// <summary>
        /// Vector Set adds are phrased as reads (once the index is created), so they require special handling.
        /// 
        /// Operations that are faked up by <see cref="ReplicateVectorSetAdd"/> running on the Primary get diverted here on a Replica.
        /// </summary>
        internal void HandleVectorSetAddReplication(Func<RespServerSession> obtainServerSession, ref SpanByte keyWithNamespace, ref RawStringInput input)
        {
            // Undo mangling that got replication going
            var inputCopy = input;
            inputCopy.arg1 = default;
            var keyBytesArr = ArrayPool<byte>.Shared.Rent(keyWithNamespace.Length - 1);
            var keyBytes = keyBytesArr.AsMemory()[..(keyWithNamespace.Length - 1)];

            keyWithNamespace.AsReadOnlySpan().CopyTo(keyBytes.Span);

            var dims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(0).Span);
            var reduceDims = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(1).Span);
            var valueType = MemoryMarshal.Read<VectorValueType>(input.parseState.GetArgSliceByRef(2).Span);
            var values = input.parseState.GetArgSliceByRef(3).Span;
            var element = input.parseState.GetArgSliceByRef(4).Span;
            var quantizer = MemoryMarshal.Read<VectorQuantType>(input.parseState.GetArgSliceByRef(5).Span);
            var buildExplorationFactor = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(6).Span);
            var attributes = input.parseState.GetArgSliceByRef(7).Span;
            var numLinks = MemoryMarshal.Read<uint>(input.parseState.GetArgSliceByRef(8).Span);

            // We have to make copies (and they need to be on the heap) to pass to background tasks
            var valuesBytes = ArrayPool<byte>.Shared.Rent(values.Length).AsMemory()[..values.Length];
            values.CopyTo(valuesBytes.Span);

            var elementBytes = ArrayPool<byte>.Shared.Rent(element.Length).AsMemory()[..element.Length];
            element.CopyTo(elementBytes.Span);

            var attributesBytes = ArrayPool<byte>.Shared.Rent(attributes.Length).AsMemory()[..attributes.Length];
            attributes.CopyTo(attributesBytes.Span);

            // Spin up replication replay tasks on first use
            if (replicationReplayStarted == 0)
            {
                if (Interlocked.CompareExchange(ref replicationReplayStarted, 1, 0) == 0)
                {
                    StartReplicationReplayTasks(this, obtainServerSession);
                }
            }

            // We need a running count of pending VADDs so WaitForVectorOperationsToComplete can work
            _ = Interlocked.Increment(ref replicationReplayPendingVAdds);
            replicationBlockEvent.Reset();
            var queued = replicationReplayChannel.Writer.TryWrite(new(keyBytes, dims, reduceDims, valueType, valuesBytes, elementBytes, quantizer, buildExplorationFactor, attributesBytes, numLinks));
            if (!queued)
            {
                // Can occur if we're being Disposed
                var pending = Interlocked.Decrement(ref replicationReplayPendingVAdds);
                if (pending == 0)
                {
                    replicationBlockEvent.Set();
                }
            }

            static void StartReplicationReplayTasks(VectorManager self, Func<RespServerSession> obtainServerSession)
            {
                self.logger?.LogInformation("Starting {0} replication tasks for VADDs", self.replicationReplayTasks.Length);

                for (var i = 0; i < self.replicationReplayTasks.Length; i++)
                {
                    self.replicationReplayTasks[i] = Task.Factory.StartNew(
                        async () =>
                        {
                            try
                            {
                                var reader = self.replicationReplayChannel.Reader;

                                using var session = obtainServerSession();

                                SessionParseState reusableParseState = default;
                                reusableParseState.Initialize(9);

                                await foreach (var entry in reader.ReadAllAsync())
                                {
                                    try
                                    {
                                        try
                                        {
                                            ApplyVectorSetAdd(self, session.storageSession, entry, ref reusableParseState);
                                        }
                                        finally
                                        {
                                            var pending = Interlocked.Decrement(ref self.replicationReplayPendingVAdds);
                                            Debug.Assert(pending >= 0, "Pending VADD ops has fallen below 0 after processing op");

                                            if (pending == 0)
                                            {
                                                self.replicationBlockEvent.Set();
                                            }
                                        }
                                    }
                                    catch
                                    {
                                        self.logger?.LogCritical(
                                            "Faulting ApplyVectorSetAdd ({key}, {dims}, {reducedDims}, {valueType}, 0x{values}, 0x{element}, {quantizer}, {bef}, {attributes}, {numLinks}",
                                            Encoding.UTF8.GetString(entry.Key.Span),
                                            entry.Dims,
                                            entry.ReduceDims,
                                            entry.ValueType,
                                            Convert.ToBase64String(entry.Values.Span),
                                            Convert.ToBase64String(entry.Values.Span),
                                            entry.Quantizer,
                                            entry.BuildExplorationFactor,
                                            Encoding.UTF8.GetString(entry.Attributes.Span),
                                            entry.NumLinks
                                        );

                                        throw;
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                self.logger?.LogCritical(e, "Unexpected abort of replication replay task");
                                throw;
                            }
                        }
                    );
                }
            }

            // Actually apply a replicated VADD
            static unsafe void ApplyVectorSetAdd(VectorManager self, StorageSession storageSession, VADDReplicationState state, ref SessionParseState reusableParseState)
            {
                ref var context = ref storageSession.basicContext;

                var (keyBytes, dims, reduceDims, valueType, valuesBytes, elementBytes, quantizer, buildExplorationFactor, attributesBytes, numLinks) = state;
                try
                {
                    Span<byte> indexSpan = stackalloc byte[IndexSizeBytes];

                    fixed (byte* keyPtr = keyBytes.Span)
                    fixed (byte* valuesPtr = valuesBytes.Span)
                    fixed (byte* elementPtr = elementBytes.Span)
                    fixed (byte* attributesPtr = attributesBytes.Span)
                    {
                        var key = SpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
                        var values = SpanByte.FromPinnedPointer(valuesPtr, valuesBytes.Length);
                        var element = SpanByte.FromPinnedPointer(elementPtr, elementBytes.Length);
                        var attributes = SpanByte.FromPinnedPointer(attributesPtr, attributesBytes.Length);

                        var indexBytes = stackalloc byte[IndexSizeBytes];
                        SpanByteAndMemory indexConfig = new(indexBytes, IndexSizeBytes);

                        var dimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref dims, 1)));
                        var reduceDimsArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref reduceDims, 1)));
                        var valueTypeArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorValueType, byte>(MemoryMarshal.CreateSpan(ref valueType, 1)));
                        var valuesArg = ArgSlice.FromPinnedSpan(values.AsReadOnlySpan());
                        var elementArg = ArgSlice.FromPinnedSpan(element.AsReadOnlySpan());
                        var quantizerArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<VectorQuantType, byte>(MemoryMarshal.CreateSpan(ref quantizer, 1)));
                        var buildExplorationFactorArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref buildExplorationFactor, 1)));
                        var attributesArg = ArgSlice.FromPinnedSpan(attributes.AsReadOnlySpan());
                        var numLinksArg = ArgSlice.FromPinnedSpan(MemoryMarshal.Cast<uint, byte>(MemoryMarshal.CreateSpan(ref numLinks, 1)));

                        reusableParseState.InitializeWithArguments([dimsArg, reduceDimsArg, valueTypeArg, valuesArg, elementArg, quantizerArg, buildExplorationFactorArg, attributesArg, numLinksArg]);

                        var input = new RawStringInput(RespCommand.VADD, ref reusableParseState);

                        // Equivalent to VectorStoreOps.VectorSetAdd
                        //
                        // We still need locking here because the replays may proceed in parallel

                        using (self.ReadOrCreateVectorIndex(storageSession, ref key, ref input, indexSpan, out var status))
                        {
                            Debug.Assert(status == GarnetStatus.OK, "Replication should only occur when an add is successful, so index must exist");

                            var addRes = self.TryAdd(indexSpan, element.AsReadOnlySpan(), valueType, values.AsReadOnlySpan(), attributes.AsReadOnlySpan(), reduceDims, quantizer, buildExplorationFactor, numLinks, out _);

                            if (addRes != VectorManagerResult.OK)
                            {
                                throw new GarnetException("Failed to add to vector set index during AOF sync, this should never happen but will cause data loss if it does");
                            }
                        }
                    }
                }
                finally
                {
                    if (MemoryMarshal.TryGetArray<byte>(keyBytes, out var toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }

                    if (MemoryMarshal.TryGetArray(valuesBytes, out toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }

                    if (MemoryMarshal.TryGetArray(elementBytes, out toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }

                    if (MemoryMarshal.TryGetArray(attributesBytes, out toFree))
                    {
                        ArrayPool<byte>.Shared.Return(toFree.Array);
                    }
                }
            }
        }

        /// <summary>
        /// Vector Set removes are phrased as reads (once the index is created), so they require special handling.
        /// 
        /// Operations that are faked up by <see cref="ReplicateVectorSetRemove"/> running on the Primary get diverted here on a Replica.
        /// </summary>
        internal void HandleVectorSetRemoveReplication(StorageSession storageSession, ref SpanByte key, ref RawStringInput input)
        {
            Span<byte> indexSpan = stackalloc byte[IndexSizeBytes];
            var element = input.parseState.GetArgSliceByRef(0);

            // Replication adds a (0) namespace - remove it
            Span<byte> keyWithoutNamespaceSpan = stackalloc byte[key.Length - 1];
            key.AsReadOnlySpan().CopyTo(keyWithoutNamespaceSpan);
            var keyWithoutNamespace = SpanByte.FromPinnedSpan(keyWithoutNamespaceSpan);

            var inputCopy = input;
            inputCopy.arg1 = default;

            using (ReadVectorIndex(storageSession, ref keyWithoutNamespace, ref inputCopy, indexSpan, out var status))
            {
                Debug.Assert(status == GarnetStatus.OK, "Replication should only occur when a remove is successful, so index must exist");

                var addRes = TryRemove(indexSpan, element.ReadOnlySpan);

                if (addRes != VectorManagerResult.OK)
                {
                    throw new GarnetException("Failed to remove from vector set index during AOF sync, this should never happen but will cause data loss if it does");
                }
            }
        }

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

                    input.header.cmd = RespCommand.VADD;
                    input.arg1 = RecreateIndexArg;

                    GarnetStatus writeRes;
                    try
                    {
                        writeRes = storageSession.RMW_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
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
        internal ReadVectorLock ReadOrCreateVectorIndex(StorageSession storageSession, ref SpanByte key, ref RawStringInput input, scoped Span<byte> indexSpan, out GarnetStatus status)
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

                    if (needsRecreate)
                    {
                        input.arg1 = RecreateIndexArg;
                    }

                    GarnetStatus writeRes;
                    try
                    {
                        writeRes = storageSession.RMW_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);

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
        /// Utility method that will read vector set index out, and acquire exclusive locks to allow it to be deleted.
        /// </summary>
        internal DeleteVectorLock ReadForDeleteVectorIndex(StorageSession storageSession, ref SpanByte key, ref RawStringInput input, scoped Span<byte> indexSpan, Span<TxnKeyEntry> exclusiveLocks, out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length == IndexSizeBytes, "Insufficient space for index");
            Debug.Assert(exclusiveLocks.Length == readLockShardCount, "Insufficient space for exclusive locks");

            Debug.Assert(ActiveThreadSession == null, "Shouldn't enter context when already in one");
            ActiveThreadSession = storageSession;

            var keyHash = storageSession.lockableContext.GetKeyHash(key);

            for (var i = 0; i < exclusiveLocks.Length; i++)
            {
                exclusiveLocks[i].isObject = false;
                exclusiveLocks[i].lockType = LockType.Exclusive;
                exclusiveLocks[i].keyHash = (keyHash & ~readLockShardMask) | (long)i;
            }

            var indexConfig = SpanByteAndMemory.FromPinnedSpan(indexSpan);

            ref var lockCtx = ref storageSession.objectStoreLockableContext;
            lockCtx.BeginLockable();

            lockCtx.Lock<TxnKeyEntry>(exclusiveLocks);

            // Get the index
            try
            {
                status = storageSession.Read_MainStore(ref key, ref input, ref indexConfig, ref storageSession.basicContext);
            }
            catch
            {
                lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                lockCtx.EndLockable();

                throw;
            }

            if (status != GarnetStatus.OK)
            {
                // This can happen is something else successfully deleted before we acquired the lock

                lockCtx.Unlock<TxnKeyEntry>(exclusiveLocks);
                lockCtx.EndLockable();
                return default;
            }

            return new(ref lockCtx, exclusiveLocks);
        }

        /// <summary>
        /// Wait until all ops passed to <see cref="HandleVectorSetAddReplication"/> have completed.
        /// </summary>
        public void WaitForVectorOperationsToComplete()
        {
            try
            {
                replicationBlockEvent.Wait();
            }
            catch (ObjectDisposedException)
            {
                // This is possible during dispose
                //
                // Dispose already takes pains to drain everything before disposing, so this is safe to ignore
            }
        }

        private async Task RunCleanupTaskAsync()
        {
            // Each drop index will queue a null object here
            // We'll handle multiple at once if possible, but using a channel simplifies cancellation and dispose
            await foreach (var ignored in cleanupTaskChannel.Reader.ReadAllAsync())
            {
                try
                {
                    HashSet<ulong> needCleanup;
                    lock (this)
                    {
                        needCleanup = contextMetadata.GetNeedCleanup();
                    }

                    if (needCleanup == null)
                    {
                        // Previous run already got here, so bail
                        continue;
                    }

                    // TODO: this doesn't work with multi-db setups
                    // TODO: this doesn't work with non-RESP impls... which maybe we don't care about?
                    using var cleanupSession = (RespServerSession)getCleanupSession();

                    PostDropCleanupFunctions callbacks = new(cleanupSession.storageSession, needCleanup);

                    ref var ctx = ref cleanupSession.storageSession.vectorContext;

                    // Scan whole keyspace (sigh) and remove any associated data
                    //
                    // We don't really have a choice here, just do it
                    _ = ctx.Session.Iterate(ref callbacks);

                    lock (this)
                    {
                        foreach (var cleanedUp in needCleanup)
                        {
                            contextMetadata.FinishedCleaningUp(cleanedUp);
                        }
                    }

                    UpdateContextMetadata(ref ctx);
                }
                catch (Exception e)
                {
                    logger?.LogError(e, "Failure during background cleanup of deleted vector sets, implies storage leak");
                }
            }
        }

        private void PrepareReadLockHash(StorageSession storageSession, ref SpanByte key, out long keyHash, out long readLockHash)
        {
            var id = Thread.GetCurrentProcessorId() & readLockShardMask;

            keyHash = storageSession.basicContext.GetKeyHash(ref key);
            readLockHash = (keyHash & ~readLockShardMask) | id;
        }

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

        /// <summary>
        /// Helper to complete read/writes during vector set op replay that go async.
        /// </summary>
        private static void CompletePending(ref Status status, ref SpanByteAndMemory output, ref BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> context)
        {
            _ = context.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            var more = completedOutputs.Next();
            Debug.Assert(more);
            status = completedOutputs.Current.Status;
            output = completedOutputs.Current.Output;
            more = completedOutputs.Next();
            Debug.Assert(!more);
            completedOutputs.Dispose();
        }

        /// <summary>
        /// Determine the dimensions of a vector given its <see cref="VectorValueType"/> and its raw data.
        /// </summary>
        internal static uint CalculateValueDimensions(VectorValueType valueType, ReadOnlySpan<byte> values)
        {
            if (valueType == VectorValueType.FP32)
            {
                return (uint)(values.Length / sizeof(float));
            }
            else if (valueType == VectorValueType.XB8)
            {
                return (uint)(values.Length);
            }
            else
            {
                throw new NotImplementedException($"{valueType}");
            }
        }

        [Conditional("DEBUG")]
        private static void AssertHaveStorageSession()
        {
            Debug.Assert(ActiveThreadSession != null, "Should have StorageSession by now");
        }
    }
}