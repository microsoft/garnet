// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

    /// <summary>
    /// Methods related to cleaning up data after a Vector Set is deleted.
    /// </summary>
    public sealed partial class VectorManager
    {
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
                var pairedContext = (ulong)ns & ~(ContextStep - 1);
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

        private readonly Channel<object> cleanupTaskChannel;
        private readonly Task cleanupTask;
        private readonly Func<IMessageConsumer> getCleanupSession;

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

                    // TODO: this doesn't work with non-RESP impls... which maybe we don't care about?
                    using var cleanupSession = (RespServerSession)getCleanupSession();
                    if (cleanupSession.activeDbId != dbId && !cleanupSession.TrySwitchActiveDatabaseSession(dbId))
                    {
                        throw new GarnetException($"Could not switch VectorManager cleanup session to {dbId}, initialization failed");
                    }

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

        /// <summary>
        /// Called in response to <see cref="TryMarkDeleteInProgress{TContext}(ref TContext, ref SpanByte, ulong)"/> or <see cref="ClearDeleteInProgress{TContext}(ref TContext, ref SpanByte, ulong)"/> to update metadata in Tsavorite.
        /// </summary>
        internal static void UpdateInProgressDeletes(Span<byte> updateMessage, ref SpanByte inLogValue, ref RecordInfo recordInfo, ref RMWInfo rmwInfo)
        {
            var context = BinaryPrimitives.ReadUInt64LittleEndian(updateMessage);
            var len = BinaryPrimitives.ReadInt32LittleEndian(updateMessage[sizeof(ulong)..]);
            var isAdding = len > 0;
            var key = updateMessage[(sizeof(ulong) + sizeof(int))..];

            Debug.Assert(key.Length == (isAdding ? len : -len), "Key length not expected");
            Debug.Assert(context is >= ContextStep, "Special context not allowed");

            var remaining = inLogValue.AsSpan();
            while (!remaining.IsEmpty)
            {
                var curCtx = BinaryPrimitives.ReadUInt64LittleEndian(remaining);

                if (curCtx == 0)
                {
                    // Reached uninitialized data
                    break;
                }

                var curLen = BinaryPrimitives.ReadInt32LittleEndian(remaining[sizeof(ulong)..]);
                if (curCtx == context)
                {
                    if (isAdding)
                    {
                        // Already added, ignore and make no other changes
                        return;
                    }

                    // Copy later values to cover the one we're removing
                    var afterCur = remaining[(sizeof(ulong) + sizeof(int) + curLen)..];
                    afterCur.CopyTo(remaining);

                    // Clear everything after that so we won't think it's valid
                    remaining[^(sizeof(ulong) + sizeof(int) + curLen)..].Clear();

                    return;
                }

                remaining = remaining[(sizeof(ulong) + sizeof(int) + curLen)..];
            }

            // Not already added, so slap it in
            BinaryPrimitives.WriteUInt64LittleEndian(remaining, context);
            BinaryPrimitives.WriteInt32LittleEndian(remaining[sizeof(ulong)..], len);

            key.CopyTo(remaining[(sizeof(ulong) + sizeof(int))..]);
        }

        /// <summary>
        /// Before we start smashing a <see cref="Index"/> for deletion, records that we started to delete it so we can recover from crashes.
        /// </summary>
        internal bool TryMarkDeleteInProgress<TContext>(ref TContext ctx, ref SpanByte key, ulong context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            Span<byte> keySpan = stackalloc byte[2];

            Span<byte> dataSpan = stackalloc byte[sizeof(ulong) + sizeof(int) + key.Length];
            BinaryPrimitives.WriteUInt64LittleEndian(dataSpan, context);

            // Positive length indicates we're adding this to the list
            BinaryPrimitives.WriteInt32LittleEndian(dataSpan[sizeof(ulong)..], key.LengthWithoutMetadata);
            key.AsReadOnlySpan().CopyTo(dataSpan[(sizeof(ulong) + sizeof(int))..]);

            // 0:0 is ContextMetadata
            // 0:1 is InProgressDeletes
            var inProgressDeletesKey = SpanByte.FromPinnedSpan(keySpan);

            inProgressDeletesKey.MarkNamespace();
            inProgressDeletesKey.SetNamespaceInPayload(0);
            inProgressDeletesKey.AsSpan()[0] = 1;

            VectorInput input = default;
            input.Callback = 0;

            // Negative to indicate dynamic-ness
            input.WriteDesiredSize = -(sizeof(ulong) + sizeof(int) + key.Length);
            unsafe
            {
                input.CallbackContext = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dataSpan));
            }

            var status = ctx.RMW(ref inProgressDeletesKey, ref input);

            if (status.IsPending)
            {
                SpanByte ignored = default;
                CompletePending(ref status, ref ignored, ref ctx);
            }

            return status.IsCompletedSuccessfully;
        }

        /// <summary>
        /// Enumerate any deletes of Vector Sets that are in progress.
        /// 
        /// Used with <see cref="TryMarkDeleteInProgress{TContext}(ref TContext, ref SpanByte, ulong)"/> and <see cref="ClearDeleteInProgress{TContext}(ref TContext, ref SpanByte, ulong)"/> to recover from interrupted deletes.
        /// </summary>
        internal List<(ReadOnlyMemory<byte> Key, ulong Context)> GetInDeletesInProgress(StorageSession storageSession)
        {
            Span<byte> keySpan = stackalloc byte[1];

            // 0:1 is InProgressDeletes, but ReadSizeUnknown will attach the context for us
            var inProgressDeletesKey = SpanByte.FromPinnedSpan(keySpan);

            inProgressDeletesKey.AsSpan()[0] = 1;

            SpanByteAndMemory readValue = default;

            List<(ReadOnlyMemory<byte> Key, ulong Context)> ret = [];
            try
            {
                ActiveThreadSession = storageSession;
                try
                {
                    if (!ReadSizeUnknown(context: 0, keySpan, ref readValue))
                    {
                        return ret;
                    }
                }
                finally
                {
                    ActiveThreadSession = null;
                }

                var remaining = readValue.AsReadOnlySpan();
                while (!remaining.IsEmpty)
                {
                    var ctx = BinaryPrimitives.ReadUInt64LittleEndian(remaining);
                    if (ctx == 0)
                    {
                        // Encountered uninitialized data
                        break;
                    }

                    var len = BinaryPrimitives.ReadInt32LittleEndian(remaining[sizeof(ulong)..]);

                    var key = remaining.Slice(sizeof(ulong) + sizeof(uint), len);

                    ret.Add((key.ToArray(), ctx));

                    remaining = remaining[(sizeof(ulong) + sizeof(uint) + len)..];
                }

                return ret;
            }
            finally
            {
                readValue.Memory?.Dispose();
            }
        }

        /// <summary>
        /// After a delete has completed, removes the given key from metadata.
        /// </summary>
        internal void ClearDeleteInProgress<TContext>(ref TContext ctx, ref SpanByte key, ulong context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            Span<byte> keySpan = stackalloc byte[2];

            Span<byte> dataSpan = stackalloc byte[sizeof(ulong) + sizeof(int) + key.Length];
            BinaryPrimitives.WriteUInt64LittleEndian(dataSpan, context);

            // Negative length indicates we're adding this to the list
            BinaryPrimitives.WriteInt32LittleEndian(dataSpan[sizeof(ulong)..], -key.LengthWithoutMetadata);
            key.AsReadOnlySpan().CopyTo(dataSpan[(sizeof(ulong) + sizeof(int))..]);

            // 0:0 is ContextMetadata
            // 0:1 is InProgressDeletes
            var inProgressDeletesKey = SpanByte.FromPinnedSpan(keySpan);

            inProgressDeletesKey.MarkNamespace();
            inProgressDeletesKey.SetNamespaceInPayload(0);
            inProgressDeletesKey.AsSpan()[0] = 1;

            VectorInput input = default;
            input.Callback = 0;

            // Negative to indicate dynamic-ness
            input.WriteDesiredSize = -(sizeof(ulong) + sizeof(int) + key.Length);
            unsafe
            {
                input.CallbackContext = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dataSpan));
            }

            var status = ctx.RMW(ref inProgressDeletesKey, ref input);

            if (status.IsPending)
            {
                SpanByte ignored = default;
                CompletePending(ref status, ref ignored, ref ctx);
            }
        }

        /// <summary>
        /// After an index is dropped, called to start the process of removing ancillary data (elements, neighbor lists, attributes, etc.).
        /// </summary>
        internal void CleanupDroppedIndex<TContext>(ref TContext ctx, ulong context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
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
        /// Detects if a Vector Set index read out of the main store is in the middle of being deleted.
        /// </summary>
        private static bool PartiallyDeleted(ReadOnlySpan<byte> indexConfig)
        {
            ReadIndex(indexConfig, out var context, out _, out _, out _, out _, out _, out _, out _);
            return context == 0;
        }
    }
}