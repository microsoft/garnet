// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.networking;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;
    
    /// <summary>
    /// Methods related to cleaning up data after a Vector Set is deleted.
    /// </summary>
    public sealed partial class VectorManager
    {
        private readonly Channel<object> cleanupTaskChannel;
        private readonly Task cleanupTask;
        private readonly Func<IMessageConsumer> getCleanupSession;

        private async Task RunCleanupTaskAsync()
        {
            await Task.Yield();

            // TODO: Move implementation over
            //throw new NotImplementedException();
        }

        /// <summary>
        /// Called in response to <see cref="TryMarkDeleteInProgress"/> or <see cref="ClearDeleteInProgress"/> to update metadata in Tsavorite.
        /// 
        /// Returns false if there is insufficient size for the value.
        /// </summary>
        internal static bool TryUpdateInProgressDeletes(Span<byte> updateMessage, ref LogRecord recordInfo, ref RMWInfo rmwInfo)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Called in response to <see cref="TryMarkDeleteInProgress"/> or <see cref="ClearDeleteInProgress"/> to update metadata in Tsavorite.
        /// 
        /// Returns false if there is insufficient size for the value.
        /// </summary>
        internal static bool TryUpdateInProgressDeletes(Span<byte> updateMessage, ref LogRecord recordInfo, ref UpsertInfo upsertInfo)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Before we start smashing a <see cref="Index"/> for deletion, records that we started to delete it so we can recover from crashes.
        /// </summary>
        internal bool TryMarkDeleteInProgress(ref BasicContext<VectorInput, PinnedSpanByte, long, VectorSessionFunctions, StoreFunctions, StoreAllocator> ctx, ref PinnedSpanByte key, ulong context)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Enumerate any deletes of Vector Sets that are in progress.
        /// 
        /// Used with <see cref="TryMarkDeleteInProgress"/> and <see cref="ClearDeleteInProgress"/> to recover from interrupted deletes.
        /// </summary>
        internal List<(ReadOnlyMemory<byte> Key, ulong Context)> GetDeletesInProgress(StorageSession storageSession)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// After a delete has completed, removes the given key from metadata.
        /// </summary>
        internal void ClearDeleteInProgress(ref BasicContext<VectorInput, PinnedSpanByte, long, VectorSessionFunctions, StoreFunctions, StoreAllocator> ctx, ref PinnedSpanByte key, ulong context)
        {
            Span<byte> inProgressDeletesKey = stackalloc byte[1];

            Span<byte> dataSpan = stackalloc byte[sizeof(ulong) + sizeof(int) + key.Length];
            BinaryPrimitives.WriteUInt64LittleEndian(dataSpan, context);

            // Negative length indicates we're removing this from the list
            BinaryPrimitives.WriteInt32LittleEndian(dataSpan[sizeof(ulong)..], -key.Length);
            key.ReadOnlySpan.CopyTo(dataSpan[(sizeof(ulong) + sizeof(int))..]);

            // 0:0 is ContextMetadata
            // 0:1 is InProgressDeletes
            
            inProgressDeletesKey[0] = 1;

            VectorInput input = default;
            input.Callback = 0;
            input.Namespace = 0;

            // Negative to indicate dynamic-ness
            input.WriteDesiredSize = -(sizeof(ulong) + sizeof(int) + key.Length);
            unsafe
            {
                input.CallbackContext = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dataSpan));
            }

            var status = ctx.RMW(inProgressDeletesKey, ref input);

            if (status.IsPending)
            {
                PinnedSpanByte ignored = default;
                CompletePending(ref status, ref ignored, ref ctx);
            }
        }

        /// <summary>
        /// After an index is dropped, called to start the process of removing ancillary data (elements, neighbor lists, attributes, etc.).
        /// </summary>
        internal void CleanupDroppedIndex(ref BasicContext<VectorInput, PinnedSpanByte, long, VectorSessionFunctions, StoreFunctions, StoreAllocator> ctx, ulong context)
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