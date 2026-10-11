// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus Read_UnifiedStore<TUnifiedContext>(ReadOnlySpan<byte> key, ref UnifiedInput input, ref UnifiedOutput output, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = unifiedContext.Read((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForUnifiedStoreSession(ref status, ref output, ref unifiedContext);

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public GarnetStatus RMW_UnifiedStore<TUnifiedContext>(ReadOnlySpan<byte> key, ref UnifiedInput input, ref UnifiedOutput output, ref TUnifiedContext context)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.RMW((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForUnifiedStoreSession(ref status, ref output, ref context);

            return status.Found || status.Record.Created || status.Record.InPlaceUpdated
                ? GarnetStatus.OK
                : GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Reads from the unified store, reporting rather than waiting when the record is not in memory, so
        /// the caller can decide how to wait for the device instead of holding the calling thread on it.
        /// </summary>
        /// <param name="key">Key to read.</param>
        /// <param name="input">Input describing what to read.</param>
        /// <param name="output">Output for a read served from memory.</param>
        /// <param name="pending">True if the read went to the device, leaving the status meaningless.</param>
        /// <param name="unifiedContext">Context to read through.</param>
        /// <returns>The status, or the pending sentinel when <paramref name="pending"/> is set.</returns>
        public GarnetStatus Read_UnifiedStore_WithPending<TUnifiedContext>(ReadOnlySpan<byte> key, ref UnifiedInput input, ref UnifiedOutput output, out bool pending, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = unifiedContext.Read((FixedSpanByteKey)key, ref input, ref output);
            if (status.IsPending)
            {
                incr_session_pending();
                pending = true;
                return PendingStatus;
            }

            pending = false;
            return InterpretRead(status);
        }

        /// <summary>
        /// Updates the unified store, reporting rather than waiting when the record is not in memory. The
        /// update is already applied when this reports pending; only its outcome is still on the device.
        /// </summary>
        /// <param name="key">Key to update.</param>
        /// <param name="input">Input describing the update.</param>
        /// <param name="output">Output for an update served from memory.</param>
        /// <param name="pending">True if the update went to the device, leaving the status meaningless.</param>
        /// <param name="context">Context to update through.</param>
        /// <returns>The status, or the pending sentinel when <paramref name="pending"/> is set.</returns>
        public GarnetStatus RMW_UnifiedStore_WithPending<TUnifiedContext>(ReadOnlySpan<byte> key, ref UnifiedInput input, ref UnifiedOutput output, out bool pending, ref TUnifiedContext context)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.RMW((FixedSpanByteKey)key, ref input, ref output);
            if (status.IsPending)
            {
                incr_session_pending();
                pending = true;
                return PendingStatus;
            }

            pending = false;
            return InterpretRmw(status);
        }

        /// <summary>
        /// Drains this session's pending unified-store operations, waiting on the device if asked to.
        /// </summary>
        /// <param name="completedOutputs">Outputs of the operations that completed.</param>
        /// <param name="wait">Whether to wait for every outstanding operation.</param>
        /// <param name="unifiedContext">Context to drain.</param>
        /// <returns>True if every outstanding operation completed.</returns>
        public bool Unified_CompletePending<TUnifiedContext>(out CompletedOutputIterator<UnifiedInput, UnifiedOutput, long> completedOutputs, bool wait, ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
            => unifiedContext.CompletePendingWithOutputs(out completedOutputs, wait);

        /// <summary>
        /// Drains this session's pending unified-store operations without holding the calling thread on the
        /// device.
        /// </summary>
        /// <param name="unifiedContext">Context to drain.</param>
        /// <returns>The completed outputs, which the caller must dispose.</returns>
        public ValueTask<CompletedOutputIterator<UnifiedInput, UnifiedOutput, long>> Unified_CompletePendingAsync<TUnifiedContext>(ref TUnifiedContext unifiedContext)
            where TUnifiedContext : ITsavoriteContext<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator>
            => unifiedContext.CompletePendingWithOutputsAsync();

        /// <summary>
        /// Maps the status of a completed unified-store read, matching <see cref="Read_UnifiedStore{T}"/>.
        /// </summary>
        internal static GarnetStatus InterpretRead(Status status)
            => status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;

        /// <summary>
        /// Maps the status of a completed unified-store update, matching <see cref="RMW_UnifiedStore{T}"/>.
        /// </summary>
        internal static GarnetStatus InterpretRmw(Status status)
            => status.Found || status.Record.Created || status.Record.InPlaceUpdated
                ? GarnetStatus.OK
                : GarnetStatus.NOTFOUND;

        /// <summary>
        /// The value the advanced API returns for an operation that went to the device. Deliberately outside
        /// <see cref="GarnetStatus"/>, whose members are part of the public API.
        /// </summary>
        internal const GarnetStatus PendingStatus = (GarnetStatus)byte.MaxValue;
    }
}