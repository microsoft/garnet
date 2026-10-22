// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus GET_WithPending<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, long ctx, out bool pending, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadWithPendingStatus(context.Read((FixedSpanByteKey)key, ref input, ref output, ctx), out pending);

        public GarnetStatus GET_WithDetachedPending<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, long ctx, out bool pending, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var readOptions = new ReadOptions { HoistKey = true };
            return ReadWithPendingStatus(context.Read((FixedSpanByteKey)key, ref input, ref output, ref readOptions, ctx), out pending);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        GarnetStatus ReadWithPendingStatus(Status status, out bool pending)
        {
            if (status.IsPending)
            {
                incr_session_pending();
                pending = true;
                return (GarnetStatus)byte.MaxValue; // special return value to indicate pending operation, we do not add it to enum in order not to confuse users of GarnetApi
            }

            pending = false;
            if (status.IsWrongType)
            {
                return GarnetStatus.WRONGTYPE;
            }
            else if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }
            else
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }
        }

        /// <summary>
        /// Completes this session's pending string reads without waiting on the device, yielding the thread
        /// until the I/O lands.
        /// </summary>
        /// <param name="context">String context to complete against.</param>
        /// <typeparam name="TStringContext">Type of the string context.</typeparam>
        /// <returns>The completed outputs, which the caller must dispose.</returns>
        public ValueTask<CompletedOutputIterator<StringInput, StringOutput, long>> GET_CompletePendingAsync<TStringContext>(ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => context.CompletePendingWithOutputsAsync();

        /// <summary>
        /// Records the outcome of a read that completed from pending I/O in the session metrics, which the
        /// issuing call could not do because it did not yet have a status.
        /// </summary>
        /// <param name="found">Whether the record was found.</param>
        internal void IncrementPendingReadResult(bool found)
        {
            if (found)
                incr_session_found();
            else
                incr_session_notfound();
        }

        public bool GET_CompletePending<TStringContext>((GarnetStatus Status, StringOutput Output)[] outputArr, bool wait, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            Debug.Assert(outputArr != null);

            latencyMetrics?.Start(LatencyMetricsType.PENDING_LAT);
            var ret = context.CompletePendingWithOutputs(out var completedOutputs, wait);
            latencyMetrics?.Stop(LatencyMetricsType.PENDING_LAT);

            ScatterCompletedGets(completedOutputs, outputArr);
            return ret;
        }

        /// <summary>
        /// Places completed reads into their submission slots and records them in the session metrics.
        /// </summary>
        /// <remarks>
        /// Completions arrive in device order, not submission order, so each output is routed by the context
        /// the caller stamped on it at submission. Disposes the iterator.
        /// </remarks>
        /// <param name="completedOutputs">Outputs of the completion, whose contexts index <paramref name="outputArr"/>.</param>
        /// <param name="outputArr">Per-submission slots to fill.</param>
        internal void ScatterCompletedGets(CompletedOutputIterator<StringInput, StringOutput, long> completedOutputs,
            (GarnetStatus Status, StringOutput Output)[] outputArr)
        {
            Debug.Assert(outputArr != null);

            while (completedOutputs.Next())
            {
                outputArr[(int)completedOutputs.Current.Context] = (completedOutputs.Current.Status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND, completedOutputs.Current.Output);

                if (completedOutputs.Current.Status.Found)
                    sessionMetrics?.incr_total_found();
                else
                    sessionMetrics?.incr_total_notfound();
            }
            completedOutputs.Dispose();
        }

        public bool GET_CompletePending<TStringContext>(out CompletedOutputIterator<StringInput, StringOutput, long> completedOutputs, bool wait, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            latencyMetrics?.Start(LatencyMetricsType.PENDING_LAT);
            var ret = context.CompletePendingWithOutputs(out completedOutputs, wait);
            latencyMetrics?.Stop(LatencyMetricsType.PENDING_LAT);
            return ret;
        }

        public GarnetStatus RMW_MainStore<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.RMW((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context);

            return StringUpdateStatus(status);
        }

        public GarnetStatus Read_MainStore<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.Read((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context);

            return StringReadStatus(status);
        }

        /// <summary>
        /// Updates the string store, reporting rather than waiting when the record is not in memory. A pending
        /// update is applied when the record arrives, in the completion of the pending operation, so it must
        /// not be issued again.
        /// </summary>
        /// <param name="key">Key to update.</param>
        /// <param name="input">Input describing the update.</param>
        /// <param name="output">Output of the update. It is written when the update completes, so its memory
        /// must outlive a pending update.</param>
        /// <param name="context">Context to update through.</param>
        /// <returns>The store's status, which each command maps to a <see cref="GarnetStatus"/> its own way.</returns>
        public Status RMW_MainStore_WithPending<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.RMW((FixedSpanByteKey)key, ref input, ref output);
            if (status.IsPending)
                incr_session_pending();
            return status;
        }

        /// <summary>
        /// Reads the string store, reporting rather than waiting when the record is not in memory.
        /// </summary>
        /// <param name="key">Key to read.</param>
        /// <param name="input">Input describing what to read.</param>
        /// <param name="output">Output of the read. It is written when the read completes, so its memory must
        /// outlive a pending read.</param>
        /// <param name="context">Context to read through.</param>
        /// <returns>The store's status, which each command maps to a <see cref="GarnetStatus"/> its own way.</returns>
        public Status Read_MainStore_WithPending<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.Read((FixedSpanByteKey)key, ref input, ref output);
            if (status.IsPending)
                incr_session_pending();
            return status;
        }

        /// <summary>
        /// Maps the status of a string read whose outcome the session metrics count: <c>GET</c>, <c>GETEX</c>
        /// and <c>GETRANGE</c>.
        /// </summary>
        internal GarnetStatus CountedStringReadStatus(Status status)
        {
            if (status.Found)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }

            if (status.IsWrongType)
                return GarnetStatus.WRONGTYPE;

            incr_session_notfound();
            return GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Maps the status of a string read whose outcome is not counted.
        /// </summary>
        internal static GarnetStatus StringReadStatus(Status status)
            => status.Found ? GarnetStatus.OK
                : status.IsWrongType ? GarnetStatus.WRONGTYPE
                : GarnetStatus.NOTFOUND;

        /// <summary>
        /// Maps the status of a string update, which counts as having found its record if it created one.
        /// </summary>
        internal static GarnetStatus StringUpdateStatus(Status status)
            => status.Found || status.Record.Created || status.Record.InPlaceUpdated ? GarnetStatus.OK
                : status.IsWrongType ? GarnetStatus.WRONGTYPE
                : GarnetStatus.NOTFOUND;

        /// <summary>
        /// Maps the status of a conditional set, which reports whether the key existed before it.
        /// </summary>
        internal GarnetStatus ConditionalSetStatus(Status status)
        {
            if (status.IsWrongType)
                return GarnetStatus.WRONGTYPE;

            if (status.NotFound)
            {
                incr_session_notfound();
                return GarnetStatus.NOTFOUND;
            }

            incr_session_found();
            return GarnetStatus.OK;
        }

        /// <summary>
        /// Maps the status of a conditional delete. Deletions in RMW are done by expiring the record, so
        /// expiration of a record that was found is what indicates success; a record that had already expired was not found.
        /// </summary>
        internal GarnetStatus ConditionalDeleteStatus(Status status)
        {
            if (status.Found && status.IsExpired)
            {
                incr_session_found();
                return GarnetStatus.OK;
            }

            if (status.NotFound)
                incr_session_notfound();

            return GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Maps the status of a string update that succeeds unless the key holds another type.
        /// </summary>
        internal static GarnetStatus StringTypedStatus(Status status)
            => status.IsWrongType ? GarnetStatus.WRONGTYPE : GarnetStatus.OK;

        /// <summary>
        /// Specialized Read for RangeIndex stubs. Suppresses Tsavorite's automatic
        /// <c>CopyReadsToTail</c> / <c>CopyReadsToReadCache</c> for this single Read by passing
        /// <see cref="ReadCopyOptions.None"/>, then calls into the standard Read pipeline.
        ///
        /// <para>Why a separate API: RangeIndex performs its own controlled promotion via
        /// <c>RIPROMOTE</c> RMW (which propagates RecordType, manages TreeHandle ownership in
        /// <c>PostCopyUpdater</c>, and pre-stages <c>data.bftree</c> with proper locking).
        /// Allowing Tsavorite's CTT to race with that path would (a) leave the destination
        /// record without <c>RecordType=RangeIndexRecordType</c> (CTT does not propagate
        /// RecordType), and (b) trigger <c>PostCopyToTail</c>-cold which takes the per-key
        /// X-lock, self-deadlocking against the reader's S-lock when CopyReadsToTail is
        /// enabled at the session/KV level. Keeping this on a dedicated API ensures every
        /// RangeIndex stub Read goes through the suppression and other Read callers (Bitmap,
        /// HLL, etc.) incur zero overhead.</para>
        /// </summary>
        public GarnetStatus Read_RangeIndex<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var readOptions = new ReadOptions { CopyOptions = ReadCopyOptions.None };
            var status = context.Read((FixedSpanByteKey)key, ref input, ref output, ref readOptions);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context);

            if (status.Found)
                return GarnetStatus.OK;
            else if (status.IsWrongType)
                return GarnetStatus.WRONGTYPE;
            else
                return GarnetStatus.NOTFOUND;
        }

        public void ReadWithPrefetch<TBatch, TContext>(ref TBatch batch, ref TContext context, long userContext = default)
            where TBatch : IReadArgBatch<FixedSpanByteKey, StringInput, StringOutput>, allows ref struct
            where TContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        => context.ReadWithPrefetch(ref batch, userContext);
    }
}