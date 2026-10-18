// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus GET_WithPending<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, long ctx, out bool pending, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.Read((FixedSpanByteKey)key, ref input, ref output, ctx);

            if (status.IsPending)
            {
                incr_session_pending();
                pending = true;
                return (GarnetStatus)byte.MaxValue; // special return value to indicate pending operation, we do not add it to enum in order not to confuse users of GarnetApi
            }

            pending = false;
            if (status.Found)
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

        public bool GET_CompletePending<TStringContext>((GarnetStatus, StringOutput)[] outputArr, bool wait, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            Debug.Assert(outputArr != null);

            latencyMetrics?.Start(LatencyMetricsType.PENDING_LAT);
            var ret = context.CompletePendingWithOutputs(out var completedOutputs, wait);
            latencyMetrics?.Stop(LatencyMetricsType.PENDING_LAT);

            // Update array with completed outputs
            while (completedOutputs.Next())
            {
                outputArr[(int)completedOutputs.Current.Context] = (completedOutputs.Current.Status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND, completedOutputs.Current.Output);

                if (completedOutputs.Current.Status.Found)
                    sessionMetrics?.incr_total_found();
                else
                    sessionMetrics?.incr_total_notfound();
            }
            completedOutputs.Dispose();

            return ret;
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

            if (status.Found || status.Record.Created || status.Record.InPlaceUpdated)
                return GarnetStatus.OK;
            else if (status.IsWrongType)
                return GarnetStatus.WRONGTYPE;
            else
                return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus Read_MainStore<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.Read((FixedSpanByteKey)key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context);

            if (status.Found)
                return GarnetStatus.OK;
            else if (status.IsWrongType)
                return GarnetStatus.WRONGTYPE;
            else
                return GarnetStatus.NOTFOUND;
        }

        /// <summary>
        /// Specialized Read for RangeIndex stubs. Suppresses Tsavorite's automatic
        /// <c>CopyReadsToTail</c> / <c>CopyReadsToReadCache</c> for this single Read by passing
        /// <see cref="ReadCopyOptions.None"/>, then calls into the standard Read pipeline.
        ///
        /// <para>Why a separate API: RangeIndex performs its own controlled promotion via
        /// <c>RIPROMOTE</c> RMW (which propagates RecordType and manages TreeHandle ownership in
        /// <c>PostCopyUpdater</c>, after the promoting reader staged <c>data.bftree</c> under the
        /// per-key lock). Allowing Tsavorite's CTT to race with that path would leave the destination
        /// record without <c>RecordType=RangeIndexRecordType</c> (CTT does not propagate RecordType).
        /// Keeping this on a dedicated API ensures every RangeIndex stub Read goes through the
        /// suppression and other Read callers (Bitmap, HLL, etc.) incur zero overhead.</para>
        /// </summary>
        public GarnetStatus Read_RangeIndex<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
            => Read_RangeIndex(key, ref input, ref output, ref context, out _);

        /// <summary>
        /// <see cref="Read_RangeIndex{TStringContext}(ReadOnlySpan{byte}, ref StringInput, ref StringOutput, ref TStringContext)"/>,
        /// also returning the logical address of the stub's record (<see cref="LogAddress.kInvalidAddress"/> if it was read
        /// from the read cache), which names its flush snapshot.
        /// </summary>
        public GarnetStatus Read_RangeIndex<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, ref TStringContext context, out long address)
            where TStringContext : ITsavoriteContext<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var readOptions = new ReadOptions { CopyOptions = ReadCopyOptions.None };
            var status = context.Read((FixedSpanByteKey)key, ref input, ref output, ref readOptions, out var recordMetadata);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context, out recordMetadata);

            address = recordMetadata.Address;

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