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
            where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.Read(key, ref input, ref output, ctx);

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
            where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
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
            where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            latencyMetrics?.Start(LatencyMetricsType.PENDING_LAT);
            var ret = context.CompletePendingWithOutputs(out completedOutputs, wait);
            latencyMetrics?.Stop(LatencyMetricsType.PENDING_LAT);
            return ret;
        }

        public GarnetStatus RMW_MainStore<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.RMW(key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context);

            if (status.Found || status.Record.Created || status.Record.InPlaceUpdated)
                return GarnetStatus.OK;
            else
                return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus Read_MainStore<TStringContext>(ReadOnlySpan<byte> key, ref StringInput input, ref StringOutput output, ref TStringContext context)
            where TStringContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = context.Read(key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context);

            if (status.Found)
                return GarnetStatus.OK;
            else
                return GarnetStatus.NOTFOUND;
        }


        public void ReadWithPrefetch<TBatch, TContext>(ref TBatch batch, ref TContext context, long userContext = default)
            where TBatch : IReadArgBatch<StringInput, StringOutput>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TContext : ITsavoriteContext<StringInput, StringOutput, long, MainSessionFunctions, StoreFunctions, StoreAllocator>
        => context.ReadWithPrefetch(ref batch, userContext);
    }
}