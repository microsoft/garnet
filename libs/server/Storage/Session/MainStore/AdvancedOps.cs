// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    sealed partial class StorageSession : IDisposable
    {
        public GarnetStatus GET_WithPending<TContext>(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, long ctx, out bool pending, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var status = context.Read(ref key, ref input, ref output, ctx);

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

        public bool GET_CompletePending<TContext>((GarnetStatus, SpanByteAndMemory)[] outputArr, bool wait, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
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

        public bool GET_CompletePending<TContext>(out CompletedOutputIterator<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long> completedOutputs, bool wait, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            latencyMetrics?.Start(LatencyMetricsType.PENDING_LAT);
            var ret = context.CompletePendingWithOutputs(out completedOutputs, wait);
            latencyMetrics?.Stop(LatencyMetricsType.PENDING_LAT);
            return ret;
        }

        public GarnetStatus RMW_MainStore<TContext>(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var status = context.RMW(ref key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context);

            if (status.Found || status.Record.Created || status.Record.InPlaceUpdated)
                return GarnetStatus.OK;
            else
                return GarnetStatus.NOTFOUND;
        }

        public GarnetStatus Read_MainStore<TContext>(ref SpanByte key, ref RawStringInput input, ref SpanByteAndMemory output, ref TContext context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            var status = context.Read(ref key, ref input, ref output);

            if (status.IsPending)
                CompletePendingForSession(ref status, ref output, ref context);

            if (status.Found)
            {
                return GarnetStatus.OK;
            }
            else if (status.IsCanceled)
            {
                // Vector Sets signal WRONGTYPE via cancellation - everything else will fall into NOTFOUND
                return GarnetStatus.WRONGTYPE;
            }
            else
            {
                return GarnetStatus.NOTFOUND;
            }
        }


        public void ReadWithPrefetch<TBatch, TContext>(ref TBatch batch, ref TContext context, long userContext = default)
            where TBatch : IReadArgBatch<SpanByte, RawStringInput, SpanByteAndMemory>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TContext : ITsavoriteContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        => context.ReadWithPrefetch(ref batch, userContext);
    }
}