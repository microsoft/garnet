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
        public GarnetStatus GET_WithPending<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, long ctx, out bool pending)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = dualContext.Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ctx);

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
            incr_session_notfound();
            return GarnetStatus.NOTFOUND;
        }

        public bool GET_CompletePending<TKeyLocker, TEpochGuard>((GarnetStatus, SpanByteAndMemory)[] outputArr, bool wait)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            Debug.Assert(outputArr != null);

            latencyMetrics?.Start(LatencyMetricsType.PENDING_LAT);
            var ret = MainContext.CompletePendingWithOutputs<TKeyLocker>(out var completedOutputs, wait);
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

        public bool GET_CompletePending<TKeyLocker>(out CompletedOutputIterator<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long> completedOutputs, bool wait)
            where TKeyLocker : struct, ISessionLocker
        {
            latencyMetrics?.Start(LatencyMetricsType.PENDING_LAT);
            var ret = MainContext.CompletePendingWithOutputs<TKeyLocker>(out completedOutputs, wait);
            latencyMetrics?.Stop(LatencyMetricsType.PENDING_LAT);
            return ret;
        }

        public GarnetStatus RMW_MainStore<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = dualContext.RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out output);

            return status.Found || status.Record.Created ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }

        public GarnetStatus Read_MainStore<TKeyLocker, TEpochGuard>(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = dualContext.Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output);

            if (status.IsPending)
                CompletePending<TKeyLocker>(out status, out output);

            return status.Found ? GarnetStatus.OK : GarnetStatus.NOTFOUND;
        }
    }
}