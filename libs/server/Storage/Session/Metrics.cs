// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;

namespace Garnet.server
{
    sealed partial class StorageSession
    {
        public GarnetLatencyMetricsSession latencyMetrics => LatencyMetrics;
        readonly GarnetSessionMetrics sessionMetrics;
        readonly GarnetLatencyMetricsSession LatencyMetrics;

        public void incr_session_found()
            => sessionMetrics?.incr_total_found();

        public void incr_session_notfound()
            => sessionMetrics?.incr_total_notfound();

        public void incr_session_pending()
            => sessionMetrics?.incr_total_pending();

        public void StartPendingMetrics()
        {
            sessionMetrics?.incr_total_pending();
            latencyMetrics?.Start(LatencyMetricsType.PENDING_LAT);
        }

        public void StopPendingMetrics()
        {
            latencyMetrics?.Stop(LatencyMetricsType.PENDING_LAT);
        }
    }
}