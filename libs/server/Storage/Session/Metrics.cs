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

        /// <summary>
        /// Counts an operation that went to the device and starts timing its wait, for callers that wait on
        /// the device immediately after issuing the operation.
        /// </summary>
        public void StartPendingMetrics()
        {
            incr_session_pending();
            StartPendingLatency();
        }

        /// <summary>
        /// Starts timing a device wait for operations already counted by <see cref="incr_session_pending"/>
        /// when they were issued, which is how every operation that reports pending to its caller is counted.
        /// </summary>
        public void StartPendingLatency()
            => latencyMetrics?.Start(LatencyMetricsType.PENDING_LAT);

        /// <summary>
        /// Stops timing a device wait.
        /// </summary>
        public void StopPendingLatency()
            => latencyMetrics?.Stop(LatencyMetricsType.PENDING_LAT);
    }
}