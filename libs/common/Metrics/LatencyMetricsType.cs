// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.common
{
    /// <summary>
    /// Types of latency metrics exposed by Garnet server
    /// </summary>
    public enum LatencyMetricsType : byte
    {
        /// <summary>
        /// Latency of processing, per network receive call (server side) - consider batches with only non-admin requests
        /// Measured from when we start processing the first request in packet, until when we finishing processing the last request in packet (including sending responses).
        /// Only calls that result in at least one request processed, are considered.
        /// </summary>
        NET_RS_LAT = 0,

        /// <summary>
        /// Latency of processing, per network receive call (server side) - consider batches with at least one non-admin request
        /// </summary>
        NET_RS_LAT_ADMIN = 5,

        /// <summary>
        /// Pending request completion latency
        /// </summary>
        PENDING_LAT = 1,
        /// <summary>
        /// Transaction proc latency monitoring
        /// </summary>
        TX_PROC_LAT = 2,
        /// <summary>
        /// Bytes processed, per network receive call (server side)
        /// </summary>
        NET_RS_BYTES = 3,
        /// <summary>
        /// Ops processed, per network receive call (server side)
        /// </summary>
        NET_RS_OPS = 4,
    }

    public static class LatencyMetricsUtils
    {
        /// <summary>
        /// Parse latency metrics type from span
        /// </summary>
        /// <param name="input">ReadOnlySpan input to parse</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryParseLatencyMetricsType(ReadOnlySpan<byte> input, out LatencyMetricsType value)
        {
            value = default;

            if (input.EqualsUpperCaseSpanIgnoringCase("NET_RS_LAT"u8))
                value = LatencyMetricsType.NET_RS_LAT;
            else if (input.EqualsUpperCaseSpanIgnoringCase("PENDING_LAT"u8))
                value = LatencyMetricsType.PENDING_LAT;
            else if (input.EqualsUpperCaseSpanIgnoringCase("TX_PROC_LAT"u8))
                value = LatencyMetricsType.TX_PROC_LAT;
            else if (input.EqualsUpperCaseSpanIgnoringCase("NET_RS_BYTES"u8))
                value = LatencyMetricsType.NET_RS_BYTES;
            else if (input.EqualsUpperCaseSpanIgnoringCase("NET_RS_OPS"u8))
                value = LatencyMetricsType.NET_RS_OPS;
            else if (input.EqualsUpperCaseSpanIgnoringCase("NET_RS_LAT_ADMIN"u8))
                value = LatencyMetricsType.NET_RS_LAT_ADMIN;
            else return false;

            return true;
        }
    }
}