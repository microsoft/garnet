// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Extension methods for <see cref="SessionParseState"/>.
    /// </summary>
    public static class SessionParseStateExtensions
    {
        /// <summary>
        /// Parse info metrics type from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryGetInfoMetricsType(this SessionParseState parseState, int idx, out InfoMetricsType value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("SERVER"u8))
                value = InfoMetricsType.SERVER;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("MEMORY"u8))
                value = InfoMetricsType.MEMORY;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("CLUSTER"u8))
                value = InfoMetricsType.CLUSTER;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("REPLICATION"u8))
                value = InfoMetricsType.REPLICATION;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("STATS"u8))
                value = InfoMetricsType.STATS;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("STORE"u8))
                value = InfoMetricsType.STORE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("OBJECTSTORE"u8))
                value = InfoMetricsType.OBJECTSTORE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("STOREHASHTABLE"u8))
                value = InfoMetricsType.STOREHASHTABLE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("OBJECTSTOREHASHTABLE"u8))
                value = InfoMetricsType.OBJECTSTOREHASHTABLE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("STOREREVIV"u8))
                value = InfoMetricsType.STOREREVIV;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("OBJECTSTOREREVIV"u8))
                value = InfoMetricsType.OBJECTSTOREREVIV;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("PERSISTENCE"u8))
                value = InfoMetricsType.PERSISTENCE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("CLIENTS"u8))
                value = InfoMetricsType.CLIENTS;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("KEYSPACE"u8))
                value = InfoMetricsType.KEYSPACE;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("MODULES"u8))
                value = InfoMetricsType.MODULES;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("BPSTATS"u8))
                value = InfoMetricsType.BPSTATS;
            else return false;

            return true;
        }

        /// <summary>
        /// Parse latency metrics type from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryGetLatencyMetricsType(this SessionParseState parseState, int idx, out LatencyMetricsType value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("NET_RS_LAT"u8))
                value = LatencyMetricsType.NET_RS_LAT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("PENDING_LAT"u8))
                value = LatencyMetricsType.PENDING_LAT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("TX_PROC_LAT"u8))
                value = LatencyMetricsType.TX_PROC_LAT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("NET_RS_BYTES"u8))
                value = LatencyMetricsType.NET_RS_BYTES;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("NET_RS_OPS"u8))
                value = LatencyMetricsType.NET_RS_OPS;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("NET_RS_LAT_ADMIN"u8))
                value = LatencyMetricsType.NET_RS_LAT_ADMIN;
            else return false;

            return true;
        }

        /// <summary>
        /// Parse client type from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryGetClientType(this SessionParseState parseState, int idx, out ClientType value)
        {
            value = ClientType.Invalid;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("NORMAL"u8))
                value = ClientType.NORMAL;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("MASTER"u8))
                value = ClientType.MASTER;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("REPLICA"u8))
                value = ClientType.REPLICA;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("PUBSUB"u8))
                value = ClientType.PUBSUB;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("SLAVE"u8))
                value = ClientType.SLAVE;

            return value != ClientType.Invalid;
        }

        /// <summary>
        /// Parse bit field overflow from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        internal static bool TryGetBitFieldOverflow(this SessionParseState parseState, int idx, out BitFieldOverflow value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("WRAP"u8))
                value = BitFieldOverflow.WRAP;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("SAT"u8))
                value = BitFieldOverflow.SAT;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("FAIL"u8))
                value = BitFieldOverflow.FAIL;
            else return false;

            return true;
        }

        /// <summary>
        /// Parse manager type from parse state at specified index
        /// </summary>
        /// <param name="parseState">The parse state</param>
        /// <param name="idx">The argument index</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        internal static bool TryGetManagerType(this SessionParseState parseState, int idx, out ManagerType value)
        {
            value = default;
            var sbArg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

            if (sbArg.EqualsUpperCaseSpanIgnoringCase("MIGRATIONMANAGER"u8))
                value = ManagerType.MigrationManager;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("REPLICATIONMANAGER"u8))
                value = ManagerType.ReplicationManager;
            else if (sbArg.EqualsUpperCaseSpanIgnoringCase("SERVERLISTENER"u8))
                value = ManagerType.ServerListener;
            else return false;

            return true;
        }
    }
}