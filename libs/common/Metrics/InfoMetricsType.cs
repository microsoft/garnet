// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;

namespace Garnet.common
{
    /// <summary>
    /// Types of info metrics exposed by Garnet server
    /// </summary>
    public enum InfoMetricsType : byte
    {
        /// <summary>
        /// Server info
        /// </summary>
        SERVER,
        /// <summary>
        /// Memory info
        /// </summary>
        MEMORY,
        /// <summary>
        /// Cluster info
        /// </summary>
        CLUSTER,
        /// <summary>
        /// Replication info
        /// </summary>
        REPLICATION,
        /// <summary>
        /// Stats info
        /// </summary>
        STATS,
        /// <summary>
        /// Store info
        /// </summary>
        STORE,
        /// <summary>
        /// Object store info
        /// </summary>
        OBJECTSTORE,
        /// <summary>
        /// Store hash table info
        /// </summary>
        STOREHASHTABLE,
        /// <summary>
        /// Object store hash table info
        /// </summary>
        OBJECTSTOREHASHTABLE,
        /// <summary>
        /// Store revivification info
        /// </summary>
        STOREREVIV,
        /// <summary>
        /// Object store hash table info
        /// </summary>
        OBJECTSTOREREVIV,
        /// <summary>
        /// Persistence information
        /// </summary>
        PERSISTENCE,
        /// <summary>
        /// Clients connections stats
        /// </summary>
        CLIENTS,
        /// <summary>
        /// Database related stats
        /// </summary>
        KEYSPACE,
        /// <summary>
        /// Modules info
        /// </summary>
        MODULES,
        /// <summary>
        /// Shared buffer pool stats
        /// </summary>
        BPSTATS,
    }

    /// <summary>
    /// Utils for info command
    /// </summary>
    public static class InfoCommandUtils
    {
        static readonly byte[][] infoSections = Enum.GetValues<InfoMetricsType>()
            .Select(x => Encoding.ASCII.GetBytes($"${x.ToString().Length}\r\n{x}\r\n")).ToArray();

        /// <summary>
        /// Return resp formatted info section
        /// </summary>
        /// <param name="infoMetricsType"></param>
        /// <returns></returns>
        public static byte[] GetRespFormattedInfoSection(InfoMetricsType infoMetricsType)
            => infoMetricsType == default ? default(byte[]) : infoSections[(int)infoMetricsType];

        /// <summary>
        /// Parse slot state from span
        /// </summary>
        /// <param name="input">ReadOnlySpan input to parse</param>
        /// <param name="value">Parsed value</param>
        /// <returns>True if value parsed successfully</returns>
        public static bool TryParseInfoMetricsType(ReadOnlySpan<byte> input, out InfoMetricsType value)
        {
            value = default;

            if (input.EqualsUpperCaseSpanIgnoringCase("SERVER"u8))
            {
                value = InfoMetricsType.SERVER;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("MEMORY"u8))
            {
                value = InfoMetricsType.MEMORY;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("CLUSTER"u8))
            {
                value = InfoMetricsType.CLUSTER;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("REPLICATION"u8))
            {
                value = InfoMetricsType.REPLICATION;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("STATS"u8))
            {
                value = InfoMetricsType.STATS;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("STORE"u8))
            {
                value = InfoMetricsType.STORE;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("OBJECTSTORE"u8))
            {
                value = InfoMetricsType.OBJECTSTORE;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("STOREHASHTABLE"u8))
            {
                value = InfoMetricsType.STOREHASHTABLE;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("OBJECTSTOREHASHTABLE"u8))
            {
                value = InfoMetricsType.OBJECTSTOREHASHTABLE;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("STOREREVIV"u8))
            {
                value = InfoMetricsType.STOREREVIV;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("OBJECTSTOREREVIV"u8))
            {
                value = InfoMetricsType.OBJECTSTOREREVIV;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("PERSISTENCE"u8))
            {
                value = InfoMetricsType.PERSISTENCE;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("CLIENTS"u8))
            {
                value = InfoMetricsType.CLIENTS;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("KEYSPACE"u8))
            {
                value = InfoMetricsType.KEYSPACE;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("MODULES"u8))
            {
                value = InfoMetricsType.MODULES;
                return true;
            }
            if (input.EqualsUpperCaseSpanIgnoringCase("BPSTATS"u8))
            {
                value = InfoMetricsType.BPSTATS;
                return true;
            }

            return false;
        }
    }
}