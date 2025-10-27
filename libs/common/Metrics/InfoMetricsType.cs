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
        // IMPORTANT: Any changes to the values of this enum should be reflected in its parser (SessionParseStateExtensions.TryGetInfoMetricsType)

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
        /// Store hash table info
        /// </summary>
        STOREHASHTABLE,
        /// <summary>
        /// Store revivification info
        /// </summary>
        STOREREVIV,
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
        /// <summary>
        /// Checkpoint information used for cluster
        /// </summary>
        CINFO,
        /// <summary>
        /// Scan and return distribution of in-memory portion of hybrid logs
        /// </summary>
        HLOGSCAN,
    }

    /// <summary>
    /// Utils for info command
    /// </summary>
    public static class InfoCommandUtils
    {
        static readonly byte[][] infoSections = [.. Enum.GetValues<InfoMetricsType>().Select(x => Encoding.ASCII.GetBytes($"${x.ToString().Length}\r\n{x}\r\n"))];

        /// <summary>
        /// Return resp formatted info section
        /// </summary>
        /// <param name="infoMetricsType"></param>
        /// <returns></returns>
        public static byte[] GetRespFormattedInfoSection(InfoMetricsType infoMetricsType)
            => infoMetricsType == default ? default(byte[]) : infoSections[(int)infoMetricsType];
    }
}