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
    }

    /// <summary>
    /// Utils for info command
    /// </summary>
    public static class InfoCommandUtils
    {
        static readonly byte[][] infoSections =
            Enum.GetValues(typeof(InfoMetricsType)).
            Cast<InfoMetricsType>().
            Select(x => Encoding.ASCII.GetBytes($"${x.ToString().Length}\r\n{x}\r\n")).ToArray();

        /// <summary>
        /// Return resp formatted info section
        /// </summary>
        /// <param name="infoMetricsType"></param>
        /// <returns></returns>
        public static byte[] GetRespFormattedInfoSection(InfoMetricsType infoMetricsType)
            => infoMetricsType == default ? default(byte[]) : infoSections[(int)infoMetricsType];
    }
}