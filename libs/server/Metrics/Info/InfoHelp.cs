// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Garnet.common;

namespace Garnet.server
{
    class InfoHelp
    {
        internal const string HELP = "HELP";
        internal const string ALL = "ALL";
        internal const string RESET = "RESET";

        public static List<string> GetInfoTypeHelpMessage()
        {
            return new List<string>
            {
                "# Info options",
                $"{nameof(InfoMetricsType.SERVER)}: General information about the Garnet instance.",
                $"{nameof(InfoMetricsType.MEMORY)}: Server memory usage information.",
                $"{nameof(InfoMetricsType.CLUSTER)}: Cluster instance specific operational info.",
                $"{nameof(InfoMetricsType.REPLICATION)}: Replication info.",
                $"{nameof(InfoMetricsType.STATS)}: General server operational stats.",
                $"{nameof(InfoMetricsType.STORE)}: Main store operational information.",
                $"{nameof(InfoMetricsType.OBJECTSTORE)}: Object store operational information.",
                $"{nameof(InfoMetricsType.STOREHASHTABLE)}: Hash table distribution info for main store (expensive, not returned by default).",
                $"{nameof(InfoMetricsType.OBJECTSTOREHASHTABLE)}: Hash table distribution info for object store (expensive, not returned by default).",
                $"{nameof(InfoMetricsType.STOREREVIV)}: Revivification info for deleted records in main store (not returned by default).",
                $"{nameof(InfoMetricsType.OBJECTSTOREREVIV)}: Record revivification info for deleted records in object store (not returned by default).",
                $"{nameof(InfoMetricsType.PERSISTENCE)}: Persistence related information (i.e. Checkpoint and AOF).",
                $"{nameof(InfoMetricsType.CLIENTS)}: Information related to client connections.",
                $"{nameof(InfoMetricsType.KEYSPACE)}: Database related statistics.",
                $"{nameof(InfoMetricsType.MODULES)}: Information related to loaded modules.",
                $"{nameof(InfoMetricsType.HLOGSCAN)}: Distribution of records in main store's hybrid log in-memory portion.",
                $"{nameof(ALL)}: Return all informational sections.",
                $"{nameof(HELP)}: Print this help message.",
                $"{nameof(RESET)}: Reset stats.",
                "\r\n",
            };
        }
    }
}