// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Cluster Logger Extensions
    /// </summary>
    public static partial class Log
    {
        [LoggerMessage(
            EventId = 0,
            Level = LogLevel.Trace,
            Message = "[{op}]: isMainStore:({storeType}) totalKeyCount:({totalKeyCount})")]
        public static partial void LogTraceDataImport(this ILogger logger, string op, bool storeType, string totalKeyCount);
    }
}