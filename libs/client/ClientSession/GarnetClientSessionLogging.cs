// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;

namespace Garnet.client
{
    /// <summary>
    /// GarnetClientSession Logger Extensions
    /// </summary>
    public static partial class Log
    {
        [LoggerMessage(
            EventId = 0,
            Level = LogLevel.Trace,
            Message = "[{op}]: isMainStore:({storeType}) totalKeyCount:({totalKeyCount}), totalPayloadSize:({totalPayloadSize} KB)")]
        public static partial void LogMigrateProgress(this ILogger logger, string op, bool storeType, string totalKeyCount, string totalPayloadSize);
    }
}