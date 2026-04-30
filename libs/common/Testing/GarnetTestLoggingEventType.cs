// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    public enum GarnetTestLoggingEventType : int
    {
        LogPrimaryStreamType,
        LogRunAofSyncTask
    };

    public struct GarnetTestLoggingEvent
    {
        public GarnetTestLoggingEventType Type;
        public string Message;

        public override string ToString() => $"++<{Type}>++: {Message}";
    }

    public static class LoggingExtensions
    {
        public static void LogTesting(this ILogger logger, GarnetTestLoggingEvent state)
        {
            logger?.Log(LogLevel.Critical,
                eventId: default,
                state: state,
                exception: null,
                formatter: static (state, _) => $"{state}");
        }
    }
}