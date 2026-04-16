// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;

namespace Garnet.common
{
    public enum GarnetTestLoggingEventType : int
    {
        LogPrimaryStreamType
    };

    public struct GarnetTestLoggingEvent
    {
        public GarnetTestLoggingEventType type;
        public string msg;

        public override string ToString() => $"<{type}>: {msg}";
    }

    public static class LoggingExtensions
    {
        public static void LogTesting(this ILogger logger, LogLevel logLevel, GarnetTestLoggingEvent state)
        {
            logger?.Log(LogLevel.Critical,
                eventId: default,
                state: state,
                exception: null,
                formatter: static (state, _) => $"{state}");
        }
    }
}