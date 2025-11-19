// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    public static class LogRecordUtils
    {
        /// <summary>
        /// Determines whether the specified log record has expired.
        /// Returns true if the record has an expiration set and its expiration time is earlier than the current UTC time.
        /// </summary>
        /// <typeparam name="TSourceLogRecord">The type of the log record, which must implement <see cref="ISourceLogRecord"/>.</typeparam>
        /// <param name="srcLogRecord">The log record to check for expiration.</param>
        /// <returns>True if the log record has expired; otherwise, false.</returns>
        internal static bool CheckExpiry<TSourceLogRecord>(in TSourceLogRecord srcLogRecord)
            where TSourceLogRecord : ISourceLogRecord
            => srcLogRecord.Info.HasExpiration && srcLogRecord.Expiration < DateTimeOffset.UtcNow.Ticks;
    }
}