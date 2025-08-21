// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    public static class LogRecordUtils
    {
        internal static bool CheckExpiry<TSourceLogRecord>(in TSourceLogRecord srcLogRecord)
            where TSourceLogRecord : ISourceLogRecord
            => srcLogRecord.Info.HasExpiration && srcLogRecord.Expiration < DateTimeOffset.UtcNow.Ticks;
    }
}
