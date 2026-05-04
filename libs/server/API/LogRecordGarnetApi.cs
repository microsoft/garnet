// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// API wrapper for log record copy operations. Provides Upsert using the log record context.
    /// </summary>
    public struct LogRecordGarnetApi
    {
        readonly StorageSession storageSession;
        LogRecordBasicContext logRecordContext;

        internal LogRecordGarnetApi(StorageSession storageSession, LogRecordBasicContext logRecordContext)
        {
            this.storageSession = storageSession;
            this.logRecordContext = logRecordContext;
        }

        /// <summary>
        /// Upsert a log record into the store.
        /// </summary>
        public Status Upsert(FixedSpanByteKey key, ref LogRecordInput<ISourceLogRecord> input)
            => logRecordContext.Upsert(key, ref input);
    }
}
