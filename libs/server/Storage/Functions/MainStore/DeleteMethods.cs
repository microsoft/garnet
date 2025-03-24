// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool SingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            logRecord.InfoRef.ClearHasETag();
            functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            return true;
        }

        /// <inheritdoc />
        public void PostSingleDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            if (functionsState.appendOnlyFile != null)
                WriteLogDelete(logRecord.Key, deleteInfo.Version, deleteInfo.SessionID);
        }

        /// <inheritdoc />
        public bool ConcurrentDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            logRecord.ClearOptionals();
            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogDelete(logRecord.Key, deleteInfo.Version, deleteInfo.SessionID);
            return true;
        }
    }
}