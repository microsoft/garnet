// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
            recordInfo.ClearHasETag();
            functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            return true;
        }

        /// <inheritdoc />
        public void PostSingleDeleter(ref SpanByte key, ref DeleteInfo deleteInfo)
        {
            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
        }

        /// <inheritdoc />
        public bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
            recordInfo.ClearHasETag();
            if (!deleteInfo.RecordInfo.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }

        /// <inheritdoc />
        public void PostDeleteOperation<TEpochAccessor>(ref SpanByte key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if ((deleteInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
            {
                WriteLogDelete(ref key, deleteInfo.Version, deleteInfo.SessionID, epochAccessor);
            }
        }
    }
}