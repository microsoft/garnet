// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly partial struct MainSessionFunctions : ISessionFunctions<StringInput, StringOutput, long>
    {
        /// <inheritdoc />
        public bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            logRecord.InfoRef.ClearHasETag();
            functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            return true;
        }

        /// <inheritdoc />
        public void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
        }

        /// <inheritdoc />
        public bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            // Free BfTree if this is a RangeIndex record.
            // TODO: This only disposes trees in the mutable region.
            // We need to also dispose any unevicted trees in read-only region of memory
            if (logRecord.RecordType == RangeIndexManager.RangeIndexRecordType)
            {
                ref readonly var stub = ref RangeIndexManager.ReadIndex(logRecord.ValueSpan);
                if (stub.TreeHandle != 0 && stub.ProcessInstanceId == functionsState.rangeIndexManager.ProcessInstanceId)
                {
                    using (functionsState.rangeIndexManager.AcquireExclusiveForDelete(deleteInfo.KeyHash))
                        functionsState.rangeIndexManager.UnregisterIndex(stub.TreeHandle);
                }
            }

            logRecord.ClearOptionals();
            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }

        /// <inheritdoc />
        public void PostDeleteOperation<TKey, TEpochAccessor>(TKey key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
            if ((deleteInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
                WriteLogDelete(key.KeyBytes, deleteInfo.Version, deleteInfo.SessionID, epochAccessor);
        }
    }
}