// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedInput, UnifiedOutput, long>
    {
        public bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            if (!logRecord.Info.ValueIsObject)
            {
                logRecord.InfoRef.ClearHasETag();
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            }

            return true;
        }

        public void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            if (logRecord.Info.ValueIsObject && !logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);

            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
        }

        public bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            if (!logRecord.Info.ValueIsObject)
                logRecord.ClearOptionals();

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);

            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF

            if (logRecord.Info.ValueIsObject)
            {
                functionsState.cacheSizeTracker?.AddHeapSize(-logRecord.ValueObject.HeapMemorySize);

                // Can't access 'this' in a lambda so dispose directly and pass a no-op lambda.
                functionsState.storeFunctions.DisposeValueObject(logRecord.ValueObject, DisposeReason.Deleted);
                logRecord.ClearValueIfHeap(_ => { });
            }
            return true;
        }

        /// <inheritdoc />
        public void PostDeleteOperation<TEpochAccessor>(ReadOnlySpan<byte> key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if ((deleteInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
                WriteLogDelete(key, deleteInfo.Version, deleteInfo.SessionID, epochAccessor);
        }
    }
}