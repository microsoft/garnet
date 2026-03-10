// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
    {
        /// <inheritdoc />
        public bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
            => true;

        /// <inheritdoc />
        public void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
        }

        /// <inheritdoc />
        public bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            functionsState.cacheSizeTracker?.AddHeapSize(-logRecord.ValueObject.HeapMemorySize);

            if (logRecord.Info.ValueIsObject)
            {
                // Can't access 'this' in a lambda so dispose directly and pass a no-op lambda.
                functionsState.storeFunctions.DisposeValueObject(logRecord.ValueObject, DisposeReason.Deleted);
                logRecord.ClearValueIfHeap(_ => { });
            }
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