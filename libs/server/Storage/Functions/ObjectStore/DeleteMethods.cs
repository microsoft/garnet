// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
            functionsState.objectStoreSizeTracker?.AddTrackedSize(-logRecord.ValueObject.HeapMemorySize);

            if (logRecord.Info.ValueIsObject)
            {
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