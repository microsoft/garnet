// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool SingleDeleter(ref LogRecord<IGarnetObject> logRecord, ref DeleteInfo deleteInfo)
            => true;

        /// <inheritdoc />
        public void PostSingleDeleter(ref LogRecord<IGarnetObject> logRecord, ref DeleteInfo deleteInfo)
        {
            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogDelete(logRecord.Key, deleteInfo.Version, deleteInfo.SessionID);
        }

        /// <inheritdoc />
        public bool ConcurrentDeleter(ref byte[] key, ref IGarnetObject value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
            if (!deleteInfo.RecordInfo.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogDelete(ref key, deleteInfo.Version, deleteInfo.SessionID);
            functionsState.objectStoreSizeTracker?.AddTrackedSize(-value.Size);
            return true;
        }
    }
}