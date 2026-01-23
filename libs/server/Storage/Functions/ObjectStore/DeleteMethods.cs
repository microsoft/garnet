// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool SingleDeleter(ref byte[] key, ref IGarnetObject value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
            => true;

        /// <inheritdoc />
        public void PostSingleDeleter(ref byte[] key, ref DeleteInfo deleteInfo)
        {
            if (!deleteInfo.RecordInfo.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
        }

        /// <inheritdoc />
        public bool ConcurrentDeleter(ref byte[] key, ref IGarnetObject value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
        {
            if (!deleteInfo.RecordInfo.Modified)
                functionsState.watchVersionMap.IncrementVersion(deleteInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                deleteInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            functionsState.objectStoreSizeTracker?.AddTrackedSize(-value.Size);
            value = null;
            return true;
        }

        /// <inheritdoc />
        public void PostDeleteOperation<TEpochAccessor>(ref byte[] key, ref DeleteInfo deleteInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if ((deleteInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
            {
                WriteLogDelete(ref key, deleteInfo.Version, deleteInfo.SessionID, epochAccessor);
            }
        }
    }
}