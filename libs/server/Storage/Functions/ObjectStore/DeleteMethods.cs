// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectStoreFunctions : IFunctions<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
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
                WriteLogDelete(ref key, deleteInfo.Version, deleteInfo.SessionID);
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