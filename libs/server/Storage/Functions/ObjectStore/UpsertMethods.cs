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
        public bool SingleWriter(ref LogRecord<IGarnetObject> dstLogRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, IGarnetObject srcValue, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            if (!dstLogRecord.TrySetValueObject(srcValue, ref sizeInfo))
                return false;
            // TODO ETag
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool SingleCopyWriter<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<IGarnetObject> dstLogRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
            => dstLogRecord.TryCopyRecordValues(ref srcLogRecord, ref sizeInfo);

        /// <inheritdoc />
        public void PostSingleWriter(ref LogRecord<IGarnetObject> logRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, IGarnetObject srcValue, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            if (reason != WriteReason.CopyToTail)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (reason == WriteReason.Upsert && functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, srcValue, upsertInfo.Version, upsertInfo.SessionID);

            if (reason == WriteReason.CopyToReadCache)
                functionsState.objectStoreSizeTracker?.AddReadCacheTrackedSize(srcValue.Size);
            else
                functionsState.objectStoreSizeTracker?.AddTrackedSize(srcValue.Size);
        }

        /// <inheritdoc />
        public bool ConcurrentWriter(ref LogRecord<IGarnetObject> logRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, IGarnetObject srcValue, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo)
        {
            _ = logRecord.TrySetValueObject(srcValue, ref sizeInfo);
            if (!(input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1)))
                return false;
            sizeInfo.AssertOptionals(logRecord.Info);

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, srcValue, upsertInfo.Version, upsertInfo.SessionID);
            functionsState.objectStoreSizeTracker?.AddTrackedSize(logRecord.ValueObject.Size - srcValue.Size);
            return true;
        }
    }
}