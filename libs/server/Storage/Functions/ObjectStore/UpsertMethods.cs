// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ReadOnlySpan<byte> srcValue, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            if (!dstLogRecord.TrySetValueSpan(srcValue, ref sizeInfo))
                return false;
            // TODO ETag
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, IHeapObject srcValue, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
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
        public bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ref TSourceLogRecord inputLogRecord, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord
            => dstLogRecord.TryCopyFrom(ref inputLogRecord, ref sizeInfo);

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ReadOnlySpan<byte> srcValue, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, srcValue, upsertInfo.Version, upsertInfo.SessionID);

            // TODO: Need to track original length as well, if it was overflow, and add overflow here as well as object size
            // TODO: Need to track lengths written to readcache, which is now internal in Tsavorite
            if (logRecord.Info.ValueIsOverflow)
                functionsState.objectStoreSizeTracker?.AddTrackedSize(srcValue.Length);
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, IHeapObject srcValue, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            var garnetObject = (IGarnetObject)srcValue;
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, garnetObject, upsertInfo.Version, upsertInfo.SessionID);

            // TODO: Need to track original length as well, if it was overflow, and add overflow here as well as object size
            functionsState.objectStoreSizeTracker?.AddTrackedSize(srcValue.Size);
        }

        /// <inheritdoc />
        public void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ref TSourceLogRecord inputLogRecord, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                if (!inputLogRecord.Info.ValueIsObject)
                    WriteLogUpsert(logRecord.Key, ref input, logRecord.ValueSpan, upsertInfo.Version, upsertInfo.SessionID);
                else
                    WriteLogUpsert(logRecord.Key, ref input, (IGarnetObject)logRecord.ValueObject, upsertInfo.Version, upsertInfo.SessionID);
            }

            // TODO: Need to track original length as well, if it was overflow, and add overflow here as well as object size
            functionsState.objectStoreSizeTracker?.AddTrackedSize(size);
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ReadOnlySpan<byte> srcValue, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo)
        {
            var oldSize = logRecord.Info.ValueIsInline
                ? 0
                : (!logRecord.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.Size);

            _ = logRecord.TrySetValueSpan(srcValue, ref sizeInfo);
            if (!(input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1)))
                return false;
            sizeInfo.AssertOptionals(logRecord.Info);

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, srcValue, upsertInfo.Version, upsertInfo.SessionID);

            // TODO: Need to track original length as well, if it was overflow, and add overflow here as well as object size
            if (logRecord.Info.ValueIsOverflow)
                functionsState.objectStoreSizeTracker?.AddTrackedSize(srcValue.Length - oldSize);
            return true;
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, IHeapObject srcValue, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo)
        {
            var garnetObject = (IGarnetObject)srcValue;

            var oldSize = logRecord.Info.ValueIsInline
                ? 0
                : (!logRecord.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.Size);

            _ = logRecord.TrySetValueObject(srcValue, ref sizeInfo);
            if (!(input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1)))
                return false;
            sizeInfo.AssertOptionals(logRecord.Info);

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, garnetObject, upsertInfo.Version, upsertInfo.SessionID);

            functionsState.objectStoreSizeTracker?.AddTrackedSize(srcValue.Size - oldSize);
            return true;
        }

        /// <inheritdoc />
        public bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref ObjectInput input, ref TSourceLogRecord inputLogRecord, ref GarnetObjectStoreOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            var oldSize = logRecord.Info.ValueIsInline
                ? 0
                : (!logRecord.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.Size);

            _ = logRecord.TryCopyFrom(ref inputLogRecord, ref sizeInfo);
            if (!(input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1)))
                return false;
            sizeInfo.AssertOptionals(logRecord.Info);

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                if (!inputLogRecord.Info.ValueIsObject)
                    WriteLogUpsert(logRecord.Key, ref input, logRecord.ValueSpan, upsertInfo.Version, upsertInfo.SessionID);
                else
                    WriteLogUpsert(logRecord.Key, ref input, (IGarnetObject)logRecord.ValueObject, upsertInfo.Version, upsertInfo.SessionID);
            }

            var newSize = logRecord.Info.ValueIsInline
                ? 0
                : (!logRecord.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.Size);
            functionsState.objectStoreSizeTracker?.AddTrackedSize(newSize - oldSize);
            return true;
        }
    }
}