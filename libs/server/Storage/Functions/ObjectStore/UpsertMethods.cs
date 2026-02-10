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
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ReadOnlySpan<byte> srcValue, ref ObjectOutput output, ref UpsertInfo upsertInfo)
        {
            if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo))
                return false;
            // TODO ETag
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, IHeapObject srcValue, ref ObjectOutput output, ref UpsertInfo upsertInfo)
        {
            if (!dstLogRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo))
                return false;
            // TODO ETag
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, in TSourceLogRecord inputLogRecord, ref ObjectOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
            => dstLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ReadOnlySpan<byte> srcValue, ref ObjectOutput output, ref UpsertInfo upsertInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, srcValue, upsertInfo.Version, upsertInfo.SessionID);
            if (logRecord.Info.RecordHasObjects)
                functionsState.cacheSizeTracker?.AddHeapSize(MemoryUtils.CalculateHeapMemorySize(in logRecord));
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, IHeapObject srcValue, ref ObjectOutput output, ref UpsertInfo upsertInfo)
        {
            var garnetObject = (IGarnetObject)srcValue;
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, garnetObject, upsertInfo.Version, upsertInfo.SessionID);
            if (logRecord.Info.RecordHasObjects)
                functionsState.cacheSizeTracker?.AddHeapSize(MemoryUtils.CalculateHeapMemorySize(in logRecord));
        }

        /// <inheritdoc />
        public void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, in TSourceLogRecord inputLogRecord, ref ObjectOutput output, ref UpsertInfo upsertInfo)
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
            if (logRecord.Info.RecordHasObjects)
                functionsState.cacheSizeTracker?.AddHeapSize(MemoryUtils.CalculateHeapMemorySize(in logRecord));
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ReadOnlySpan<byte> srcValue, ref ObjectOutput output, ref UpsertInfo upsertInfo)
        {
            var oldSize = logRecord.Info.ValueIsInline
                ? 0
                : (!logRecord.Info.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.HeapMemorySize);

            _ = logRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo);
            if (!(input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1)))
                return false;
            sizeInfo.AssertOptionals(logRecord.Info);

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, srcValue, upsertInfo.Version, upsertInfo.SessionID);

            // TODO: Need to track original length as well, if it was overflow, and add overflow here as well as object size
            if (logRecord.Info.ValueIsOverflow)
                functionsState.cacheSizeTracker?.AddHeapSize(srcValue.Length - oldSize);
            return true;
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, IHeapObject srcValue, ref ObjectOutput output, ref UpsertInfo upsertInfo)
        {
            var garnetObject = (IGarnetObject)srcValue;

            var oldSize = logRecord.Info.ValueIsInline
                ? 0
                : (!logRecord.Info.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.HeapMemorySize);

            _ = logRecord.TrySetValueObjectAndPrepareOptionals(srcValue, in sizeInfo);
            if (!(input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1)))
                return false;
            sizeInfo.AssertOptionals(logRecord.Info);

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, garnetObject, upsertInfo.Version, upsertInfo.SessionID);

            functionsState.cacheSizeTracker?.AddHeapSize(srcValue.HeapMemorySize - oldSize);
            return true;
        }

        /// <inheritdoc />
        public bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, in TSourceLogRecord inputLogRecord, ref ObjectOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            var oldSize = logRecord.Info.ValueIsInline
                ? 0
                : (!logRecord.Info.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.HeapMemorySize);

            _ = logRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);
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
                : (!logRecord.Info.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.HeapMemorySize);
            functionsState.cacheSizeTracker?.AddHeapSize(newSize - oldSize);
            return true;
        }
    }
}