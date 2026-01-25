// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedInput, UnifiedOutput, long>
    {
        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            ReadOnlySpan<byte> srcValue, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
        {
            if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo))
                return false;
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            IHeapObject srcValue, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
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
        public bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo,
            ref UnifiedInput input, in TSourceLogRecord inputLogRecord, ref UnifiedOutput output,
            ref UpsertInfo upsertInfo) where TSourceLogRecord : ISourceLogRecord
        {
            if (!dstLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo))
                return false;

            if (input.header.CheckWithETagFlag())
            {
                // If the old record had an ETag, we will replace it. Otherwise, we must have reserved space for it.
                Debug.Assert(sizeInfo.FieldInfo.HasETag, "CheckWithETagFlag specified but SizeInfo.HasETag is false");
                var newETag = functionsState.etagState.ETag + 1;
                dstLogRecord.TrySetETag(newETag);
            }
            return true;
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            ReadOnlySpan<byte> srcValue, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, srcValue, upsertInfo.Version, upsertInfo.SessionID);
            if (logRecord.Info.RecordHasObjects)
                functionsState.objectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateHeapMemorySize(in logRecord));
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            IHeapObject srcValue, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
        {
            var garnetObject = (IGarnetObject)srcValue;
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, garnetObject, upsertInfo.Version, upsertInfo.SessionID);
            if (logRecord.Info.RecordHasObjects)
                functionsState.objectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateHeapMemorySize(in logRecord));
        }

        /// <inheritdoc />
        public void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo,
            ref UnifiedInput input, in TSourceLogRecord inputLogRecord, ref UnifiedOutput output,
            ref UpsertInfo upsertInfo) where TSourceLogRecord : ISourceLogRecord
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
                functionsState.objectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateHeapMemorySize(in logRecord));
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            ReadOnlySpan<byte> newValue, ref UnifiedOutput output, ref UpsertInfo upsertInfo)

        {
            if (logRecord.Info.ValueIsObject)
            {
                var oldSize = logRecord.Info.ValueIsInline
                    ? 0
                    : (!logRecord.Info.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.HeapMemorySize);

                _ = logRecord.TrySetValueSpanAndPrepareOptionals(newValue, in sizeInfo);
                if (!(input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1)))
                    return false;
                sizeInfo.AssertOptionals(logRecord.Info);

                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    WriteLogUpsert(logRecord.Key, ref input, newValue, upsertInfo.Version, upsertInfo.SessionID);

                // TODO: Need to track original length as well, if it was overflow, and add overflow here as well as object size
                if (logRecord.Info.ValueIsOverflow)
                    functionsState.objectStoreSizeTracker?.AddTrackedSize(newValue.Length - oldSize);
                return true;
            }

            if (!logRecord.TrySetValueSpanAndPrepareOptionals(newValue, in sizeInfo))
                return false;
            var ok = input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1);
            if (ok)
            {
                if (input.header.CheckWithETagFlag())
                {
                    var newETag = functionsState.etagState.ETag + 1;
                    ok = logRecord.TrySetETag(newETag);
                    if (ok)
                    {
                        functionsState.CopyRespNumber(newETag, ref output.SpanByteAndMemory);
                    }
                }
                else
                    ok = logRecord.RemoveETag();
            }
            if (ok)
            {
                sizeInfo.AssertOptionals(logRecord.Info);
                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    WriteLogUpsert(logRecord.Key, ref input, newValue, upsertInfo.Version, upsertInfo.SessionID);
                return true;
            }
            return false;
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            IHeapObject newValue, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
        {
            var garnetObject = (IGarnetObject)newValue;

            var oldSize = logRecord.Info.ValueIsInline
                ? 0
                : (!logRecord.Info.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.HeapMemorySize);

            _ = logRecord.TrySetValueObjectAndPrepareOptionals(newValue, in sizeInfo);
            if (!(input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1)))
                return false;
            sizeInfo.AssertOptionals(logRecord.Info);

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, garnetObject, upsertInfo.Version, upsertInfo.SessionID);

            functionsState.objectStoreSizeTracker?.AddTrackedSize(newValue.HeapMemorySize - oldSize);
            return true;
        }

        /// <inheritdoc />
        public bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo,
            ref UnifiedInput input, in TSourceLogRecord inputLogRecord, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            var oldSize = logRecord.Info.ValueIsInline
                ? 0
                : (!logRecord.Info.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.HeapMemorySize);

            _ = logRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);

            var ok = input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1);
            if (ok)
            {
                if (input.header.CheckWithETagFlag())
                {
                    var newETag = functionsState.etagState.ETag + 1;
                    ok = logRecord.TrySetETag(newETag);
                    if (ok)
                    {
                        functionsState.CopyRespNumber(newETag, ref output.SpanByteAndMemory);
                    }
                }
                else
                    ok = logRecord.RemoveETag();
            }
            if (ok)
            {
                sizeInfo.AssertOptionals(logRecord.Info);

                if (!logRecord.Info.Modified)
                    functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                {
                    if (!inputLogRecord.Info.ValueIsObject)
                        WriteLogUpsert(logRecord.Key, ref input, logRecord.ValueSpan, upsertInfo.Version,
                            upsertInfo.SessionID);
                    else
                        WriteLogUpsert(logRecord.Key, ref input, (IGarnetObject)logRecord.ValueObject,
                            upsertInfo.Version, upsertInfo.SessionID);
                }

                var newSize = logRecord.Info.ValueIsInline
                    ? 0
                    : (!logRecord.Info.ValueIsObject
                        ? logRecord.ValueSpan.Length
                        : logRecord.ValueObject.HeapMemorySize);
                functionsState.objectStoreSizeTracker?.AddTrackedSize(newSize - oldSize);
                return true;
            }

            return false;
        }
    }
}