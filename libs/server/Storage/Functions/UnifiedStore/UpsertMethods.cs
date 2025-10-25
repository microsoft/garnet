// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedStoreInput, GarnetUnifiedStoreOutput, long>
    {
        public bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ReadOnlySpan<byte> srcValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo)
        {
            if (!logRecord.TrySetValueSpan(srcValue, in sizeInfo))
                return false;
            if (input.arg1 != 0 && !logRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionals(logRecord.Info);
            return true;
        }

        public bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            IHeapObject srcValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo)
        {
            if (!logRecord.TrySetValueObject(srcValue, in sizeInfo))
                return false;
            // TODO ETag
            if (input.arg1 != 0 && !logRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionals(logRecord.Info);
            return true;
        }

        public bool InitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo,
            ref UnifiedStoreInput input,
            in TSourceLogRecord inputLogRecord, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
            => logRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);

        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ReadOnlySpan<byte> srcValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo)
        {
            if (logRecord.Info.ValueIsObject)
            {
                // TODO: This is called by readcache directly, but is the only ISessionFunctions call for that; the rest is internal. Clean this up, maybe as a new PostReadCacheInsert method.
                if (upsertInfo.Address == LogAddress.kInvalidAddress)
                {
                    functionsState.objectStoreSizeTracker?.AddReadCacheTrackedSize(
                        MemoryUtils.CalculateHeapMemorySize(in logRecord));
                    return;
                }
            }

            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, srcValue, upsertInfo.Version, upsertInfo.SessionID);

            if (logRecord.Info.ValueIsObject)
            {
                // TODO: Need to track original length as well, if it was overflow, and add overflow here as well as object size
                // TODO: Need to track lengths written to readcache, which is now internal in Tsavorite
                functionsState.objectStoreSizeTracker?.AddTrackedSize(
                    MemoryUtils.CalculateHeapMemorySize(in logRecord));
            }
        }

        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            IHeapObject srcValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo)
        {
            var garnetObject = (IGarnetObject)srcValue;
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, garnetObject, upsertInfo.Version, upsertInfo.SessionID);

            // TODO: Need to track original length as well, if it was overflow, and add overflow here as well as object size
            functionsState.objectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateHeapMemorySize(in logRecord));
        }

        public void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo,
            ref UnifiedStoreInput input, in TSourceLogRecord inputLogRecord, ref GarnetUnifiedStoreOutput output,
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

            if (logRecord.Info.ValueIsObject)
            {
                // TODO: Need to track original length as well, if it was overflow, and add overflow here as well as object size
                functionsState.objectStoreSizeTracker?.AddTrackedSize(MemoryUtils.CalculateHeapMemorySize(in logRecord));
            }
        }

        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            ReadOnlySpan<byte> newValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo)

        {
            if (logRecord.Info.ValueIsObject)
            {
                var oldSize = logRecord.Info.ValueIsInline
                    ? 0
                    : (!logRecord.Info.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.HeapMemorySize);

                _ = logRecord.TrySetValueSpan(newValue, in sizeInfo);
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

            if (!logRecord.TrySetValueSpan(newValue, in sizeInfo))
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

        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedStoreInput input,
            IHeapObject newValue, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo)
        {
            var garnetObject = (IGarnetObject)newValue;

            var oldSize = logRecord.Info.ValueIsInline
                ? 0
                : (!logRecord.Info.ValueIsObject ? logRecord.ValueSpan.Length : logRecord.ValueObject.HeapMemorySize);

            _ = logRecord.TrySetValueObject(newValue, in sizeInfo);
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

        public bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo,
            ref UnifiedStoreInput input, in TSourceLogRecord inputLogRecord, ref GarnetUnifiedStoreOutput output, ref UpsertInfo upsertInfo)
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
            functionsState.objectStoreSizeTracker?.AddTrackedSize(newSize - oldSize);
            return true;
        }
    }
}