// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<StringInput, StringOutput, long>
    {
        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ReadOnlySpan<byte> srcValue, ref StringOutput output, ref UpsertInfo upsertInfo)
        {
            if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo))
                return false;
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, IHeapObject srcValue, ref StringOutput output, ref UpsertInfo upsertInfo)
            => throw new GarnetException("String store should not be called with IHeapObject");

        /// <inheritdoc />
        public bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, in TSourceLogRecord inputLogRecord, ref StringOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (inputLogRecord.Info.ValueIsObject)
                throw new GarnetException("String store should not be called with IHeapObject");
            return dstLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ReadOnlySpan<byte> srcValue, ref StringOutput output, ref UpsertInfo upsertInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, IHeapObject srcValue, ref StringOutput output, ref UpsertInfo upsertInfo)
            => throw new GarnetException("String store should not be called with IHeapObject");

        /// <inheritdoc />
        public void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, in TSourceLogRecord inputLogRecord, ref StringOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                Debug.Assert(!inputLogRecord.Info.ValueIsObject, "String store should not be called with IHeapObject");
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            }
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ReadOnlySpan<byte> srcValue, ref StringOutput output, ref UpsertInfo upsertInfo)
        {
            // If we are not adding an ETag or Expiration we don't need to grow the record size for any reason other than value growth,
            // so we can optimize this to just set the length and span. 
            if (logRecord.Info.ValueIsInline && (!input.header.CheckWithETagFlag() || logRecord.Info.HasETag) && (input.arg1 == 0 || logRecord.Info.HasExpiration))
            {
                var (valueAddress, valueLength) = logRecord.PinnedValueAddressAndLength;
                if (!logRecord.TrySetPinnedValueSpan(srcValue, valueAddress, ref valueLength))
                    return false;
            }
            else
            {
                // Create local sizeInfo
                var sizeInfo2 = new RecordSizeInfo() { FieldInfo = GetUpsertFieldInfo(logRecord, srcValue, ref input) };
                functionsState.storeWrapper.store.Log.PopulateRecordSizeInfo(ref sizeInfo2);

                if (!logRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo2))
                    return false;
            }

            // Update expiration
            if (input.arg1 != 0)
            {
                if (!logRecord.TrySetExpiration(input.arg1))
                    Debug.Fail("Should have succeeded in setting Expiration as we should have ensured there was space there already");
            }
            else if (logRecord.Info.HasExpiration)
                _ = logRecord.RemoveExpiration();

            if (input.header.CheckWithETagFlag())
            {
                var newETag = functionsState.etagState.ETag + 1;
                if (!logRecord.TrySetETag(newETag))
                    Debug.Fail("Should have succeeded in setting ETag as we should have ensured there was space there already");
                functionsState.CopyRespNumber(newETag, ref output.SpanByteAndMemory);
            }
            else
                _ = logRecord.RemoveETag();
            //sizeInfo2.AssertOptionals(logRecord.Info);

            if (!logRecord.Info.Modified)
                functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, IHeapObject srcValue, ref StringOutput output, ref UpsertInfo upsertInfo)
            => throw new GarnetException("String store should not be called with IHeapObject");

        /// <inheritdoc />
        public bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, in TSourceLogRecord inputLogRecord, ref StringOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (inputLogRecord.Info.ValueIsObject)
                throw new GarnetException("String store should not be called with IHeapObject");
            return logRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);
        }

        /// <inheritdoc />
        public void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref StringInput input, ReadOnlySpan<byte> valueSpan, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
            if ((upsertInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
                WriteLogUpsert(key.KeyBytes, ref input, valueSpan, upsertInfo.Version, upsertInfo.SessionID, epochAccessor);
        }

        /// <inheritdoc />
        public void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref StringInput input, IHeapObject valueObject, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
            throw new GarnetException("String store should not be called with IHeapObject");
        }
    }
}