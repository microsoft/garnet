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
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            if (!dstLogRecord.TrySetValueSpan(srcValue, ref sizeInfo))
                return false;
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, IHeapObject srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
            => throw new GarnetException("String store should not be called with IHeapObject");

        /// <inheritdoc />
        public bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref TSourceLogRecord inputLogRecord, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (inputLogRecord.Info.ValueIsObject)
                throw new GarnetException("String store should not be called with IHeapObject");
            return dstLogRecord.TryCopyFrom(ref inputLogRecord, ref sizeInfo);
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, srcValue, upsertInfo.Version, upsertInfo.SessionID);
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, IHeapObject srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
            => throw new GarnetException("String store should not be called with IHeapObject");

        /// <inheritdoc />
        public void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref TSourceLogRecord inputLogRecord, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
            where TSourceLogRecord : ISourceLogRecord
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
            {
                Debug.Assert(!inputLogRecord.Info.ValueIsObject, "String store should not be called with IHeapObject");
                WriteLogUpsert(logRecord.Key, ref input, inputLogRecord.ValueSpan, upsertInfo.Version, upsertInfo.SessionID);
            }
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            if (!logRecord.TrySetValueSpan(srcValue, ref sizeInfo))
                return false;
            var ok = input.arg1 == 0 ? logRecord.RemoveExpiration() : logRecord.TrySetExpiration(input.arg1);
            if (ok)
            {
                if (input.header.CheckWithETagFlag())
                {
                    var newETag = functionsState.etagState.ETag + 1;
                    ok = logRecord.TrySetETag(newETag);
                    if (ok)
                        functionsState.CopyRespNumber(newETag, ref output);
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
                    WriteLogUpsert(logRecord.Key, ref input, srcValue, upsertInfo.Version, upsertInfo.SessionID);
                return true;
            }
            return false;
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, IHeapObject srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            => throw new GarnetException("String store should not be called with IHeapObject");

        /// <inheritdoc />
        public bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, ref RecordSizeInfo sizeInfo, ref RawStringInput input, ref TSourceLogRecord inputLogRecord, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (inputLogRecord.Info.ValueIsObject)
                throw new GarnetException("String store should not be called with IHeapObject");
            return logRecord.TryCopyFrom(ref inputLogRecord, ref sizeInfo);
        }
    }
}