﻿// Copyright (c) Microsoft Corporation.
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
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            if (!dstLogRecord.TrySetValueSpan(srcValue, in sizeInfo))
                return false;
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionals(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, IHeapObject srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            => throw new GarnetException("String store should not be called with IHeapObject");

        /// <inheritdoc />
        public bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, in TSourceLogRecord inputLogRecord, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (inputLogRecord.Info.ValueIsObject)
                throw new GarnetException("String store should not be called with IHeapObject");
            return dstLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                WriteLogUpsert(logRecord.Key, ref input, srcValue, upsertInfo.Version, upsertInfo.SessionID);
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, IHeapObject srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            => throw new GarnetException("String store should not be called with IHeapObject");

        /// <inheritdoc />
        public void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, in TSourceLogRecord inputLogRecord, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
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
        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
        {
            if (!logRecord.TrySetValueSpan(srcValue, in sizeInfo))
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
        public bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, IHeapObject srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            => throw new GarnetException("String store should not be called with IHeapObject");

        /// <inheritdoc />
        public bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref RawStringInput input, in TSourceLogRecord inputLogRecord, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (inputLogRecord.Info.ValueIsObject)
                throw new GarnetException("String store should not be called with IHeapObject");
            return logRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);
        }
    }
}