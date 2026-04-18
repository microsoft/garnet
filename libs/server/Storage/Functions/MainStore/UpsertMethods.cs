// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;
using static Garnet.server.SessionFunctionsUtils;

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
            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);
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

            // Account for value-side overflow heap on the freshly-inserted record.
            // Key overflow is tracked internally by Tsavorite via logSizeTracker.
            var heap = logRecord.GetValueHeapMemorySize();
            if (heap != 0)
                functionsState.cacheSizeTracker?.AddHeapSize(heap);
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

            var heap = logRecord.GetValueHeapMemorySize();
            if (heap != 0)
                functionsState.cacheSizeTracker?.AddHeapSize(heap);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceWriter(ref LogRecord logRecord, ref StringInput input, ReadOnlySpan<byte> srcValue, ref StringOutput output, ref UpsertInfo upsertInfo)
        {
            if (!InPlaceWriterForSpanValue(ref logRecord, ref input, srcValue, ref output.SpanByteAndMemory, ref upsertInfo, this, functionsState, input.header.CheckWithETagFlag(), input.arg1))
                return false;
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }

        /// <inheritdoc />
        public bool InPlaceWriter(ref LogRecord logRecord, ref StringInput input, IHeapObject srcValue, ref StringOutput output, ref UpsertInfo upsertInfo)
        {
            GarnetException.Throw("String store should not be called with IHeapObject");
            return false;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, ref StringInput input, in TSourceLogRecord inputLogRecord, ref StringOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (inputLogRecord.Info.ValueIsObject)
                GarnetException.Throw("String store should not be called with IHeapObject");
            if (!InPlaceWriterForLogRecordValue(ref logRecord, ref input, in inputLogRecord, ref output.SpanByteAndMemory, ref upsertInfo, this, functionsState, input.header.CheckWithETagFlag(), input.arg1))
                return false;
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
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