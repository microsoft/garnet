// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Tsavorite.core;
using static Garnet.server.SessionFunctionsUtils;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedInput, UnifiedOutput, long>
    {
        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            ReadOnlySpan<byte> srcValue, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
        {
            if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo))
                return false;
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);
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
            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public bool InitialWriter<TSourceLogRecord>(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo,
            ref UnifiedInput input, in TSourceLogRecord inputLogRecord, ref UnifiedOutput output,
            ref UpsertInfo upsertInfo) where TSourceLogRecord : ISourceLogRecord
        {
            if (!dstLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo))
                return false;
            return true;
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            ReadOnlySpan<byte> srcValue, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF

        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref UnifiedInput input,
            IHeapObject srcValue, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
        {
            var garnetObject = (IGarnetObject)srcValue;
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF

        }

        /// <inheritdoc />
        public void PostInitialWriter<TSourceLogRecord>(ref LogRecord logRecord, in RecordSizeInfo sizeInfo,
            ref UnifiedInput input, in TSourceLogRecord inputLogRecord, ref UnifiedOutput output,
            ref UpsertInfo upsertInfo) where TSourceLogRecord : ISourceLogRecord
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF

        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceWriter(ref LogRecord logRecord, ref UnifiedInput input, ReadOnlySpan<byte> newValue, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
        {
            if (!InPlaceWriterForSpanValue(ref logRecord, ref input, newValue, ref output.SpanByteAndMemory, ref upsertInfo, this, functionsState, input.arg1))
                return false;
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceWriter(ref LogRecord logRecord, ref UnifiedInput input, IHeapObject newValue, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
        {
            if (!InPlaceWriterForHeapObjectValue(ref logRecord, ref input, newValue, ref output.SpanByteAndMemory, ref upsertInfo, this, functionsState, input.arg1))
                return false;
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }

        /// <inheritdoc />
        public bool InPlaceWriter<TSourceLogRecord>(ref LogRecord logRecord, ref UnifiedInput input,
            in TSourceLogRecord inputLogRecord, ref UnifiedOutput output, ref UpsertInfo upsertInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!InPlaceWriterForLogRecordValue(ref logRecord, ref input, in inputLogRecord, ref output.SpanByteAndMemory, ref upsertInfo, this, functionsState, input.arg1))
                return false;
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }

        /// <inheritdoc />
        public void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref UnifiedInput input, ReadOnlySpan<byte> valueSpan, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
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
        public void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref UnifiedInput input, IHeapObject valueObject, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
            if ((upsertInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
                WriteLogUpsert(key.KeyBytes, ref input, (IGarnetObject)valueObject, upsertInfo.Version, upsertInfo.SessionID, epochAccessor);
        }
    }
}