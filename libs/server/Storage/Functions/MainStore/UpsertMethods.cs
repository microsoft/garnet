// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
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
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output, ref UpsertInfo upsertInfo)
        {
            if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(input.parseState.GetArgSliceByRef(0), in sizeInfo))
                return false;
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref StringInput input, ref StringOutput output, ref UpsertInfo upsertInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceWriter(ref LogRecord logRecord, ref StringInput input, ref StringOutput output, ref UpsertInfo upsertInfo)
        {
            // Prevent SET from overwriting VectorSet or RangeIndex stubs – normal string records have RecordType 0; skip all checks in that common case.
            var recordType = logRecord.RecordType;
            if (recordType != 0 && (recordType == VectorManager.RecordType || recordType == RangeIndexManager.RangeIndexRecordType))
            {
                upsertInfo.Action = UpsertAction.WrongType;
                return false;
            }

            if (!InPlaceWriterForSpanValue(ref logRecord, ref input, input.parseState.GetArgSliceByRef(0), ref output.SpanByteAndMemory, ref upsertInfo, this, functionsState, input.arg1))
                return false;
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref StringInput input, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TEpochAccessor : IEpochAccessor
        {
            if ((upsertInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
                WriteLogUpsert(key.KeyBytes, ref input, upsertInfo.Version, upsertInfo.SessionID, epochAccessor);
        }
    }
}