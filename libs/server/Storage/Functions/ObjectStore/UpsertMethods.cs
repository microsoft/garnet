// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using Tsavorite.core;
using static Garnet.server.SessionFunctionsUtils;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
    {
        /// <inheritdoc />
        public bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref UpsertInfo upsertInfo)
        {
            if (input.garnetObject != null)
            {
                if (!dstLogRecord.TrySetValueObjectAndPrepareOptionals(input.garnetObject, in sizeInfo))
                    return false;
            }
            else
            {
                if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(input.parseState.GetArgSliceByRef(0), in sizeInfo))
                    return false;
            }
            // TODO ETag
            if (input.arg1 != 0 && !dstLogRecord.TrySetExpiration(input.arg1))
                return false;
            sizeInfo.AssertOptionalsIfSet(dstLogRecord.Info);
            return true;
        }

        /// <inheritdoc />
        public void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref ObjectInput input, ref ObjectOutput output, ref UpsertInfo upsertInfo)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InPlaceWriter(ref LogRecord logRecord, ref ObjectInput input, ref ObjectOutput output, ref UpsertInfo upsertInfo)
        {
            if (input.garnetObject != null)
                return false; // Force copy-to-tail for direct object replacement
            if (!InPlaceWriterForSpanValue(ref logRecord, ref input, input.parseState.GetArgSliceByRef(0), ref output.SpanByteAndMemory, ref upsertInfo, this, functionsState, input.arg1))
                return false;
            if (functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
            return true;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref ObjectInput input, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
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