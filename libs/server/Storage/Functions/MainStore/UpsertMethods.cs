// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool SingleWriter(ref SpanByte key, ref RawStringInput input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
        {
            // Since upsert may be on existing key we need to wipe out the record info property
            recordInfo.ClearHasETag();
            return SpanByteFunctions<RawStringInput, SpanByteAndMemory, long>.DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo, input.arg1);
        }

        /// <inheritdoc />
        public void PostSingleWriter(ref SpanByte key, ref RawStringInput input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
            if (reason == WriteReason.Upsert && functionsState.appendOnlyFile != null)
                upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
        }

        /// <inheritdoc />
        public bool ConcurrentWriter(ref SpanByte key, ref RawStringInput input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
        {
            // Since upsert may be on existing key we need to wipe out the record info property
            recordInfo.ClearHasETag();
            if (ConcurrentWriterWorker(ref src, ref dst, ref input, ref upsertInfo, ref recordInfo))
            {
                if (!upsertInfo.RecordInfo.Modified)
                    functionsState.watchVersionMap.IncrementVersion(upsertInfo.KeyHash);
                if (functionsState.appendOnlyFile != null)
                    upsertInfo.UserData |= NeedAofLog; // Mark that we need to write to AOF
                return true;
            }
            return false;
        }

        static bool ConcurrentWriterWorker(ref SpanByte src, ref SpanByte dst, ref RawStringInput input, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            => SpanByteFunctions<RawStringInput, SpanByteAndMemory, long>.DoSafeCopy(ref src, ref dst, ref upsertInfo, ref recordInfo, input.arg1);

        /// <inheritdoc />
        public void PostUpsertOperation<TEpochAccessor>(ref SpanByte key, ref RawStringInput input, ref SpanByte src, ref UpsertInfo upsertInfo, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if ((upsertInfo.UserData & NeedAofLog) == NeedAofLog) // Check if we need to write to AOF
            {
                WriteLogUpsert(ref key, ref input, ref src, upsertInfo.Version, upsertInfo.SessionID, epochAccessor);
            }
        }
    }
}