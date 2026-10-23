// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Diagnostics;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using static Garnet.server.SessionFunctionsUtils;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedInput, UnifiedOutput, long>
    {
        /// <summary>
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert<TEpochAccessor>(ReadOnlySpan<byte> key, ref UnifiedInput input, ReadOnlySpan<byte> value, long version, int sessionID, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if (functionsState.StoredProcMode)
                return;

            // We need this check because when we ingest records from the primary
            // if the input is zero then input overlaps with value so any update to RespInputHeader->flags
            // will incorrectly modify the total length of value.
            if (input.SerializedLength > 0)
                input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Log.Enqueue(
                AofEntryType.UnifiedStoreStringUpsert, version, sessionID, key, value, ref input, epochAccessor, out _);
        }

        void WriteV5ImportedReplacement<TEpochAccessor>(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, long entityTag, long expiration,
            long version, int sessionId, TEpochAccessor epochAccessor) where TEpochAccessor : IEpochAccessor
        {
            GarnetLog log = functionsState.appendOnlyFile.Log;
            bool ownsTransaction = !functionsState.HasEnclosingTransaction;
            BitVector[] vectors = null;
            ulong physicalAccess = 0;
            if (ownsTransaction)
            {
                int physical = log.GetPhysicalSublogIdx(key);
                physicalAccess = 1UL << physical;
                vectors = new BitVector[log.Size];
                vectors[physical] = new BitVector(AofShardedLogTransactionHeader.ReplayTaskAccessVectorBytes);
                _ = vectors[physical].SetBit(log.GetReplayTaskIdx(key));
                log.EnqueueTxn(AofEntryType.TxnStart, version, sessionId, physicalAccess, vectors, 1);
            }
            log.Enqueue(AofEntryType.UnifiedStoreDelete, version, sessionId, key, default, epochAccessor, out _);
            if (entityTag == 0)
            {
                StringInput input = new(RespCommand.SET, RespInputFlags.Deterministic, expiration);
                log.Enqueue(AofEntryType.StoreUpsert, version, sessionId, key, value, ref input, epochAccessor, out _);
            }
            else
            {
                Span<byte> tag = stackalloc byte[20];
                if (!Utf8Formatter.TryFormat(entityTag, tag, out int tagLength))
                    throw new GarnetException("Failed to format imported entity tag");
                fixed (byte* valuePointer = value)
                fixed (byte* tagPointer = tag)
                {
                    StringInput input = new(RespCommand.SETIFGREATER, RespInputFlags.Deterministic, expiration);
                    input.parseState.InitializeWithArguments(PinnedSpanByte.FromPinnedPointer(valuePointer, value.Length),
                        PinnedSpanByte.FromPinnedPointer(tagPointer, tagLength));
                    log.Enqueue(AofEntryType.StoreRMW, version, sessionId, key, ref input, epochAccessor, out _);
                }
            }
            if (ownsTransaction)
                log.EnqueueTxn(AofEntryType.TxnCommit, version, sessionId, physicalAccess, vectors, 1);
        }

        /// <summary>
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert<TKey, TEpochAccessor>(TKey key, ref UnifiedInput input, IGarnetObject value, long version, int sessionID, TEpochAccessor epochAccessor)
            where TKey : IKey, allows ref struct
            where TEpochAccessor : IEpochAccessor
        {
            if (functionsState.StoredProcMode)
                return;

            input.header.flags |= RespInputFlags.Deterministic;

            // Object serialization is deferred to the AOF: GarnetLog.EnqueueObjectChunked streams the object into chunk
            // records via the ChunkedObjectSerializer without materializing the whole serialized value. The store epoch
            // (epochAccessor) is suspended only while blocked on an AOF flush during chunk allocation.
            functionsState.appendOnlyFile.Log.EnqueueObjectChunked(
                AofEntryType.UnifiedStoreObjectUpsert,
                version,
                sessionID,
                key,
                value,
                ref input,
                functionsState.garnetObjectSerializer,
                epochAccessor,
                out _);
        }

        /// <summary>
        ///  Logging Delete from
        ///  a. InPlaceDeleter
        ///  b. PostInitialDeleter
        /// </summary>
        void WriteLogDelete<TEpochAccessor>(ReadOnlySpan<byte> key, long version, int sessionID, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if (functionsState.StoredProcMode)
                return;

            functionsState.appendOnlyFile.Log.Enqueue(
                AofEntryType.UnifiedStoreDelete,
                version,
                sessionID,
                key,
                value: default,
                epochAccessor,
                out _);
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW<TEpochAccessor>(ReadOnlySpan<byte> key, ref UnifiedInput input, long version, int sessionId, TEpochAccessor epochAccessor)
            where TEpochAccessor : IEpochAccessor
        {
            if (functionsState.StoredProcMode) return;
            input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Log.Enqueue(
                AofEntryType.UnifiedStoreRMW,
                version,
                sessionId,
                key,
                ref input,
                epochAccessor,
                out _);
        }

        bool EvaluateExpireCopyUpdate(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ExpireOption optionType, long newExpiry, ReadOnlySpan<byte> newValue, ref UnifiedOutput output)
        {
            var hasExpiration = logRecord.DataHeader.HasExpiration;

            // TODO ETag?
            if (!logRecord.TrySetValueSpanAndPrepareOptionals(newValue, in sizeInfo))
            {
                functionsState.logger?.LogError("Failed to set value in {methodName}", nameof(EvaluateExpireCopyUpdate));
                return false;
            }

            var isSuccessful = EvaluateExpire(ref logRecord, optionType, newExpiry, hasExpiration,
                logErrorOnFail: true, functionsState.logger, out var expirationChanged);

            functionsState.CopyDefaultResp(
                isSuccessful && expirationChanged ? CmdStrings.RESP_RETURN_VAL_1 : CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);

            return isSuccessful;
        }

        IPUResult EvaluateExpireInPlace(ref LogRecord logRecord, ExpireOption optionType, long newExpiry, bool hasExpiration, ref UnifiedOutput output)
        {
            Debug.Assert(output.SpanByteAndMemory.IsSpanByte, "This code assumes it is called in-place and did not go pending");

            if (!EvaluateExpire(ref logRecord, optionType, newExpiry, hasExpiration, logErrorOnFail: false, functionsState.logger, out var expirationChanged))
                return IPUResult.Failed;

            if (expirationChanged)
            {
                functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_1, ref output.SpanByteAndMemory);
                return IPUResult.Succeeded;
            }
            functionsState.CopyDefaultResp(CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);
            return IPUResult.NotUpdated;
        }
    }
}