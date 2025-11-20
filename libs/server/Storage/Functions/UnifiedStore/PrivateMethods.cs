// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedStoreInput, UnifiedStoreOutput, long>
    {
        /// <summary>
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert(ReadOnlySpan<byte> key, ref UnifiedStoreInput input, ReadOnlySpan<byte> value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode)
                return;

            // We need this check because when we ingest records from the primary
            // if the input is zero then input overlaps with value so any update to RespInputHeader->flags
            // will incorrectly modify the total length of value.
            if (input.SerializedLength > 0)
                input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Enqueue(
                new AofHeader { opType = AofEntryType.UnifiedStoreStringUpsert, storeVersion = version, sessionID = sessionID },
                key, value, out _);
        }

        /// <summary>
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert(ReadOnlySpan<byte> key, ref UnifiedStoreInput input, IGarnetObject value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode)
                return;

            input.header.flags |= RespInputFlags.Deterministic;

            GarnetObjectSerializer.Serialize(value, out var valueBytes);
            fixed (byte* valPtr = valueBytes)
            {
                functionsState.appendOnlyFile.Enqueue(
                    new AofHeader { opType = AofEntryType.UnifiedStoreObjectUpsert, storeVersion = version, sessionID = sessionID },
                    key, new ReadOnlySpan<byte>(valPtr, valueBytes.Length), out _);
            }
        }

        /// <summary>
        ///  Logging Delete from
        ///  a. InPlaceDeleter
        ///  b. PostInitialDeleter
        /// </summary>
        void WriteLogDelete(ReadOnlySpan<byte> key, long version, int sessionID)
        {
            if (functionsState.StoredProcMode)
                return;

            functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.UnifiedStoreDelete, storeVersion = version, sessionID = sessionID },
                key, item2: default, out _);
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW(ReadOnlySpan<byte> key, ref UnifiedStoreInput input, long version, int sessionId)
        {
            if (functionsState.StoredProcMode)
                return;

            input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Enqueue(
                new AofHeader { opType = AofEntryType.UnifiedStoreRMW, storeVersion = version, sessionID = sessionId },
                key, ref input, out _);
        }

        bool EvaluateExpireCopyUpdate(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ExpireOption optionType, long newExpiry, ReadOnlySpan<byte> newValue, ref UnifiedStoreOutput output)
        {
            var hasExpiration = logRecord.Info.HasExpiration;

            // TODO ETag?
            if (!logRecord.TrySetValueSpanAndPrepareOptionals(newValue, in sizeInfo))
            {
                functionsState.logger?.LogError("Failed to set value in {methodName}", nameof(EvaluateExpireCopyUpdate));
                return false;
            }

            var isSuccessful = SessionFunctionsUtils.EvaluateExpire(ref logRecord, optionType, newExpiry, hasExpiration,
                logErrorOnFail: true, functionsState.logger, out var expirationChanged);

            functionsState.CopyDefaultResp(
                isSuccessful && expirationChanged ? CmdStrings.RESP_RETURN_VAL_1 : CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);

            return isSuccessful;
        }

        bool EvaluateExpireInPlace(ref LogRecord logRecord, ExpireOption optionType, long newExpiry, bool hasExpiration, ref UnifiedStoreOutput output)
        {
            Debug.Assert(output.SpanByteAndMemory.IsSpanByte, "This code assumes it is called in-place and did not go pending");

            var isSuccessful = SessionFunctionsUtils.EvaluateExpire(ref logRecord, optionType, newExpiry, hasExpiration,
                logErrorOnFail: false, functionsState.logger, out var expirationChanged);

            if (isSuccessful)
            {
                functionsState.CopyDefaultResp(
                    expirationChanged ? CmdStrings.RESP_RETURN_VAL_1 : CmdStrings.RESP_RETURN_VAL_0, ref output.SpanByteAndMemory);
            }

            return isSuccessful;
        }
    }
}