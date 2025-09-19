// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Unified store functions
    /// </summary>
    public readonly unsafe partial struct UnifiedSessionFunctions : ISessionFunctions<UnifiedStoreInput, GarnetUnifiedStoreOutput, long>
    {
        /// <summary>
        ///  Logging Delete from
        ///  a. InPlaceDeleter
        ///  b. PostInitialDeleter
        /// </summary>
        void WriteLogDelete(ReadOnlySpan<byte> key, long version, int sessionID)
        {
            if (functionsState.StoredProcMode)
                return;

            functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.UnifiedStoreDelete, storeVersion = version, sessionID = sessionID }, key, item2: default, out _);
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW(ReadOnlySpan<byte> key, ref UnifiedStoreInput input, long version, int sessionId)
        {
            if (functionsState.StoredProcMode) return;
            input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Enqueue(
                new AofHeader { opType = AofEntryType.UnifiedStoreRMW, storeVersion = version, sessionID = sessionId },
                key, ref input, out _);
        }

        bool EvaluateExpireCopyUpdate(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ExpireOption optionType, long newExpiry, ReadOnlySpan<byte> newValue, ref GarnetUnifiedStoreOutput output)
        {
            using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);

            // TODO ETag?
            if (!logRecord.TrySetValueSpan(newValue, in sizeInfo))
            {
                functionsState.logger?.LogError("Failed to set value in {methodName}", "EvaluateExpireCopyUpdate");
                writer.WriteZero();
                return false;
            }

            return TrySetRecordExpiration(ref logRecord, optionType, newExpiry, writer);
        }

        bool EvaluateObjectExpireInPlace(ref LogRecord logRecord, ExpireOption optionType, long newExpiry, ref GarnetUnifiedStoreOutput output)
        {
            Debug.Assert(output.SpanByteAndMemory.IsSpanByte, "This code assumes it is called in-place and did not go pending");

            using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
            return TrySetRecordExpiration(ref logRecord, optionType, newExpiry, writer);
        }

        bool TrySetRecordExpiration(ref LogRecord logRecord, ExpireOption optionType, long newExpiry, RespMemoryWriter writer)
        {
            var expiryExists = logRecord.Info.HasExpiration;

            if (expiryExists)
            {
                // Expiration already exists so there is no need to check for space (i.e. failure of TrySetExpiration)
                switch (optionType)
                {
                    case ExpireOption.NX:
                        writer.WriteZero();
                        return true;
                    case ExpireOption.XX:
                    case ExpireOption.None:
                        _ = logRecord.TrySetExpiration(newExpiry);
                        writer.WriteOne();
                        return true;
                    case ExpireOption.GT:
                    case ExpireOption.XXGT:
                        if (newExpiry > logRecord.Expiration)
                        {
                            _ = logRecord.TrySetExpiration(newExpiry);
                            writer.WriteOne();
                            return true;
                        }
                        writer.WriteZero();
                        return true;
                    case ExpireOption.LT:
                    case ExpireOption.XXLT:
                        if (newExpiry < logRecord.Expiration)
                        {
                            _ = logRecord.TrySetExpiration(newExpiry);
                            writer.WriteOne();
                            return true;
                        }
                        writer.WriteZero();
                        return true;
                    default:
                        throw new GarnetException($"EvaluateExpireCopyUpdate exception when expiryExists is false: optionType{optionType}");
                }
            }
            else
            {
                // No expiration yet. Because this is CopyUpdate we should already have verified the space, but check anyway
                switch (optionType)
                {
                    case ExpireOption.NX:
                    case ExpireOption.None:
                    case ExpireOption.LT:   // If expiry doesn't exist, LT should treat the current expiration as infinite
                        if (!logRecord.TrySetExpiration(newExpiry))
                        {
                            functionsState.logger?.LogError("Failed to add expiration in {methodName}.{caseName}", "EvaluateExpireCopyUpdate", "LT");
                            writer.WriteZero();
                            return false;
                        }
                        writer.WriteOne();
                        return true;
                    case ExpireOption.XX:
                    case ExpireOption.GT:
                    case ExpireOption.XXGT:
                    case ExpireOption.XXLT:
                        writer.WriteZero();
                        return true;
                    default:
                        throw new GarnetException($"EvaluateExpireCopyUpdate exception when expiryExists is true: optionType{optionType}");
                }
            }
        }
    }
}
