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
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert(ReadOnlySpan<byte> key, ref UnifiedStoreInput input, ReadOnlySpan<byte> value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode)
                return;

            input.header.flags |= RespInputFlags.Deterministic;

            if (functionsState.appendOnlyFile.Log.Size == 1)
            {
                functionsState.appendOnlyFile.Log.SigleLog.Enqueue(
                    new AofHeader
                    {
                        opType = AofEntryType.UnifiedStoreStringUpsert,
                        storeVersion = version,
                        sessionID = sessionID
                    },
                key, value, out _);
            }
            else
            {
                var extendedAofHeader = new AofExtendedHeader(
                    new AofHeader
                    {
                        opType = AofEntryType.UnifiedStoreStringUpsert,
                        storeVersion = version,
                        sessionID = sessionID
                    },
                    functionsState.appendOnlyFile.seqNumGen.GetSequenceNumber(),
                    0);

                functionsState.appendOnlyFile.Log.Enqueue(extendedAofHeader, key, value, out _);
            }
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
                if (functionsState.appendOnlyFile.Log.Size == 1)
                {
                    functionsState.appendOnlyFile.Log.SigleLog.Enqueue(
                        new AofHeader
                        {
                            opType = AofEntryType.UnifiedStoreObjectUpsert,
                            storeVersion = version,
                            sessionID = sessionID
                        },
                    key, new ReadOnlySpan<byte>(valPtr, valueBytes.Length), out _);
                }
                else
                {
                    var extendedAofHeader = new AofExtendedHeader(
                        new AofHeader
                        {
                            opType = AofEntryType.UnifiedStoreObjectUpsert,
                            storeVersion = version,
                            sessionID = sessionID
                        },
                        functionsState.appendOnlyFile.seqNumGen.GetSequenceNumber(),
                        0);

                    functionsState.appendOnlyFile.Log.Enqueue(extendedAofHeader, key, new ReadOnlySpan<byte>(valPtr, valueBytes.Length), out _);
                }
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

            if (functionsState.appendOnlyFile.Log.Size == 1)
            {
                functionsState.appendOnlyFile.Log.SigleLog.Enqueue(
                    new AofHeader
                    {
                        opType = AofEntryType.UnifiedStoreDelete,
                        storeVersion = version,
                        sessionID = sessionID
                    }, key, item2: default, out _);
            }
            else
            {
                var extendedAofHeader = new AofExtendedHeader(
                    new AofHeader
                    {
                        opType = AofEntryType.UnifiedStoreDelete,
                        storeVersion = version,
                        sessionID = sessionID
                    },
                    functionsState.appendOnlyFile.seqNumGen.GetSequenceNumber(),
                    0);

                functionsState.appendOnlyFile.Log.Enqueue(extendedAofHeader, key, value: default, out _);
            }
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

            if (functionsState.appendOnlyFile.Log.Size == 1)
            {
                functionsState.appendOnlyFile.Log.SigleLog.Enqueue(
                    new AofHeader
                    {
                        opType = AofEntryType.UnifiedStoreRMW,
                        storeVersion = version,
                        sessionID = sessionId
                    },
                key, ref input, out _);
            }
            else
            {
                var extendedAofHeader = new AofExtendedHeader(
                    new AofHeader
                    {
                        opType = AofEntryType.UnifiedStoreRMW,
                        storeVersion = version,
                        sessionID = sessionId
                    },
                    functionsState.appendOnlyFile.seqNumGen.GetSequenceNumber(),
                    0);

                functionsState.appendOnlyFile.Log.Enqueue(extendedAofHeader, key, ref input, out _);
            }
        }

        bool EvaluateExpireCopyUpdate(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ExpireOption optionType, bool expiryExisted, long newExpiry, ReadOnlySpan<byte> newValue, ref GarnetUnifiedStoreOutput output)
        {
            // TODO ETag?
            if (!logRecord.TrySetValueSpanAndPrepareOptionals(newValue, in sizeInfo))
            {
                functionsState.logger?.LogError("Failed to set value in {methodName}", "EvaluateExpireCopyUpdate");
                return false;
            }

            return TrySetRecordExpiration(ref logRecord, optionType, expiryExisted, newExpiry, ref output);
        }

        bool EvaluateExpireInPlace(ref LogRecord logRecord, ExpireOption optionType, bool expiryExisted, long newExpiry, ref GarnetUnifiedStoreOutput output)
        {
            Debug.Assert(output.SpanByteAndMemory.IsSpanByte, "This code assumes it is called in-place and did not go pending");

            return TrySetRecordExpiration(ref logRecord, optionType, expiryExisted, newExpiry, ref output);
        }

        bool TrySetRecordExpiration(ref LogRecord logRecord, ExpireOption optionType, bool expiryExisted, long newExpiry, ref GarnetUnifiedStoreOutput output)
        {
            var o = (OutputHeader*)output.SpanByteAndMemory.SpanByte.ToPointer();
            o->result1 = 0;

            if (expiryExisted)
            {
                // Expiration already exists so there is no need to check for space (i.e. failure of TrySetExpiration)
                switch (optionType)
                {
                    case ExpireOption.NX:
                        return true;
                    case ExpireOption.XX:
                    case ExpireOption.None:
                        _ = logRecord.TrySetExpiration(newExpiry);
                        o->result1 = 1;
                        return true;
                    case ExpireOption.GT:
                    case ExpireOption.XXGT:
                        if (newExpiry > logRecord.Expiration)
                        {
                            _ = logRecord.TrySetExpiration(newExpiry);
                            o->result1 = 1;
                        }
                        return true;
                    case ExpireOption.LT:
                    case ExpireOption.XXLT:
                        if (newExpiry < logRecord.Expiration)
                        {
                            _ = logRecord.TrySetExpiration(newExpiry);
                            o->result1 = 1;
                        }
                        return true;
                    default:
                        throw new GarnetException($"EvaluateExpireCopyUpdate exception when expiryExists is false: optionType{optionType}");
                }
            }

            // No expiration yet.
            switch (optionType)
            {
                case ExpireOption.NX:
                case ExpireOption.None:
                case ExpireOption.LT:   // If expiry doesn't exist, LT should treat the current expiration as infinite
                    if (!logRecord.TrySetExpiration(newExpiry))
                    {
                        functionsState.logger?.LogError("Failed to add expiration in {methodName}.{caseName}", "EvaluateExpireCopyUpdate", "LT");
                        return false;
                    }
                    o->result1 = 1;
                    return true;
                case ExpireOption.XX:
                case ExpireOption.GT:
                case ExpireOption.XXGT:
                case ExpireOption.XXLT:
                    return true;
                default:
                    throw new GarnetException($"EvaluateExpireCopyUpdate exception when expiryExists is true: optionType{optionType}");
            }
        }
    }
}