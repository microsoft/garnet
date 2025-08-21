// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <summary>
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert(ReadOnlySpan<byte> key, ref ObjectInput input, ReadOnlySpan<byte> value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode)
                return;
            input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Enqueue(
                new AofHeader { opType = AofEntryType.ObjectStoreUpsert, storeVersion = version, sessionID = sessionID },
                key, value, out _);
        }

        /// <summary>
        /// Logging upsert from
        /// a. InPlaceWriter
        /// b. PostInitialWriter
        /// </summary>
        void WriteLogUpsert(ReadOnlySpan<byte> key, ref ObjectInput input, IGarnetObject value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode)
                return;
            input.header.flags |= RespInputFlags.Deterministic;

            GarnetObjectSerializer.Serialize(value, out var valueBytes);
            fixed (byte* valPtr = valueBytes)
            {
                functionsState.appendOnlyFile.Enqueue(
                    new AofHeader { opType = AofEntryType.ObjectStoreUpsert, storeVersion = version, sessionID = sessionID },
                    key, new ReadOnlySpan<byte>(valPtr, valueBytes.Length), out _);
            }
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW(ReadOnlySpan<byte> key, ref ObjectInput input, long version, int sessionID)
        {
            if (functionsState.StoredProcMode) return;
            input.header.flags |= RespInputFlags.Deterministic;

            functionsState.appendOnlyFile.Enqueue(
                new AofHeader { opType = AofEntryType.ObjectStoreRMW, storeVersion = version, sessionID = sessionID },
                key, ref input, out _);
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

            functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.ObjectStoreDelete, storeVersion = version, sessionID = sessionID }, key, item2: default, out _);
        }

        static bool EvaluateObjectExpireInPlace(ref LogRecord logRecord, ExpireOption optionType, long newExpiry, ref GarnetObjectStoreOutput output)
        {
            Debug.Assert(output.SpanByteAndMemory.IsSpanByte, "This code assumes it is called in-place and did not go pending");
            var o = (ObjectOutputHeader*)output.SpanByteAndMemory.SpanByte.ToPointer();
            o->result1 = 0;
            if (logRecord.Info.HasExpiration)
            {
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
                        throw new GarnetException($"EvaluateObjectExpireInPlace exception expiryExists: True, optionType {optionType}");
                }
            }
            else
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                    case ExpireOption.None:
                    case ExpireOption.LT:  // If expiry doesn't exist, LT should treat the current expiration as infinite, so the new value must be less
                        var ok = logRecord.TrySetExpiration(newExpiry);
                        o->result1 = 1;
                        return ok;
                    case ExpireOption.XX:
                    case ExpireOption.GT:  // If expiry doesn't exist, GT should treat the current expiration as infinite, so the new value cannot be greater
                    case ExpireOption.XXGT:
                    case ExpireOption.XXLT:
                        return true;
                    default:
                        throw new GarnetException($"EvaluateObjectExpireInPlace exception expiryExists: False, optionType {optionType}");
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CustomObjectFunctions GetCustomObjectCommand(ref ObjectInput input, GarnetObjectType type)
        {
            var cmdId = input.header.SubId;
            var customObjectCommand = functionsState.GetCustomObjectSubCommandFunctions((byte)type, cmdId);
            return customObjectCommand;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool IncorrectObjectType(ref ObjectInput input, IGarnetObject value, ref SpanByteAndMemory output)
        {
            var inputType = (byte)input.header.type;
            if (inputType != value.Type) // Indicates an incorrect type of key
            {
                output.Length = 0;
                return true;
            }

            return false;
        }
    }
}