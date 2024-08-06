﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <summary>
        /// Logging upsert from
        /// a. ConcurrentWriter
        /// b. PostSingleWriter
        /// </summary>
        void WriteLogUpsert(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode) return;
            input.header.flags |= RespInputFlags.Deterministic;

            var valueBytes = GarnetObjectSerializer.Serialize(value);
            fixed (byte* ptr = key)
            {
                fixed (byte* valPtr = valueBytes)
                {
                    var keySB = SpanByte.FromPinnedPointer(ptr, key.Length);
                    var valSB = SpanByte.FromPinnedPointer(valPtr, valueBytes.Length);

                    functionsState.appendOnlyFile.Enqueue(
                        new AofHeader { opType = AofEntryType.ObjectStoreUpsert, version = version, sessionID = sessionID },
                        ref keySB, ref valSB, out _);
                }
            }
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW(ref byte[] key, ref ObjectInput input, long version, int sessionID)
        {
            if (functionsState.StoredProcMode) return;
            input.header.flags |= RespInputFlags.Deterministic;

            fixed (byte* ptr = key)
            {
                var sbKey = SpanByte.FromPinnedPointer(ptr, key.Length);
                var sbInput = new ArgSlice(input.ToPointer(), sizeof(ObjectInput)).SpanByte;
                var sbInputPayload = input.payload.SpanByte;
                functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.ObjectStoreRMW, version = version, sessionID = sessionID },
                    ref sbKey, ref sbInput, ref sbInputPayload, out _);
            }
        }

        /// <summary>
        ///  Logging Delete from
        ///  a. ConcurrentDeleter
        ///  b. PostSingleDeleter
        /// </summary>
        void WriteLogDelete(ref byte[] key, long version, int sessionID)
        {
            if (functionsState.StoredProcMode) return;
            fixed (byte* ptr = key)
            {
                var keySB = SpanByte.FromPinnedPointer(ptr, key.Length);
                SpanByte valSB = default;

                functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.ObjectStoreDelete, version = version, sessionID = sessionID }, ref keySB, ref valSB, out _);
            }
        }

        internal static bool CheckExpiry(IGarnetObject src) => src.Expiration < DateTimeOffset.UtcNow.Ticks;

        static void CopyRespNumber(long number, ref SpanByteAndMemory dst)
        {
            byte* curr = dst.SpanByte.ToPointer();
            byte* end = curr + dst.SpanByte.Length;
            if (RespWriteUtils.WriteInteger(number, ref curr, end, out var integerLen, out int totalLen))
            {
                dst.SpanByte.Length = (int)(curr - dst.SpanByte.ToPointer());
                return;
            }

            //handle resp buffer overflow here
            dst.ConvertToHeap();
            dst.Length = totalLen;
            dst.Memory = MemoryPool<byte>.Shared.Rent(totalLen);
            fixed (byte* ptr = dst.Memory.Memory.Span)
            {
                byte* cc = ptr;
                *cc++ = (byte)':';
                NumUtils.LongToBytes(number, (int)integerLen, ref cc);
                *cc++ = (byte)'\r';
                *cc++ = (byte)'\n';
            }
        }

        static void CopyDefaultResp(ReadOnlySpan<byte> resp, ref SpanByteAndMemory dst)
        {
            if (resp.Length < dst.SpanByte.Length)
            {
                resp.CopyTo(dst.SpanByte.AsSpan());
                dst.SpanByte.Length = resp.Length;
                return;
            }

            dst.ConvertToHeap();
            dst.Length = resp.Length;
            dst.Memory = MemoryPool<byte>.Shared.Rent(resp.Length);
            resp.CopyTo(dst.Memory.Memory.Span);
        }

        static bool EvaluateObjectExpireInPlace(ExpireOption optionType, bool expiryExists, long expiration, ref IGarnetObject value, ref GarnetObjectStoreOutput output)
        {
            Debug.Assert(output.spanByteAndMemory.IsSpanByte, "This code assumes it is called in-place and did not go pending");
            var o = (ObjectOutputHeader*)output.spanByteAndMemory.SpanByte.ToPointer();
            if (expiryExists)
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                        o->result1 = 0;
                        break;
                    case ExpireOption.XX:
                    case ExpireOption.None:
                        value.Expiration = expiration;
                        o->result1 = 1;
                        break;
                    case ExpireOption.GT:
                        bool replace = expiration < value.Expiration;
                        value.Expiration = replace ? value.Expiration : expiration;
                        if (replace)
                            o->result1 = 0;
                        else
                            o->result1 = 1;
                        break;
                    case ExpireOption.LT:
                        replace = expiration > value.Expiration;
                        value.Expiration = replace ? value.Expiration : expiration;
                        if (replace)
                            o->result1 = 0;
                        else
                            o->result1 = 1;
                        break;
                    default:
                        throw new GarnetException($"EvaluateObjectExpireInPlace exception expiryExists:{expiryExists}, optionType{optionType}");
                }
            }
            else
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                    case ExpireOption.None:
                        value.Expiration = expiration;
                        o->result1 = 1;
                        break;
                    case ExpireOption.XX:
                    case ExpireOption.GT:
                    case ExpireOption.LT:
                        o->result1 = 0;
                        break;
                    default:
                        throw new GarnetException($"EvaluateObjectExpireInPlace exception expiryExists:{expiryExists}, optionType{optionType}");
                }
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CustomObjectFunctions GetCustomObjectCommand(ref ObjectInput input, GarnetObjectType type)
        {
            var objectId = (byte)((byte)type - CustomCommandManager.StartOffset);
            var cmdId = input.header.SubId;
            var customObjectCommand = functionsState.customObjectCommands[objectId].commandMap[cmdId].functions;
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