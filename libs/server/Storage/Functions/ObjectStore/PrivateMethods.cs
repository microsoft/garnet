// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectStoreFunctions : IFunctions<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        /// <summary>
        /// Logging upsert from
        /// a. ConcurrentWriter
        /// b. PostSingleWriter
        /// </summary>
        void WriteLogUpsert(ref byte[] key, ref SpanByte input, ref IGarnetObject value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode) return;
            var header = (RespInputHeader*)input.ToPointer();
            header->flags |= RespInputFlags.Deterministic;
            fixed (byte* ptr = key)
            {
                var keySB = SpanByte.FromPointer(ptr, key.Length);
                functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.ObjectStoreUpsert, version = version, sessionID = sessionID }, ref keySB, ref input, out _);
            }
        }

        /// <summary>
        /// Logging RMW from
        /// a. PostInitialUpdater
        /// b. InPlaceUpdater
        /// c. PostCopyUpdater
        /// </summary>
        void WriteLogRMW(ref byte[] key, ref SpanByte input, ref IGarnetObject value, long version, int sessionID)
        {
            if (functionsState.StoredProcMode) return;
            var header = (RespInputHeader*)input.ToPointer();
            header->flags |= RespInputFlags.Deterministic;

            fixed (byte* ptr = key)
            {
                var keySB = SpanByte.FromPointer(ptr, key.Length);
                functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.ObjectStoreRMW, version = version, sessionID = sessionID }, ref keySB, ref input, out _);
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
                var keySB = SpanByte.FromPointer(ptr, key.Length);
                SpanByte valSB = default;
                functionsState.appendOnlyFile.Enqueue(new AofHeader { opType = AofEntryType.ObjectStoreDelete, version = version, sessionID = sessionID }, ref keySB, ref valSB, out _);
            }
        }

        static void CopyRespNumber(long number, ref SpanByteAndMemory dst)
        {
            byte* curr = dst.SpanByte.ToPointer();
            byte* end = curr + dst.SpanByte.Length;
            int totalLen = 0;
            if (RespWriteUtils.WriteInteger(number, ref curr, end, out var integerLen, out totalLen))
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

        static bool EvaluateObjectExpireInPlace(ExpireOption optionType, bool expiryExists, ref SpanByte input, ref IGarnetObject value, ref GarnetObjectStoreOutput output)
        {
            ObjectOutputHeader* o = (ObjectOutputHeader*)output.spanByteAndMemory.SpanByte.ToPointer();
            if (expiryExists)
            {
                switch (optionType)
                {
                    case ExpireOption.NX:
                        o->countDone = 0;
                        break;
                    case ExpireOption.XX:
                    case ExpireOption.None:
                        value.Expiration = input.ExtraMetadata;
                        o->countDone = 1;
                        break;
                    case ExpireOption.GT:
                        bool replace = input.ExtraMetadata < value.Expiration;
                        value.Expiration = replace ? value.Expiration : input.ExtraMetadata;
                        if (replace)
                            o->countDone = 0;
                        else
                            o->countDone = 1;
                        break;
                    case ExpireOption.LT:
                        replace = input.ExtraMetadata > value.Expiration;
                        value.Expiration = replace ? value.Expiration : input.ExtraMetadata;
                        if (replace)
                            o->countDone = 0;
                        else
                            o->countDone = 1;
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
                        value.Expiration = input.ExtraMetadata;
                        o->countDone = 1;
                        break;
                    case ExpireOption.XX:
                    case ExpireOption.GT:
                    case ExpireOption.LT:
                        o->countDone = 0;
                        break;
                    default:
                        throw new GarnetException($"EvaluateObjectExpireInPlace exception expiryExists:{expiryExists}, optionType{optionType}");
                }
            }
            return true;
        }
    }
}