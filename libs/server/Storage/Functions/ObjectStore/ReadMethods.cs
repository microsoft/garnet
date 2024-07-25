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
    public readonly unsafe partial struct ObjectStoreFunctions : ISessionFunctions<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool SingleReader(ref byte[] key, ref SpanByte input, ref IGarnetObject value, ref GarnetObjectStoreOutput dst, ref ReadInfo readInfo)
        {
            if (value.Expiration > 0 && value.Expiration < DateTimeOffset.Now.UtcTicks)
            {
                // Do not set 'value = null' or otherwise mark this; Reads should not update the database. We rely on consistently checking for expiration everywhere.
                readInfo.Action = ReadAction.Expire;
                return false;
            }

            if (input.Length != 0)
            {
                var header = (RespInputHeader*)input.ToPointer();
                if (header->type == GarnetObjectType.Ttl || header->type == GarnetObjectType.PTtl) // TTL command
                {
                    long ttlValue = header->type == GarnetObjectType.Ttl ?
                                    ConvertUtils.SecondsFromDiffUtcNowTicks(value.Expiration > 0 ? value.Expiration : -1) :
                                    ConvertUtils.MillisecondsFromDiffUtcNowTicks(value.Expiration > 0 ? value.Expiration : -1);
                    CopyRespNumber(ttlValue, ref dst.spanByteAndMemory);
                    return true;
                }
                else if ((byte)header->type < CustomCommandManager.StartOffset)
                    return value.Operate(ref input, ref dst.spanByteAndMemory, out _, out _);
                else
                {
                    if (IncorrectObjectType(ref input, value, ref dst.spanByteAndMemory))
                        return true;

                    (IMemoryOwner<byte> Memory, int Length) outp = (dst.spanByteAndMemory.Memory, 0);
                    var customObjectCommand = GetCustomObjectCommand(ref input, header->type);
                    var result = customObjectCommand.Reader(key, input.AsReadOnlySpan()[RespInputHeader.Size..], value, ref outp, ref readInfo);
                    dst.spanByteAndMemory.Memory = outp.Memory;
                    dst.spanByteAndMemory.Length = outp.Length;
                    return result;
                }
            }

            dst.garnetObject = value;
            return true;
        }

        /// <inheritdoc />
        public bool ConcurrentReader(ref byte[] key, ref SpanByte input, ref IGarnetObject value, ref GarnetObjectStoreOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            => SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);
    }
}