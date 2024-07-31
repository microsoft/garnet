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
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool SingleReader(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, ref GarnetObjectStoreOutput dst, ref ReadInfo readInfo)
        {
            if (value.Expiration > 0 && value.Expiration < DateTimeOffset.Now.UtcTicks)
            {
                // Do not set 'value = null' or otherwise mark this; Reads should not update the database. We rely on consistently checking for expiration everywhere.
                readInfo.Action = ReadAction.Expire;
                return false;
            }

            if (input.header.type != 0)
            {
                if (input.header.type == GarnetObjectType.Ttl || input.header.type == GarnetObjectType.PTtl) // TTL command
                {
                    var ttlValue = input.header.type == GarnetObjectType.Ttl ?
                                    ConvertUtils.SecondsFromDiffUtcNowTicks(value.Expiration > 0 ? value.Expiration : -1) :
                                    ConvertUtils.MillisecondsFromDiffUtcNowTicks(value.Expiration > 0 ? value.Expiration : -1);
                    CopyRespNumber(ttlValue, ref dst.spanByteAndMemory);
                    return true;
                }

                if ((byte)input.header.type < CustomCommandManager.StartOffset)
                    return value.Operate(ref input, ref dst.spanByteAndMemory, out _, out _);

                if (IncorrectObjectType(ref input, value, ref dst.spanByteAndMemory))
                    return true;

                (IMemoryOwner<byte> Memory, int Length) outp = (dst.spanByteAndMemory.Memory, 0);
                var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                var result = customObjectCommand.Reader(key, input.payload.ReadOnlySpan, value, ref outp, ref readInfo);
                dst.spanByteAndMemory.Memory = outp.Memory;
                dst.spanByteAndMemory.Length = outp.Length;
                return result;
            }

            dst.garnetObject = value;
            return true;
        }

        /// <inheritdoc />
        public bool ConcurrentReader(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, ref GarnetObjectStoreOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            => SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);
    }
}