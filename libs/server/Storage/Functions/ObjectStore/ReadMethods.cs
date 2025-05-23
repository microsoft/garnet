// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
                switch (input.header.type)
                {
                    case GarnetObjectType.Ttl:
                        var ttlValue = ConvertUtils.SecondsFromDiffUtcNowTicks(value.Expiration > 0 ? value.Expiration : -1);
                        CopyRespNumber(ttlValue, ref dst.SpanByteAndMemory);
                        return true;
                    case GarnetObjectType.PTtl:
                        ttlValue = ConvertUtils.MillisecondsFromDiffUtcNowTicks(value.Expiration > 0 ? value.Expiration : -1);
                        CopyRespNumber(ttlValue, ref dst.SpanByteAndMemory);
                        return true;

                    case GarnetObjectType.ExpireTime:
                        var expireTime = ConvertUtils.UnixTimeInSecondsFromTicks(value.Expiration > 0 ? value.Expiration : -1);
                        CopyRespNumber(expireTime, ref dst.SpanByteAndMemory);
                        return true;
                    case GarnetObjectType.PExpireTime:
                        expireTime = ConvertUtils.UnixTimeInMillisecondsFromTicks(value.Expiration > 0 ? value.Expiration : -1);
                        CopyRespNumber(expireTime, ref dst.SpanByteAndMemory);
                        return true;

                    default:
                        if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
                        {
                            var opResult = value.Operate(ref input, ref dst, functionsState.respProtocolVersion, out _);
                            if (dst.HasWrongType)
                                return true;

                            return opResult;
                        }

                        if (IncorrectObjectType(ref input, value, ref dst.SpanByteAndMemory))
                        {
                            dst.OutputFlags |= ObjectStoreOutputFlags.WrongType;
                            return true;
                        }

                        var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                        var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref dst.SpanByteAndMemory);
                        try
                        {
                            var result = customObjectCommand.Reader(key, ref input, value, ref writer, ref readInfo);
                            return result;
                        }
                        finally
                        {
                            writer.Dispose();
                        }

                }
            }

            dst.GarnetObject = value;
            return true;
        }

        /// <inheritdoc />
        public bool ConcurrentReader(ref byte[] key, ref ObjectInput input, ref IGarnetObject value, ref GarnetObjectStoreOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            => SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);
    }
}