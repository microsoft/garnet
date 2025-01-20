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
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool SingleReader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref ObjectInput input, ref GarnetObjectStoreOutput dst, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord<IGarnetObject>
        {
            if (srcLogRecord.Info.HasExpiration && srcLogRecord.Expiration < DateTimeOffset.Now.UtcTicks)
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
                        var ttlValue = ConvertUtils.SecondsFromDiffUtcNowTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                        functionsState.CopyRespNumber(ttlValue, ref dst.spanByteAndMemory);
                        return true;
                    case GarnetObjectType.PTtl:
                        ttlValue = ConvertUtils.MillisecondsFromDiffUtcNowTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                        functionsState.CopyRespNumber(ttlValue, ref dst.spanByteAndMemory);
                        return true;

                    case GarnetObjectType.ExpireTime:
                        var expireTime = ConvertUtils.UnixTimeInSecondsFromTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                        functionsState.CopyRespNumber(expireTime, ref dst.spanByteAndMemory);
                        return true;
                    case GarnetObjectType.PExpireTime:
                        expireTime = ConvertUtils.UnixTimeInMillisecondsFromTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                        functionsState.CopyRespNumber(expireTime, ref dst.spanByteAndMemory);
                        return true;

                    default:
                        if ((byte)input.header.type < CustomCommandManager.TypeIdStartOffset)
                            return srcLogRecord.ValueObject.Operate(ref input, ref dst.spanByteAndMemory, out _, out _);

                        if (IncorrectObjectType(ref input, srcLogRecord.ValueObject, ref dst.spanByteAndMemory))
                            return true;

                        (IMemoryOwner<byte> Memory, int Length) outp = (dst.spanByteAndMemory.Memory, 0);
                        var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                        var result = customObjectCommand.Reader(srcLogRecord.Key, ref input, srcLogRecord.ValueObject, ref outp, ref readInfo);
                        dst.spanByteAndMemory.Memory = outp.Memory;
                        dst.spanByteAndMemory.Length = outp.Length;
                        return result;
                }
            }

            dst.garnetObject = srcLogRecord.ValueObject;
            return true;
        }

        /// <inheritdoc />
        public bool ConcurrentReader(ref LogRecord<IGarnetObject> srcLogRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref ReadInfo readInfo)
            => SingleReader(ref srcLogRecord, ref input, ref output, ref readInfo);
    }
}