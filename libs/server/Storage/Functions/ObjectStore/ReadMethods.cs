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
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, GarnetObjectStoreOutput, long>
    {
        /// <inheritdoc />
        public bool Reader<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref ObjectInput input, ref GarnetObjectStoreOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
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
                        functionsState.CopyRespNumber(ttlValue, ref output.SpanByteAndMemory);
                        return true;
                    case GarnetObjectType.PTtl:
                        ttlValue = ConvertUtils.MillisecondsFromDiffUtcNowTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                        functionsState.CopyRespNumber(ttlValue, ref output.SpanByteAndMemory);
                        return true;

                    case GarnetObjectType.ExpireTime:
                        var expireTime = ConvertUtils.UnixTimeInSecondsFromTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                        functionsState.CopyRespNumber(expireTime, ref output.SpanByteAndMemory);
                        return true;
                    case GarnetObjectType.PExpireTime:
                        expireTime = ConvertUtils.UnixTimeInMillisecondsFromTicks(srcLogRecord.Info.HasExpiration ? srcLogRecord.Expiration : -1);
                        functionsState.CopyRespNumber(expireTime, ref output.SpanByteAndMemory);
                        return true;

                    default:
                        if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
                        {
                            var opResult = ((IGarnetObject)srcLogRecord.ValueObject).Operate(ref input, ref output, out _);
                            if (output.HasWrongType)
                                return true;

                            return opResult;
                        }

                        if (IncorrectObjectType(ref input, (IGarnetObject)srcLogRecord.ValueObject, ref output.SpanByteAndMemory))
                        {
                            output.OutputFlags |= ObjectStoreOutputFlags.WrongType;
                            return true;
                        }

                        (IMemoryOwner<byte> Memory, int Length) outp = (output.SpanByteAndMemory.Memory, 0);
                        var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                        var result = customObjectCommand.Reader(srcLogRecord.Key, ref input, (IGarnetObject)srcLogRecord.ValueObject, ref outp, ref readInfo);
                        output.SpanByteAndMemory.Memory = outp.Memory;
                        output.SpanByteAndMemory.Length = outp.Length;
                        return result;
                }
            }

            output.GarnetObject = (IGarnetObject)srcLogRecord.ValueObject;
            return true;
        }
    }
}