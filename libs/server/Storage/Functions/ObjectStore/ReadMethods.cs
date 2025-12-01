// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Object store functions
    /// </summary>
    public readonly unsafe partial struct ObjectSessionFunctions : ISessionFunctions<ObjectInput, ObjectOutput, long>
    {
        /// <inheritdoc />
        public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref ObjectInput input, ref ObjectOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (!srcLogRecord.Info.ValueIsObject)
            {
                readInfo.Action = ReadAction.WrongType;
                return false;
            }

            if (srcLogRecord.Info.HasExpiration && srcLogRecord.Expiration < DateTimeOffset.Now.UtcTicks)
            {
                // Do not set 'value = null' or otherwise mark this; Reads should not update the database. We rely on consistently checking for expiration everywhere.
                readInfo.Action = ReadAction.Expire;
                return false;
            }

            if (input.header.type != 0)
            {
                if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
                {
                    if (srcLogRecord.Info.HasETag)
                        ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);

                    var opResult = ((IGarnetObject)srcLogRecord.ValueObject).Operate(ref input, ref output, functionsState.respProtocolVersion, srcLogRecord.ETag, out _);

                    if (srcLogRecord.Info.HasETag)
                        ETagState.ResetState(ref functionsState.etagState);

                    return output.HasWrongType || opResult;
                }

                if (IncorrectObjectType(ref input, (IGarnetObject)srcLogRecord.ValueObject, ref output.SpanByteAndMemory))
                {
                    output.OutputFlags |= OutputFlags.WrongType;
                    return true;
                }

                if (srcLogRecord.Info.HasETag)
                {
                    functionsState.CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output.SpanByteAndMemory);
                    return true;
                }

                var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                try
                {
                    var result = customObjectCommand.Reader(srcLogRecord.Key, ref input, Unsafe.As<IGarnetObject>(srcLogRecord.ValueObject), ref writer, ref readInfo);
                    return result;
                }
                finally
                {
                    writer.Dispose();
                }
            }

            output.GarnetObject = (IGarnetObject)srcLogRecord.ValueObject;
            return true;
        }
    }
}