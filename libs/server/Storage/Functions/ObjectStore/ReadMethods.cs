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
                var garnetObject = (IGarnetObject)srcLogRecord.ValueObject;
                var srcRecordHasETag = srcLogRecord.Info.HasETag;

                if ((byte)input.header.type < CustomCommandManager.CustomTypeIdStartOffset)
                {
                    if (srcRecordHasETag)
                        ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);

                    if (!input.metaCommandInfo.CheckConditionalExecution(srcLogRecord.ETag, out _,
                            readOnlyContext: true))
                    {
                        if (srcRecordHasETag)
                            ETagState.ResetState(ref functionsState.etagState);
                        return functionsState.HandleSkippedExecution(in input.header, ref output.SpanByteAndMemory);
                    }

                    garnetObject.Operate(ref input, ref output, functionsState.respProtocolVersion, out _);

                    output.ETag = srcLogRecord.ETag;

                    if (srcRecordHasETag)
                        ETagState.ResetState(ref functionsState.etagState);

                    return true;
                }

                if (IncorrectObjectType(ref input, (IGarnetObject)srcLogRecord.ValueObject, ref output.SpanByteAndMemory))
                {
                    output.OutputFlags |= ObjectOutputFlags.WrongType;
                    return true;
                }

                if (srcRecordHasETag)
                {
                    functionsState.CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output.SpanByteAndMemory);
                    return true;
                }

                var customObjectCommand = GetCustomObjectCommand(ref input, input.header.type);
                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                try
                {
                    var result = customObjectCommand.Reader(srcLogRecord.Key, ref input, garnetObject, ref writer, ref readInfo);
                    return result;
                }
                finally
                {
                    writer.Dispose();
                }
            }

            output.ETag = srcLogRecord.ETag;
            output.GarnetObject = (IGarnetObject)srcLogRecord.ValueObject;
            return true;
        }
    }
}