// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Callback functions for main store
    /// </summary>
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<StringInput, SpanByteAndMemory, long>
    {
        /// <inheritdoc />
        public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref StringInput input, ref SpanByteAndMemory output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.Info.ValueIsObject)
            {
                readInfo.Action = ReadAction.WrongType;
                return false;
            }

            if (LogRecordUtils.CheckExpiry(in srcLogRecord))
                return false;

            var execCmd = true;
            var cmd = input.header.cmd;
            var metaCmd = input.metaCommandInfo.MetaCommand;
            var value = srcLogRecord.ValueSpan; // reduce redundant length calculations

            if (metaCmd.IsEtagCondExecCommand())
            {
                var inputEtag = input.metaCommandInfo.Arg1;
                execCmd = metaCmd.CheckConditionalExecution(srcLogRecord.ETag, inputEtag);
            }

            if (cmd > RespCommandExtensions.LastValidCommand)
            {
                if (srcLogRecord.Info.HasETag)
                {
                    functionsState.CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output);
                    return true;
                }

                var valueLength = value.Length;

                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output);
                try
                {
                    var ret = functionsState.GetCustomCommandFunctions((ushort)cmd)
                        .Reader(srcLogRecord.Key, ref input, value, ref writer, ref readInfo);
                    Debug.Assert(valueLength <= value.Length);
                    return ret;
                }
                finally
                {
                    writer.Dispose();
                }
            }

            if (srcLogRecord.Info.HasETag)
                ETagState.SetValsForRecordWithEtag(ref functionsState.etagState, in srcLogRecord);

            // Unless the command explicitly asks for the ETag in response, we do not write back the ETag
            if (input.metaCommandInfo.MetaCommand.IsEtagCommand())
            {
                if (execCmd)
                    CopyRespWithEtagData(value, ref output, srcLogRecord.Info.HasETag, functionsState.memoryPool);
                else
                    WriteValueAndEtagToDst(functionsState.nilResp, srcLogRecord.ETag, ref output, functionsState.memoryPool, writeDirect: true);
                ETagState.ResetState(ref functionsState.etagState);
                return true;
            }

            if (cmd == RespCommand.NONE)
                CopyRespTo(value, ref output);
            else
                CopyRespToWithInput(in srcLogRecord, ref input, ref output, readInfo.IsFromPending);

            if (srcLogRecord.Info.HasETag)
                ETagState.ResetState(ref functionsState.etagState);

            return true;
        }
    }
}