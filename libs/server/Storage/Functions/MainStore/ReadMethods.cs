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
    public readonly unsafe partial struct MainSessionFunctions : ISessionFunctions<StringInput, StringOutput, long>
    {
        /// <inheritdoc />
        public bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref StringInput input, ref StringOutput output, ref ReadInfo readInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.Info.ValueIsObject)
            {
                readInfo.Action = ReadAction.WrongType;
                return false;
            }

            if (LogRecordUtils.CheckExpiry(in srcLogRecord))
                return false;

            output.ETag = srcLogRecord.ETag;

            _ = EtagUtils.GetUpdatedEtag(srcLogRecord.ETag, ref input.metaCommandInfo, out var execCmd, init: false, readOnly: true);

            if (!execCmd)
            {
                var skipResp = input.header.CheckSkipRespOutputFlag();
                if (!skipResp)
                {
                    using var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
                    writer.WriteNull();
                }

                return true;
            }

            var cmd = input.header.cmd;
            var value = srcLogRecord.ValueSpan; // reduce redundant length calculations

            if (cmd > RespCommandExtensions.LastValidCommand)
            {
                if (srcLogRecord.Info.HasETag)
                {
                    functionsState.CopyDefaultResp(CmdStrings.RESP_ERR_ETAG_ON_CUSTOM_PROC, ref output.SpanByteAndMemory);
                    return true;
                }

                var valueLength = value.Length;

                var writer = new RespMemoryWriter(functionsState.respProtocolVersion, ref output.SpanByteAndMemory);
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

            if (cmd == RespCommand.NONE)
                CopyRespTo(value, ref output);
            else
                CopyRespToWithInput(in srcLogRecord, ref input, ref output, readInfo.IsFromPending);

            return true;
        }
    }
}