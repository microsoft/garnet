// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - ETag associated commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// GETWITHETAG key 
        /// Given a key get the value and it's ETag
        /// </summary>
        private bool NetworkGETWITHETAG<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(parseState.Count == 1);

            var key = parseState.GetArgSliceByRef(0).SpanByte;
            var input = new RawStringInput(RespCommand.GETWITHETAG);
            var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.GET(ref key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.NOTFOUND:
                    Debug.Assert(output.IsSpanByte);
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    if (!output.IsSpanByte)
                        SendAndReset(output.Memory, output.Length);
                    else
                        dcurr += output.Length;
                    break;
            }

            return true;
        }

        /// <summary>
        /// GETIFNOTMATCH key etag
        /// Given a key and an etag, return the value and it's etag.
        /// </summary>
        private bool NetworkGETIFNOTMATCH<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(parseState.Count == 2);

            var key = parseState.GetArgSliceByRef(0).SpanByte;
            var input = new RawStringInput(RespCommand.GETIFNOTMATCH, ref parseState, startIdx: 1);
            var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.GET(ref key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.NOTFOUND:
                    Debug.Assert(output.IsSpanByte);
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    if (!output.IsSpanByte)
                        SendAndReset(output.Memory, output.Length);
                    else
                        dcurr += output.Length;
                    break;
            }

            return true;
        }

        /// <summary>
        /// SETIFMATCH key val etag [EX|PX] [expiry]
        /// Sets a key value pair only if the already existing etag matches the etag sent as a part of the request.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkSETIFMATCH<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 3 || parseState.Count > 5)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SETIFMATCH));
            }

            int expiry = 0;
            ReadOnlySpan<byte> errorMessage = default;
            var tokenIdx = 3;

            ExpirationOption expOption = ExpirationOption.None;
            if (tokenIdx < parseState.Count)
            {
                if (!parseState.TryGetExpirationOption(tokenIdx++, out expOption) || (expOption is not ExpirationOption.EX and not ExpirationOption.PX))
                    errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                else
                {
                    if (!parseState.TryGetInt(tokenIdx++, out expiry))
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                    else if (expiry <= 0)
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_INVALIDEXP_IN_SET;
                }
            }

            if (!errorMessage.IsEmpty)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            SpanByte key = parseState.GetArgSliceByRef(0).SpanByte;

            NetworkSET_Conditional(RespCommand.SETIFMATCH, expiry, ref key, getValue: true, highPrecision: expOption == ExpirationOption.PX, withEtag: true, ref storageApi);

            return true;
        }
    }
}