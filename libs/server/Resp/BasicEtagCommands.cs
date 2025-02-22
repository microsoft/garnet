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
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
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
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
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
        /// SETIFMATCH key val etag [EX|PX] [expiry] [NOGET]
        /// Sets a key value pair with the given etag only if (1) the etag given in the request matches the already existing etag ;
        /// or (2) there was no existing value; or (3) the existing value was not associated with any etag and the sent Etag was 0.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkSETIFMATCH<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            return NetworkSetETagConditional(RespCommand.SETIFMATCH, ref storageApi);
        }


        /// <summary>
        /// SETIFGREATER key val etag [EX|PX] [expiry] [NOGET]
        /// Sets a key value pair with the given etag only if (1) the etag given in the request is greater than the already existing etag ;
        /// or (2) there was no existing value; or (3) the existing value was not associated with any etag.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkSETIFGREATER<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            return NetworkSetETagConditional(RespCommand.SETIFGREATER, ref storageApi);
        }

        private bool NetworkSetETagConditional<TGarnetApi>(RespCommand cmd, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // Currently only supports these two commands
            Debug.Assert(cmd is RespCommand.SETIFMATCH or RespCommand.SETIFGREATER);

            if (parseState.Count < 3 || parseState.Count > 6)
            {
                return AbortWithWrongNumberOfArguments(nameof(cmd));
            }

            int expiry = 0;
            ReadOnlySpan<byte> errorMessage = default;
            var tokenIdx = 3;

            ExpirationOption expOption = ExpirationOption.None;
            bool noGet = false;

            while (tokenIdx < parseState.Count)
            {
                // Parse NOGET option
                if (parseState.GetArgSliceByRef(tokenIdx).Span.EqualsUpperCaseSpanIgnoringCase(CmdStrings.NOGET))
                {
                    if (noGet)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                        break;
                    }

                    noGet = true;
                    tokenIdx++;
                    continue;
                }

                // Parse EX | PX expiry combination
                if (parseState.TryGetExpirationOption(tokenIdx, out expOption))
                {
                    if (expOption is not ExpirationOption.EX and not ExpirationOption.PX)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                        break;
                    }

                    // we know that the token is either EX or PX from above and the next value should be the expiry
                    tokenIdx++;
                    if (!parseState.TryGetInt(tokenIdx, out expiry))
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                        break;
                    }
                    else if (expiry <= 0)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_INVALIDEXP_IN_SET;
                        break;
                    }

                    tokenIdx++;
                    continue;
                }

                // neither NOGET nor EX|PX expiry combination
                errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                break;
            }

            bool etagRead = parseState.TryGetLong(2, out long etag);
            if (!etagRead || etag < 0)
            {
                errorMessage = CmdStrings.RESP_ERR_INVALID_ETAG;
            }

            if (!errorMessage.IsEmpty)
            {
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            SpanByte key = parseState.GetArgSliceByRef(0).SpanByte;

            NetworkSET_Conditional(cmd, expiry, ref key, getValue: !noGet, highPrecision: expOption == ExpirationOption.PX, withEtag: true, ref storageApi);

            return true;
        }
    }
}