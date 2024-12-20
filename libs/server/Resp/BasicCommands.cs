﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - basic commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// GET
        /// </summary>
        bool NetworkGET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (storeWrapper.serverOptions.EnableScatterGatherGet)
                return NetworkGET_SG(ref storageApi);

            if (useAsync)
                return NetworkGETAsync(ref storageApi);

            RawStringInput input = default;

            var key = parseState.GetArgSliceByRef(0).SpanByte;
            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.GET(ref key, ref input, ref o);

            switch (status)
            {
                case GarnetStatus.OK:
                    if (!o.IsSpanByte)
                        SendAndReset(o.Memory, o.Length);
                    else
                        dcurr += o.Length;
                    break;
                case GarnetStatus.NOTFOUND:
                    Debug.Assert(o.IsSpanByte);
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// GET
        /// </summary>
        bool NetworkGETEX<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1 || parseState.Count > 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.GETEX));
            }

            var key = parseState.GetArgSliceByRef(0).SpanByte;

            TimeSpan? tsExpiry = null;
            if (parseState.Count > 1)
            {
                var option = parseState.GetArgSliceByRef(1).ReadOnlySpan;
                if (option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.PERSIST))
                {
                    tsExpiry = TimeSpan.Zero;
                }
                else
                {
                    if (parseState.Count < 3 || !parseState.TryGetLong(2, out var expireTime) || expireTime <= 0)
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }

                    switch (option)
                    {
                        case var _ when option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.EX):
                            tsExpiry = TimeSpan.FromSeconds(expireTime);
                            break;

                        case var _ when option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.PX):
                            tsExpiry = TimeSpan.FromMilliseconds(expireTime);
                            break;

                        case var _ when option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.EXAT):
                            tsExpiry = DateTimeOffset.FromUnixTimeSeconds(expireTime) - DateTimeOffset.UtcNow;
                            break;

                        case var _ when option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.PXAT):
                            tsExpiry = DateTimeOffset.FromUnixTimeMilliseconds(expireTime) - DateTimeOffset.UtcNow;
                            break;

                        default:
                            while (!RespWriteUtils.WriteError($"ERR Unsupported option {parseState.GetString(1)}", ref dcurr, dend))
                                SendAndReset();
                            return true;
                    }
                }
            }

            var expiry = (tsExpiry.HasValue && tsExpiry.Value.Ticks > 0) ? DateTimeOffset.UtcNow.Ticks + tsExpiry.Value.Ticks : 0;
            var input = new RawStringInput(RespCommand.GETEX, ref parseState, startIdx: 1, arg1: expiry);

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.GETEX(ref key, ref input, ref o);

            switch (status)
            {
                case GarnetStatus.OK:
                    if (!o.IsSpanByte)
                        SendAndReset(o.Memory, o.Length);
                    else
                        dcurr += o.Length;
                    break;
                case GarnetStatus.NOTFOUND:
                    Debug.Assert(o.IsSpanByte);
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// GET - async version
        /// </summary>
        bool NetworkGETAsync<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var key = parseState.GetArgSliceByRef(0).SpanByte;
            // Optimistically ask storage to write output to network buffer
            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            // Set up input to instruct storage to write output to IMemory rather than
            // network buffer, if the operation goes pending.
            var input = new RawStringInput(RespCommand.ASYNC);

            var status = storageApi.GET_WithPending(ref key, ref input, ref o, asyncStarted, out var pending);

            if (pending)
            {
                NetworkGETPending(ref storageApi);
            }
            else
            {
                switch (status)
                {
                    case GarnetStatus.OK:
                        if (!o.IsSpanByte)
                            SendAndReset(o.Memory, o.Length);
                        else
                            dcurr += o.Length;
                        break;
                    case GarnetStatus.NOTFOUND:
                        Debug.Assert(o.IsSpanByte);
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }
            return true;
        }

        /// <summary>
        /// GET - scatter gather version
        /// </summary>
        bool NetworkGET_SG<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetAdvancedApi
        {
            var key = parseState.GetArgSliceByRef(0).SpanByte;
            RawStringInput input = default;
            var firstPending = -1;
            (GarnetStatus, SpanByteAndMemory)[] outputArr = null;
            SpanByteAndMemory o = new(dcurr, (int)(dend - dcurr));
            var c = 0;

            for (; ; c++)
            {
                if (c > 0 && !ParseGETAndKey(ref key))
                    break;

                // Store index in context, since completions are not in order
                long ctx = firstPending == -1 ? 0 : c - firstPending;

                var status = storageApi.GET_WithPending(ref key, ref input, ref o, ctx,
                    out var isPending);

                if (isPending)
                {
                    SetResult(c, ref firstPending, ref outputArr, status, default);
                    o = new SpanByteAndMemory();
                }
                else
                {
                    if (status == GarnetStatus.OK)
                    {
                        if (firstPending == -1)
                        {
                            // Found in memory without IO, and no earlier pending, so we can add directly to the output
                            if (!o.IsSpanByte)
                                SendAndReset(o.Memory, o.Length);
                            else
                                dcurr += o.Length;
                            o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                        }
                        else
                        {
                            SetResult(c, ref firstPending, ref outputArr, status, o);
                            o = new SpanByteAndMemory();
                        }
                    }
                    else
                    {
                        if (firstPending == -1)
                        {
                            // Realized not-found without IO, and no earlier pending, so we can add directly to the output
                            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                                SendAndReset();
                            o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                        }
                        else
                        {
                            SetResult(c, ref firstPending, ref outputArr, status, o);
                            o = new SpanByteAndMemory();
                        }
                    }
                }
            }

            if (firstPending != -1)
            {
                // First complete all pending ops
                storageApi.GET_CompletePending(outputArr, true);

                // Write the outputs to network buffer
                for (var i = 0; i < c - firstPending; i++)
                {
                    var status = outputArr[i].Item1;
                    var output = outputArr[i].Item2;
                    if (status == GarnetStatus.OK)
                    {
                        if (!output.IsSpanByte)
                            SendAndReset(output.Memory, output.Length);
                        else
                            dcurr += output.Length;
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                    }
                }
            }

            if (c > 1)
            {
                // Update metrics (the first GET is accounted for by the caller)
                if (latencyMetrics != null) opCount += c - 1;
                if (sessionMetrics != null)
                {
                    sessionMetrics.total_commands_processed += (ulong)(c - 1);
                    sessionMetrics.total_read_commands_processed += (ulong)(c - 1);
                }
            }

            return true;
        }

        /// <summary>
        /// SET
        /// </summary>
        private bool NetworkSET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(parseState.Count == 2);
            var key = parseState.GetArgSliceByRef(0).SpanByte;
            var value = parseState.GetArgSliceByRef(1).SpanByte;

            storageApi.SET(ref key, ref value);

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// GETSET
        /// </summary>
        private bool NetworkGETSET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(parseState.Count == 2);
            var key = parseState.GetArgSliceByRef(0);
            var value = parseState.GetArgSliceByRef(1);
            var getOption = ArgSlice.FromPinnedSpan(CmdStrings.GET);
            parseState.InitializeWithArguments(key, value, getOption);

            return NetworkSETEXNX(ref storageApi);
        }

        /// <summary>
        /// GETWITHETAG key 
        /// Given a key get the value and ETag
        /// </summary>
        private bool NetworkGETWITHETAG<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(parseState.Count == 1);

            var key = parseState.GetArgSliceByRef(0).SpanByte;
            var input = new RawStringInput(RespCommand.GETWITHETAG, ref parseState, startIdx: 1);
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
        /// Given a key and an etag, return the value and it's etag only if the sent ETag does not match the existing ETag.
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
        /// SETIFMATCH key val etag EX|PX expiry
        /// Sets a key value pair only if an already existing etag does not match the etag sent as a part of the request
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

            // SETIFMATCH Args: KEY VAL ETAG -> [ ((EX || PX) expiration)]
            int expiry = 0;
            ReadOnlySpan<byte> errorMessage = default;
            var expOption = ExpirationOption.None;

            var tokenIdx = 3;
            Span<byte> nextOpt = default;
            var optUpperCased = false;
            while (tokenIdx < parseState.Count || optUpperCased)
            {
                if (!optUpperCased)
                {
                    nextOpt = parseState.GetArgSliceByRef(tokenIdx++).Span;
                }

                if (nextOpt.SequenceEqual(CmdStrings.EX))
                {
                    // Validate expiry
                    if (!parseState.TryGetInt(tokenIdx++, out expiry))
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                        break;
                    }

                    if (expOption != ExpirationOption.None)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                        break;
                    }

                    expOption = ExpirationOption.EX;
                    if (expiry <= 0)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_INVALIDEXP_IN_SET;
                        break;
                    }
                }
                else if (nextOpt.SequenceEqual(CmdStrings.PX))
                {
                    // Validate expiry
                    if (!parseState.TryGetInt(tokenIdx++, out expiry))
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                        break;
                    }

                    if (expOption != ExpirationOption.None)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                        break;
                    }

                    expOption = ExpirationOption.PX;
                    if (expiry <= 0)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_INVALIDEXP_IN_SET;
                        break;
                    }
                }
                else
                {
                    if (!optUpperCased)
                    {
                        AsciiUtils.ToUpperInPlace(nextOpt);
                        optUpperCased = true;
                        continue;
                    }

                    errorMessage = CmdStrings.RESP_ERR_GENERIC_UNK_CMD;
                    break;
                }

                optUpperCased = false;
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

        /// <summary>
        /// SETRANGE
        /// </summary>
        private bool NetworkSetRange<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var key = parseState.GetArgSliceByRef(0);

            // Validate offset
            if (!parseState.TryGetInt(1, out var offset))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (offset < 0)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_OFFSETOUTOFRANGE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var input = new RawStringInput(RespCommand.SETRANGE, ref parseState, startIdx: 1);

            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatInt64Length];
            var output = ArgSlice.FromPinnedSpan(outputBuffer);

            storageApi.SETRANGE(key, ref input, ref output);

            while (!RespWriteUtils.WriteIntegerFromBytes(outputBuffer.Slice(0, output.Length), ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkGetRange<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var key = parseState.GetArgSliceByRef(0);
            var sbKey = key.SpanByte;

            // Validate range
            if (!parseState.TryGetInt(1, out _) || !parseState.TryGetInt(2, out _))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var input = new RawStringInput(RespCommand.GETRANGE, ref parseState, startIdx: 1);

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            var status = storageApi.GETRANGE(ref sbKey, ref input, ref o);

            if (status == GarnetStatus.OK)
            {
                sessionMetrics?.incr_total_found();
                if (!o.IsSpanByte)
                    SendAndReset(o.Memory, o.Length);
                else
                    dcurr += o.Length;
            }
            else
            {
                sessionMetrics?.incr_total_notfound();
                Debug.Assert(o.IsSpanByte);
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_EMPTY, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// SETEX
        /// </summary>
        private bool NetworkSETEX<TGarnetApi>(bool highPrecision, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var key = parseState.GetArgSliceByRef(0).SpanByte;

            // Validate expiry
            if (!parseState.TryGetInt(1, out var expiry))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (expiry <= 0)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_INVALIDEXP_IN_SET, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var valMetadata = DateTimeOffset.UtcNow.Ticks +
                              (highPrecision
                                  ? TimeSpan.FromMilliseconds(expiry).Ticks
                                  : TimeSpan.FromSeconds(expiry).Ticks);

            var sbVal = parseState.GetArgSliceByRef(2).SpanByte;

            var input = new RawStringInput(RespCommand.SETEX, 0, valMetadata);
            _ = storageApi.SET(ref key, ref input, ref sbVal);

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// SETNX
        /// </summary>
        private bool NetworkSETNX<TGarnetApi>(bool highPrecision, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(parseState.Count == 2);
            var key = parseState.GetArgSliceByRef(0);
            var value = parseState.GetArgSliceByRef(1);
            var getOption = ArgSlice.FromPinnedSpan(CmdStrings.NX);
            parseState.InitializeWithArguments(key, value, getOption);

            return NetworkSETEXNX(ref storageApi);
        }

        enum EtagOption : byte
        {
            None,
            WITHETAG,
        }

        enum ExpirationOption : byte
        {
            None,
            EX,
            PX,
            EXAT,
            PXAT,
            KEEPTTL
        }

        enum ExistOptions : byte
        {
            None,
            NX,
            XX
        }

        /// <summary>
        /// SET EX NX [WITHETAG]
        /// </summary>
        private bool NetworkSETEXNX<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var key = parseState.GetArgSliceByRef(0);
            var sbKey = key.SpanByte;

            var val = parseState.GetArgSliceByRef(1);
            var sbVal = val.SpanByte;

            var expiry = 0;
            ReadOnlySpan<byte> errorMessage = default;
            var existOptions = ExistOptions.None;
            var expOption = ExpirationOption.None;
            var etagOption = EtagOption.None;
            var getValue = false;

            var tokenIdx = 2;
            Span<byte> nextOpt = default;
            var optUpperCased = false;
            while (tokenIdx < parseState.Count || optUpperCased)
            {
                if (!optUpperCased)
                {
                    nextOpt = parseState.GetArgSliceByRef(tokenIdx++).Span;
                }

                if (nextOpt.SequenceEqual(CmdStrings.EX))
                {
                    // Validate expiry
                    if (!parseState.TryGetInt(tokenIdx++, out expiry))
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                        break;
                    }

                    if (expOption != ExpirationOption.None)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                        break;
                    }

                    expOption = ExpirationOption.EX;
                    if (expiry <= 0)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_INVALIDEXP_IN_SET;
                        break;
                    }
                }
                else if (nextOpt.SequenceEqual(CmdStrings.PX))
                {
                    // Validate expiry
                    if (!parseState.TryGetInt(tokenIdx++, out expiry))
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                        break;
                    }

                    if (expOption != ExpirationOption.None)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                        break;
                    }

                    expOption = ExpirationOption.PX;
                    if (expiry <= 0)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_INVALIDEXP_IN_SET;
                        break;
                    }
                }
                else if (nextOpt.SequenceEqual(CmdStrings.KEEPTTL))
                {
                    if (expOption != ExpirationOption.None)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                        break;
                    }

                    expOption = ExpirationOption.KEEPTTL;
                }
                else if (nextOpt.SequenceEqual(CmdStrings.NX))
                {
                    if (existOptions != ExistOptions.None)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                        break;
                    }

                    existOptions = ExistOptions.NX;
                }
                else if (nextOpt.SequenceEqual(CmdStrings.XX))
                {
                    if (existOptions != ExistOptions.None)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                        break;
                    }

                    existOptions = ExistOptions.XX;
                }
                else if (nextOpt.SequenceEqual(CmdStrings.GET))
                {
                    if (etagOption != EtagOption.None)
                    {
                        // cannot do withEtag and getValue since withEtag SET already returns ETag in response
                        errorMessage = CmdStrings.RESP_ERR_WITHETAG_AND_GETVALUE;
                        break;
                    }

                    getValue = true;
                }
                else if (nextOpt.SequenceEqual(CmdStrings.WITHETAG))
                {
                    if (etagOption != EtagOption.None)
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                        break;
                    }

                    if (getValue)
                    {
                        // cannot do withEtag and getValue since withEtag SET already returns ETag in response
                        errorMessage = CmdStrings.RESP_ERR_WITHETAG_AND_GETVALUE;
                        break;
                    }

                    etagOption = EtagOption.WITHETAG;
                }
                else
                {
                    if (!optUpperCased)
                    {
                        AsciiUtils.ToUpperInPlace(nextOpt);
                        optUpperCased = true;
                        continue;
                    }

                    errorMessage = CmdStrings.RESP_ERR_GENERIC_UNK_CMD;
                    break;
                }

                optUpperCased = false;
            }

            if (!errorMessage.IsEmpty)
            {
                while (!RespWriteUtils.WriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            bool withEtag = etagOption == EtagOption.WITHETAG;

            var isHighPrecision = expOption == ExpirationOption.PX;

            switch (expOption)
            {
                case ExpirationOption.None:
                case ExpirationOption.EX:
                case ExpirationOption.PX:
                    switch (existOptions)
                    {
                        case ExistOptions.None:
                            return getValue || withEtag
                                ? NetworkSET_Conditional(RespCommand.SET, expiry, ref sbKey, getValue,
                                    isHighPrecision, withEtag, ref storageApi)
                                : NetworkSET_EX(RespCommand.SET, expOption, expiry, ref sbKey, ref sbVal, ref storageApi); // Can perform a blind update
                        case ExistOptions.XX:
                            return NetworkSET_Conditional(RespCommand.SETEXXX, expiry, ref sbKey,
                                getValue, highPrecision: isHighPrecision, withEtag, ref storageApi);
                        case ExistOptions.NX:
                            return NetworkSET_Conditional(RespCommand.SETEXNX, expiry, ref sbKey,
                                getValue, highPrecision: isHighPrecision, withEtag, ref storageApi);
                    }
                    break;
                case ExpirationOption.KEEPTTL:
                    Debug.Assert(expiry == 0); // no expiration if KEEPTTL
                    switch (existOptions)
                    {
                        case ExistOptions.None:
                            // We can never perform a blind update due to KEEPTTL
                            return NetworkSET_Conditional(RespCommand.SETKEEPTTL, expiry, ref sbKey
                                , getValue, highPrecision: false, withEtag, ref storageApi);
                        case ExistOptions.XX:
                            return NetworkSET_Conditional(RespCommand.SETKEEPTTLXX, expiry, ref sbKey,
                                getValue, highPrecision: false, withEtag, ref storageApi);
                        case ExistOptions.NX:
                            return NetworkSET_Conditional(RespCommand.SETEXNX, expiry, ref sbKey,
                                getValue, highPrecision: false, withEtag, ref storageApi);
                    }

                    break;
            }

            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private unsafe bool NetworkSET_EX<TGarnetApi>(RespCommand cmd, ExpirationOption expOption, int expiry,
            ref SpanByte key, ref SpanByte val, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(cmd == RespCommand.SET);

            var highPrecision = expOption == ExpirationOption.PX;
            var valMetadata = DateTimeOffset.UtcNow.Ticks +
                              (highPrecision
                                  ? TimeSpan.FromMilliseconds(expiry).Ticks
                                  : TimeSpan.FromSeconds(expiry).Ticks);

            var input = new RawStringInput(cmd, 0, valMetadata);

            storageApi.SET(ref key, ref input, ref val);

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkSET_Conditional<TGarnetApi>(RespCommand cmd, int expiry, ref SpanByte key, bool getValue, bool highPrecision, bool withEtag, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var inputArg = expiry == 0
                ? 0
                : DateTimeOffset.UtcNow.Ticks +
                  (highPrecision
                      ? TimeSpan.FromMilliseconds(expiry).Ticks
                      : TimeSpan.FromSeconds(expiry).Ticks);

            var input = new RawStringInput(cmd, ref parseState, startIdx: 1, arg1: inputArg);

            if (withEtag)
                input.header.SetWithEtagFlag();

            if (getValue)
                input.header.SetSetGetFlag();

            SpanByteAndMemory outputBuffer = default;
            GarnetStatus status;

            // SETIFMATCH will always hit this conditional and have assign output buffer to the right memory location
            if (getValue || withEtag)
            {
                // anything with getValue or withEtag writes to the buffer 
                outputBuffer = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                status = storageApi.SET_Conditional(ref key,
                    ref input, ref outputBuffer);
            }
            else
            {
                // the following debug is the catch any edge case leading to SETIFMATCH skipping the above block
                Debug.Assert(cmd != RespCommand.SETIFMATCH, "SETIFMATCH should have gone though pointing to right output variable");

                status = storageApi.SET_Conditional(ref key, ref input);
            }

            switch ((getValue, withEtag, cmd, status))
            {
                // since SET with etag goes down RMW a not found is okay and data is on buffer
                case (_, true, RespCommand.SET, GarnetStatus.NOTFOUND):
                // if getvalue || etag and Status is OK then the response is always on the buffer, getvalue is never used with conditionals
                // extra pattern matching on command below for invariant get value cannot be used with EXXX and EXNX
                case (true, _, RespCommand.SET or RespCommand.SETIFMATCH or RespCommand.SETKEEPTTL, GarnetStatus.OK):
                case (_, true, _, GarnetStatus.OK or GarnetStatus.NOTFOUND):
                    if (!outputBuffer.IsSpanByte)
                        SendAndReset(outputBuffer.Memory, outputBuffer.Length);
                    else
                        dcurr += outputBuffer.Length;
                    break;

                case (false, false, RespCommand.SETEXNX, GarnetStatus.NOTFOUND): // SETEXNX is at success if not found and nothign on buffer if no get or withetag so return +OK
                case (false, false,
                    RespCommand.SET or RespCommand.SETIFMATCH or RespCommand.SETEXXX or RespCommand.SETKEEPTTL or RespCommand.SETKEEPTTLXX,
                GarnetStatus.OK): // for everything EXCPET SETEXNX if no get, and no etag, then an OK returns +OK response
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                    break;

                case (_, _, RespCommand.SETEXNX, GarnetStatus.OK): // For NX semantics an OK indicates a found, which means nothing was set and hence we return NIL
                // anything not found that did not come from SETEXNX or WITHETAG always returns NIL, also anything that is indicating wrong type or moved will return NIL
                case (_, false, not RespCommand.SETEXNX, GarnetStatus.NOTFOUND or GarnetStatus.WRONGTYPE or GarnetStatus.MOVED):
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                    break;

                default:
                    Debug.Assert(false, $"({getValue}, {withEtag}, {cmd}, {status}) unaccounted for combination in response pattern matching. Please make explicit.");
                    break;
            };

            return true;
        }

        /// <summary>
        /// Increment (INCRBY, DECRBY, INCR, DECR)
        /// </summary>
        private bool NetworkIncrement<TGarnetApi>(RespCommand cmd, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(cmd == RespCommand.INCRBY || cmd == RespCommand.DECRBY || cmd == RespCommand.INCR ||
                         cmd == RespCommand.DECR);

            if ((parseState.Count < 1 && (cmd == RespCommand.INCR || cmd == RespCommand.DECR))
                || (parseState.Count < 2 && (cmd == RespCommand.INCRBY || cmd == RespCommand.DECRBY)))
                return AbortWithWrongNumberOfArguments(cmd.ToString());

            var key = parseState.GetArgSliceByRef(0);

            long incrByValue = 0;
            if (parseState.Count > 1 && !parseState.TryGetLong(1, out incrByValue))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatInt64Length + 1];
            var output = ArgSlice.FromPinnedSpan(outputBuffer);

            var input = new RawStringInput(cmd, 0, incrByValue);
            storageApi.Increment(key, ref input, ref output);

            var errorFlag = output.Length == NumUtils.MaximumFormatInt64Length + 1
                ? (OperationError)output.Span[0]
                : OperationError.SUCCESS;

            switch (errorFlag)
            {
                case OperationError.SUCCESS:
                    while (!RespWriteUtils.WriteIntegerFromBytes(outputBuffer.Slice(0, output.Length), ref dcurr, dend))
                        SendAndReset();
                    break;
                case OperationError.INVALID_TYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();
                    break;
                default:
                    throw new GarnetException($"Invalid OperationError {errorFlag}");
            }

            return true;
        }

        /// <summary>
        /// Increment by float (INCRBYFLOAT)
        /// </summary>
        private bool NetworkIncrementByFloat<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var key = parseState.GetArgSliceByRef(0);
            var incrSlice = parseState.GetArgSliceByRef(1);

            if (!NumUtils.TryParse(incrSlice.ReadOnlySpan, out float _))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatDoubleLength + 1];
            var output = ArgSlice.FromPinnedSpan(outputBuffer);

            var input = new RawStringInput(RespCommand.INCRBYFLOAT, ref parseState, startIdx: 1);
            storageApi.Increment(key, ref input, ref output);

            var errorFlag = output.Length == NumUtils.MaximumFormatDoubleLength + 1
                ? (OperationError)output.Span[0]
                : OperationError.SUCCESS;

            switch (errorFlag)
            {
                case OperationError.SUCCESS:
                    while (!RespWriteUtils.WriteBulkString(outputBuffer.Slice(0, output.Length), ref dcurr, dend))
                        SendAndReset();
                    break;
                case OperationError.INVALID_TYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT, ref dcurr,
                                   dend))
                        SendAndReset();
                    break;
                default:
                    throw new GarnetException($"Invalid OperationError {errorFlag}");
            }

            return true;
        }

        /// <summary>
        /// APPEND command - appends value at the end of existing string
        /// </summary>
        private bool NetworkAppend<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            var input = new RawStringInput(RespCommand.APPEND, ref parseState, startIdx: 1);

            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatInt64Length];
            var output = SpanByteAndMemory.FromPinnedSpan(outputBuffer);

            storageApi.APPEND(ref sbKey, ref input, ref output);

            while (!RespWriteUtils.WriteIntegerFromBytes(outputBuffer.Slice(0, output.Length), ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// PING
        /// </summary>
        private bool NetworkPING()
        {
            if (isSubscriptionSession && respProtocolVersion == 2)
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.SUSCRIBE_PONG, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_PONG, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        /// <summary>
        /// ASKING
        /// </summary>
        private bool NetworkASKING()
        {
            //*1\r\n$6\r\n ASKING\r\n = 16
            if (storeWrapper.serverOptions.EnableCluster)
                SessionAsking = 2;
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// QUIT
        /// </summary>
        private bool NetworkQUIT()
        {
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            toDispose = true;
            return true;
        }

        /// <summary>
        /// FLUSHDB [ASYNC|SYNC] [UNSAFETRUNCATELOG]
        /// </summary>
        private bool NetworkFLUSHDB()
        {
            if (parseState.Count > 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.FLUSHDB));
            }

            FlushDb(RespCommand.FLUSHDB);

            return true;
        }

        /// <summary>
        /// FLUSHALL [ASYNC|SYNC] [UNSAFETRUNCATELOG]
        /// </summary>
        private bool NetworkFLUSHALL()
        {
            if (parseState.Count > 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.FLUSHALL));
            }

            // Since Garnet currently only supports a single database,
            // FLUSHALL and FLUSHDB share the same logic
            FlushDb(RespCommand.FLUSHALL);

            return true;
        }

        /// <summary>
        /// Mark this session as readonly session
        /// </summary>
        /// <returns></returns>
        private bool NetworkREADONLY()
        {
            //*1\r\n$8\r\nREADONLY\r\n
            clusterSession?.SetReadOnlySession();
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// Mark this session as readwrite
        /// </summary>
        /// <returns></returns>
        private bool NetworkREADWRITE()
        {
            //*1\r\n$9\r\nREADWRITE\r\n
            clusterSession?.SetReadWriteSession();
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// Returns the length of the string value stored at key. An -1 is returned when key is not found
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkSTRLEN<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.STRLEN));
            }

            //STRLEN key
            var key = parseState.GetArgSliceByRef(0);
            var status = storageApi.GET(key, out var value);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.WriteInteger(value.Length, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteInteger(0, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// Common bits of COMMAND and COMMAND INFO implementation
        /// </summary>
        private void WriteCOMMANDResponse()
        {
            var resultSb = new StringBuilder();
            var cmdCount = 0;

            foreach (var customCmd in storeWrapper.customCommandManager.customCommandsInfo.Values)
            {
                cmdCount++;
                resultSb.Append(customCmd.RespFormat);
            }

            if (RespCommandsInfo.TryGetRespCommandsInfo(out var respCommandsInfo, true, logger))
            {
                foreach (var cmd in respCommandsInfo.Values)
                {
                    cmdCount++;
                    resultSb.Append(cmd.RespFormat);
                }
            }

            while (!RespWriteUtils.WriteArrayLength(cmdCount, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.WriteAsciiDirect(resultSb.ToString(), ref dcurr, dend))
                SendAndReset();
        }

        /// <summary>
        /// Processes COMMAND command.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkCOMMAND()
        {
            // No additional args allowed
            if (parseState.Count != 0)
            {
                var subCommand = parseState.GetString(0);
                var errorMsg = string.Format(CmdStrings.GenericErrUnknownSubCommand, subCommand, nameof(RespCommand.COMMAND));
                while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                WriteCOMMANDResponse();
            }

            return true;
        }

        /// <summary>
        /// Processes COMMAND COUNT subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkCOMMAND_COUNT()
        {
            // No additional args allowed
            if (parseState.Count != 0)
            {
                var errorMsg = string.Format(CmdStrings.GenericErrWrongNumArgs, "COMMAND COUNT");
                while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (!RespCommandsInfo.TryGetRespCommandsInfoCount(out var respCommandCount, true, logger))
                {
                    respCommandCount = 0;
                }

                var commandCount = storeWrapper.customCommandManager.CustomCommandsInfoCount + respCommandCount;

                while (!RespWriteUtils.WriteInteger(commandCount, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Processes COMMAND INFO subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkCOMMAND_DOCS()
        {
            var count = parseState.Count;

            var resultSb = new StringBuilder();
            var docsCount = 0;

            if (count == 0)
            {
                if (!RespCommandDocs.TryGetRespCommandsDocs(out var cmdsDocs, true, logger))
                    return true;

                foreach (var cmdDocs in cmdsDocs.Values)
                {
                    docsCount++;
                    resultSb.Append(cmdDocs.RespFormat);
                }

                foreach (var customCmd in storeWrapper.customCommandManager.customCommandsDocs.Values)
                {
                    docsCount++;
                    resultSb.Append(customCmd.RespFormat);
                }
            }
            else
            {
                for (var i = 0; i < count; i++)
                {
                    var cmdName = parseState.GetString(i);
                    if (RespCommandDocs.TryGetRespCommandDocs(cmdName, out var cmdDocs, true, true, logger) ||
                        storeWrapper.customCommandManager.TryGetCustomCommandDocs(cmdName, out cmdDocs))
                    {
                        docsCount++;
                        resultSb.Append(cmdDocs.RespFormat);
                    }
                }
            }

            while (!RespWriteUtils.WriteArrayLength(docsCount * 2, ref dcurr, dend))
                SendAndReset();

            while (!RespWriteUtils.WriteAsciiDirect(resultSb.ToString(), ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Processes COMMAND INFO subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkCOMMAND_INFO()
        {
            var count = parseState.Count;
            if (count == 0)
            {
                // Zero arg case is equivalent to COMMAND w/o subcommand
                WriteCOMMANDResponse();
            }
            else
            {
                while (!RespWriteUtils.WriteArrayLength(count, ref dcurr, dend))
                    SendAndReset();

                for (var i = 0; i < count; i++)
                {
                    var cmdName = parseState.GetString(i);

                    if (RespCommandsInfo.TryGetRespCommandInfo(cmdName, out var cmdInfo, true, true, logger) ||
                        storeWrapper.customCommandManager.TryGetCustomCommandInfo(cmdName, out cmdInfo))
                    {
                        while (!RespWriteUtils.WriteAsciiDirect(cmdInfo.RespFormat, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteNull(ref dcurr, dend))
                            SendAndReset();
                    }
                }
            }

            return true;
        }

        private bool NetworkECHO()
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ECHO));
            }

            var message = parseState.GetArgSliceByRef(0).ReadOnlySpan;
            WriteDirectLargeRespString(message);
            return true;
        }

        // HELLO [protover [AUTH username password] [SETNAME clientname]]
        private bool NetworkHELLO()
        {
            var count = parseState.Count;
            if (count > 6)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.HELLO));
            }

            byte? tmpRespProtocolVersion = null;
            ReadOnlySpan<byte> authUsername = default, authPassword = default;
            string tmpClientName = null;
            string errorMsg = default;

            if (count > 0)
            {
                var tokenIdx = 0;
                // Validate protocol version
                if (!parseState.TryGetInt(tokenIdx++, out var localRespProtocolVersion))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();

                    return true;
                }

                tmpRespProtocolVersion = (byte)localRespProtocolVersion;

                while (tokenIdx < count)
                {
                    var param = parseState.GetArgSliceByRef(tokenIdx++).ReadOnlySpan;

                    if (param.EqualsUpperCaseSpanIgnoringCase(CmdStrings.AUTH))
                    {
                        if (count - tokenIdx < 2)
                        {
                            errorMsg = string.Format(CmdStrings.GenericSyntaxErrorOption, nameof(RespCommand.HELLO),
                                nameof(CmdStrings.AUTH));
                            break;
                        }

                        authUsername = parseState.GetArgSliceByRef(tokenIdx++).ReadOnlySpan;
                        authPassword = parseState.GetArgSliceByRef(tokenIdx++).ReadOnlySpan;
                    }
                    else if (param.EqualsUpperCaseSpanIgnoringCase(CmdStrings.SETNAME))
                    {
                        if (count - tokenIdx < 1)
                        {
                            errorMsg = string.Format(CmdStrings.GenericSyntaxErrorOption, nameof(RespCommand.HELLO),
                                nameof(CmdStrings.SETNAME));
                            break;
                        }

                        tmpClientName = parseState.GetString(tokenIdx++);
                    }
                    else
                    {
                        errorMsg = string.Format(CmdStrings.GenericSyntaxErrorOption, nameof(RespCommand.HELLO),
                            Encoding.ASCII.GetString(param));
                        break;
                    }
                }
            }

            if (errorMsg != default)
            {
                while (!RespWriteUtils.WriteError(errorMsg, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            ProcessHelloCommand(tmpRespProtocolVersion, authUsername, authPassword, tmpClientName);
            return true;
        }

        private bool NetworkTIME()
        {
            if (parseState.Count != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.TIME));
            }

            var utcTime = DateTimeOffset.UtcNow;
            var seconds = utcTime.ToUnixTimeSeconds();
            var uSeconds = utcTime.ToString("ffffff");
            var response = $"*2\r\n${seconds.ToString().Length}\r\n{seconds}\r\n${uSeconds.Length}\r\n{uSeconds}\r\n";

            while (!RespWriteUtils.WriteAsciiDirect(response, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkAUTH()
        {
            // AUTH [<username>] <password>
            var count = parseState.Count;
            if (count < 1 || count > 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.AUTH));
            }

            ReadOnlySpan<byte> username = default;

            // Optional Argument: <username>
            var passwordTokenIdx = 0;
            if (count == 2)
            {
                username = parseState.GetArgSliceByRef(0).ReadOnlySpan;
                passwordTokenIdx = 1;
            }

            // Mandatory Argument: <password>
            var password = parseState.GetArgSliceByRef(passwordTokenIdx).ReadOnlySpan;

            // NOTE: Some authenticators cannot accept username/password pairs
            if (!_authenticator.CanAuthenticate)
            {
                while (!RespWriteUtils.WriteError("ERR Client sent AUTH, but configured authenticator does not accept passwords"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // XXX: There should be high-level AuthenticatorException
            if (this.AuthenticateUser(username, password))
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (username.IsEmpty)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_WRONGPASS_INVALID_PASSWORD, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_WRONGPASS_INVALID_USERNAME_PASSWORD, ref dcurr, dend))
                        SendAndReset();
                }
            }
            return true;
        }

        //MEMORY USAGE key [SAMPLES count]
        private bool NetworkMemoryUsage<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var count = parseState.Count;
            if (count != 1 && count != 3)
            {
                return AbortWithWrongNumberOfArguments(
                    $"{nameof(RespCommand.MEMORY)}|{Encoding.ASCII.GetString(CmdStrings.USAGE)}");
            }

            var key = parseState.GetArgSliceByRef(0);

            if (count == 3)
            {
                // Calculations for nested types do not apply to garnet, but we are checking syntax for API compatibility
                if (!parseState.GetArgSliceByRef(1).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(CmdStrings.SAMPLES))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                // Validate samples count
                if (!parseState.TryGetInt(2, out _))
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            var status = storageApi.MemoryUsageForKey(key, out var memoryUsage);

            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.WriteInteger((int)memoryUsage, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// ASYNC [ON|OFF|BARRIER]
        /// </summary>
        private bool NetworkASYNC()
        {
            if (respProtocolVersion <= 2)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOT_SUPPORTED_RESP2, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ASYNC));
            }

            var param = parseState.GetArgSliceByRef(0).ReadOnlySpan;
            if (param.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ON))
            {
                useAsync = true;
            }
            else if (param.EqualsUpperCaseSpanIgnoringCase(CmdStrings.OFF))
            {
                useAsync = false;
            }
            else if (param.EqualsUpperCaseSpanIgnoringCase(CmdStrings.BARRIER))
            {
                if (asyncCompleted < asyncStarted)
                {
                    asyncDone = new(0);
                    if (dcurr > networkSender.GetResponseObjectHead())
                        Send(networkSender.GetResponseObjectHead());
                    try
                    {
                        networkSender.ExitAndReturnResponseObject();
                        while (asyncCompleted < asyncStarted) asyncDone.Wait();
                        asyncDone.Dispose();
                        asyncDone = null;
                    }
                    finally
                    {
                        networkSender.EnterAndGetResponseObject(out dcurr, out dend);
                    }
                }
            }
            else
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Process the HELLO command
        /// </summary>
        void ProcessHelloCommand(byte? respProtocolVersion, ReadOnlySpan<byte> username, ReadOnlySpan<byte> password, string clientName)
        {
            if (respProtocolVersion != null)
            {
                if (respProtocolVersion.Value is < 2 or > 3)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION, ref dcurr, dend))
                        SendAndReset();
                    return;
                }

                if (respProtocolVersion.Value != this.respProtocolVersion && asyncCompleted < asyncStarted)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_ASYNC_PROTOCOL_CHANGE, ref dcurr, dend))
                        SendAndReset();
                    return;
                }

                this.respProtocolVersion = respProtocolVersion.Value;
            }

            if (!username.IsEmpty)
            {
                if (!this.AuthenticateUser(username, password))
                {
                    if (username.IsEmpty)
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_WRONGPASS_INVALID_PASSWORD, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_WRONGPASS_INVALID_USERNAME_PASSWORD, ref dcurr, dend))
                            SendAndReset();
                    }
                    return;
                }
            }

            if (clientName != null)
            {
                this.clientName = clientName;
            }

            (string, object)[] helloResult =
                [
                    ("server", "redis"),
                    ("version", storeWrapper.redisProtocolVersion),
                    ("garnet_version", storeWrapper.version),
                    ("proto", this.respProtocolVersion),
                    ("id", 63),
                    ("mode", storeWrapper.serverOptions.EnableCluster ? "cluster" : "standalone"),
                    ("role", storeWrapper.serverOptions.EnableCluster && storeWrapper.clusterProvider.IsReplica() ? "replica" : "master"),
                ];

            if (this.respProtocolVersion == 2)
            {
                while (!RespWriteUtils.WriteArrayLength(helloResult.Length * 2 + 2, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteMapLength(helloResult.Length + 1, ref dcurr, dend))
                    SendAndReset();
            }
            for (int i = 0; i < helloResult.Length; i++)
            {
                while (!RespWriteUtils.WriteAsciiBulkString(helloResult[i].Item1, ref dcurr, dend))
                    SendAndReset();
                if (helloResult[i].Item2 is int intValue)
                {
                    while (!RespWriteUtils.WriteInteger(intValue, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteAsciiBulkString(helloResult[i].Item2.ToString(), ref dcurr, dend))
                        SendAndReset();
                }
            }
            while (!RespWriteUtils.WriteAsciiBulkString("modules", ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.WriteArrayLength(0, ref dcurr, dend))
                SendAndReset();
        }

        /// <summary>
        /// Common logic for FLUSHDB and FLUSHALL
        /// </summary>
        /// <param name="cmd">RESP command (FLUSHDB / FLUSHALL)</param>
        void FlushDb(RespCommand cmd)
        {
            Debug.Assert(cmd is RespCommand.FLUSHDB or RespCommand.FLUSHALL);
            var unsafeTruncateLog = false;
            var async = false;
            var sync = false;
            var syntaxError = false;

            var count = parseState.Count;
            for (var i = 0; i < count; i++)
            {
                var nextToken = parseState.GetArgSliceByRef(i).ReadOnlySpan;

                if (nextToken.EqualsUpperCaseSpanIgnoringCase(CmdStrings.UNSAFETRUNCATELOG))
                {
                    if (unsafeTruncateLog)
                    {
                        syntaxError = true;
                        break;
                    }

                    unsafeTruncateLog = true;
                }
                else if (nextToken.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ASYNC))
                {
                    if (sync || async)
                    {
                        syntaxError = true;
                        break;
                    }

                    async = true;
                }
                else if (nextToken.EqualsUpperCaseSpanIgnoringCase(CmdStrings.SYNC))
                {
                    if (sync || async)
                    {
                        syntaxError = true;
                        break;
                    }

                    sync = true;
                }
                else
                {
                    syntaxError = true;
                    break;
                }
            }

            if (syntaxError)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();
                return;
            }

            if (async)
                Task.Run(() => ExecuteFlushDb(unsafeTruncateLog)).ConfigureAwait(false);
            else
                ExecuteFlushDb(unsafeTruncateLog);

            logger?.LogInformation($"Running {nameof(cmd)} {{async}} {{mode}}", async ? "async" : "sync", unsafeTruncateLog ? " with unsafetruncatelog." : string.Empty);
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
        }

        void ExecuteFlushDb(bool unsafeTruncateLog)
        {
            storeWrapper.store.Log.ShiftBeginAddress(storeWrapper.store.Log.TailAddress, truncateLog: unsafeTruncateLog);
            storeWrapper.objectStore?.Log.ShiftBeginAddress(storeWrapper.objectStore.Log.TailAddress, truncateLog: unsafeTruncateLog);
        }

        /// <summary>
        /// Writes a string describing the given session into the string builder.
        /// Does not append a new line.
        ///
        /// Not all Redis fields are written as they do not all have Garnet equivalents.
        /// </summary>
        private static void WriteClientInfo(IClusterProvider provider, StringBuilder into, RespServerSession targetSession, long nowMilliseconds)
        {
            var id = targetSession.Id;
            var remoteEndpoint = targetSession.networkSender.RemoteEndpointName;
            var localEndpoint = targetSession.networkSender.LocalEndpointName;
            var clientName = targetSession.clientName;
            var user = targetSession._user;
            var resp = targetSession.respProtocolVersion;
            var nodeId = targetSession?.clusterSession?.RemoteNodeId;

            into.Append($"id={id}");
            into.Append($" addr={remoteEndpoint}");
            into.Append($" laddr={localEndpoint}");
            if (clientName is not null)
            {
                into.Append($" name={clientName}");
            }

            var ageSec = (nowMilliseconds - targetSession.CreationTicks) / 1_000;

            into.Append($" age={ageSec}");

            if (user is not null)
            {
                into.Append($" user={user.Name}");
            }

            if (provider is not null && nodeId is not null)
            {
                if (provider.IsReplica(nodeId))
                {
                    into.Append($" flags=S");
                }
                else
                {
                    into.Append($" flags=M");
                }
            }
            else
            {
                if (targetSession.isSubscriptionSession)
                {
                    into.Append($" flags=P");
                }
                else
                {
                    into.Append($" flags=N");
                }
            }

            into.Append($" resp={resp}");
            into.Append($" lib-name={targetSession.clientLibName}");
            into.Append($" lib-ver={targetSession.clientLibVersion}");
        }

        bool ParseGETAndKey(ref SpanByte key)
        {
            var oldEndReadHead = readHead = endReadHead;
            var cmd = ParseCommand(out var success);
            if (!success || cmd != RespCommand.GET)
            {
                // If we either find no command or a different command, we back off
                endReadHead = readHead = oldEndReadHead;
                return false;
            }
            key = parseState.GetArgSliceByRef(0).SpanByte;
            return true;
        }

        static void SetResult(int c, ref int firstPending, ref (GarnetStatus, SpanByteAndMemory)[] outputArr,
            GarnetStatus status, SpanByteAndMemory output)
        {
            const int initialBatchSize = 8; // number of items in initial batch
            if (firstPending == -1)
            {
                outputArr = new (GarnetStatus, SpanByteAndMemory)[initialBatchSize];
                firstPending = c;
            }

            Debug.Assert(firstPending >= 0);
            Debug.Assert(c >= firstPending);
            Debug.Assert(outputArr != null);

            if (c - firstPending >= outputArr.Length)
            {
                int newCount = (int)NextPowerOf2(c - firstPending + 1);
                var outputArr2 = new (GarnetStatus, SpanByteAndMemory)[newCount];
                Array.Copy(outputArr, outputArr2, outputArr.Length);
                outputArr = outputArr2;
            }

            outputArr[c - firstPending] = (status, output);
        }

        static long NextPowerOf2(long v)
        {
            v--;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v + 1;
        }
    }
}