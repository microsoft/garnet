// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
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
                    WriteNull();
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
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
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
                            while (!RespWriteUtils.TryWriteError($"ERR Unsupported option {parseState.GetString(1)}", ref dcurr, dend))
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
                    WriteNull();
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
                        WriteNull();
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
                            WriteNull();
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
                        WriteNull();
                    }
                }
            }

            if (c > 1)
            {
                // Update metrics (the first GET is accounted for by the caller)
                if (LatencyMetrics != null) opCount += c - 1;
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

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
            var key = parseState.GetArgSliceByRef(0).SpanByte;

            return NetworkSET_Conditional(RespCommand.SET, 0, ref key, true,
                                          false, false, ref storageApi);
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
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (offset < 0)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_OFFSETOUTOFRANGE);
            }

            var input = new RawStringInput(RespCommand.SETRANGE, ref parseState, startIdx: 1);

            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatInt64Length];
            var output = ArgSlice.FromPinnedSpan(outputBuffer);

            storageApi.SETRANGE(key, ref input, ref output);

            while (!RespWriteUtils.TryWriteIntegerFromBytes(outputBuffer.Slice(0, output.Length), ref dcurr, dend))
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
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
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
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_EMPTY, ref dcurr, dend))
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
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (expiry <= 0)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
            }

            var valMetadata = DateTimeOffset.UtcNow.Ticks +
                              (highPrecision
                                  ? TimeSpan.FromMilliseconds(expiry).Ticks
                                  : TimeSpan.FromSeconds(expiry).Ticks);

            var sbVal = parseState.GetArgSliceByRef(2).SpanByte;

            var input = new RawStringInput(RespCommand.SETEX, 0, valMetadata);
            _ = storageApi.SET(ref key, ref input, ref sbVal);

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// SETNX
        /// </summary>
        private bool NetworkSETNX<TGarnetApi>(bool highPrecision, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SETNX));
            }

            var key = parseState.GetArgSliceByRef(0);
            var sbKey = key.SpanByte;

            var input = new RawStringInput(RespCommand.SETEXNX, ref parseState, startIdx: 1);
            var status = storageApi.SET_Conditional(ref sbKey, ref input);

            // The status returned for SETNX as NOTFOUND is the expected status in the happy path
            var retVal = status == GarnetStatus.NOTFOUND ? 1 : 0;
            while (!RespWriteUtils.TryWriteInt32(retVal, ref dcurr, dend))
                SendAndReset();

            return true;
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

                // nextOpt was an expiration option
                if (parseState.TryGetExpirationOptionWithToken(ref nextOpt, out ExpirationOption parsedOption))
                {
                    // Make sure there aren't multiple expiration options in the options sent by user
                    // and that whatever parsedOption we have recieved is one of the acceptable ones only
                    if (expOption != ExpirationOption.None || (parsedOption is not (ExpirationOption.EX or ExpirationOption.PX or ExpirationOption.KEEPTTL)))
                    {
                        errorMessage = CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR;
                        break;
                    }

                    expOption = parsedOption;
                    // based on above check if it is not KEEPTTL, it has to be either EX or PX
                    if (expOption != ExpirationOption.KEEPTTL)
                    {
                        // EX and PX optionare followed by an expiry argument; account for the expiry argument by moving past the tokenIdx
                        if (!parseState.TryGetInt(tokenIdx++, out expiry))
                        {
                            errorMessage = CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
                            break;
                        }

                        if (expiry <= 0)
                        {
                            errorMessage = CmdStrings.RESP_ERR_GENERIC_INVALIDEXP_IN_SET;
                            break;
                        }
                    }
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

                    etagOption = EtagOption.WithETag;
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
                while (!RespWriteUtils.TryWriteError(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            bool withEtag = etagOption == EtagOption.WithETag;

            bool isHighPrecision = expOption == ExpirationOption.PX;

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
                                getValue, isHighPrecision, withEtag, ref storageApi);
                        case ExistOptions.NX:
                            return NetworkSET_Conditional(RespCommand.SETEXNX, expiry, ref sbKey,
                                getValue, isHighPrecision, withEtag, ref storageApi);
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

            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
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

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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

            if (!getValue && !withEtag)
            {
                // the following debug assertion is the catch any edge case leading to SETIFMATCH, or SETIFGREATER skipping the above block
                Debug.Assert(cmd is not (RespCommand.SETIFMATCH or RespCommand.SETIFGREATER), "SETIFMATCH should have gone though pointing to right output variable");

                var status = storageApi.SET_Conditional(ref key, ref input);

                // KEEPTTL without flags doesn't care whether it was found or not.
                if (cmd == RespCommand.SETKEEPTTL)
                {
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    var ok = status != GarnetStatus.NOTFOUND;

                    // the status returned for SETEXNX as NOTFOUND is the expected status in the happy path, so flip the ok flag
                    if (cmd == RespCommand.SETEXNX)
                        ok = !ok;

                    if (ok)
                    {
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                    }
                    else
                    {
                        WriteNull();
                    }
                }

                return true;
            }
            else
            {
                if (withEtag)
                    input.header.SetWithEtagFlag();

                if (getValue)
                    input.header.SetSetGetFlag();

                // anything with getValue or withEtag always writes to the buffer in the happy path
                SpanByteAndMemory outputBuffer = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                GarnetStatus status = storageApi.SET_Conditional(ref key, ref input, ref outputBuffer);

                // The data will be on the buffer either when we know the response is ok or when the withEtag flag is set.
                bool ok = status != GarnetStatus.NOTFOUND || withEtag;

                if (ok)
                {
                    if (!outputBuffer.IsSpanByte)
                        SendAndReset(outputBuffer.Memory, outputBuffer.Length);
                    else
                        dcurr += outputBuffer.Length;
                }
                else
                {
                    WriteNull();
                }

                return true;
            }
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
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
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
                    while (!RespWriteUtils.TryWriteIntegerFromBytes(outputBuffer.Slice(0, output.Length), ref dcurr, dend))
                        SendAndReset();
                    break;
                case OperationError.NAN_OR_INFINITY:
                case OperationError.INVALID_TYPE:
                    WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
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

            if (!parseState.TryGetDouble(1, out var dbl))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_NOT_VALID_FLOAT);
            }

            if (double.IsInfinity(dbl))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_NAN_INFINITY_INCR);
            }

            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatDoubleLength + 1];
            var output = ArgSlice.FromPinnedSpan(outputBuffer);
            var status = storageApi.IncrementByFloat(key, ref output, dbl);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.TryWriteBulkString(output.ReadOnlySpan, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                default:
                    if ((OperationError)output.Span[0] == OperationError.NAN_OR_INFINITY)
                        WriteError(CmdStrings.RESP_ERR_GENERIC_NAN_INFINITY_INCR);
                    else
                        WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT);
                    break;
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

            while (!RespWriteUtils.TryWriteIntegerFromBytes(outputBuffer.Slice(0, output.Length), ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.SUSCRIBE_PONG, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_PONG, ref dcurr, dend))
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
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// QUIT
        /// </summary>
        private bool NetworkQUIT()
        {
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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

            if (storeWrapper.serverOptions.EnableCluster && storeWrapper.clusterProvider.IsReplica() && !clusterSession.ReadWriteSession)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_FLUSHALL_READONLY_REPLICA, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            FlushDb(RespCommand.FLUSHDB);

            return true;
        }

        /// <summary>
        /// FLUSHALL [ASYNC|SYNC] [UNSAFETRUNCATELOG]
        /// </summary>
        private bool NetworkFLUSHALL()
        {
            if (parseState.Count > 3)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.FLUSHALL));
            }

            if (storeWrapper.serverOptions.EnableCluster && storeWrapper.clusterProvider.IsReplica() && !clusterSession.ReadWriteSession)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_FLUSHALL_READONLY_REPLICA);
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
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
                    while (!RespWriteUtils.TryWriteInt32(value.Length, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteInt32(0, ref dcurr, dend))
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
            var spam = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var writer = new RespMemoryWriter(respProtocolVersion, ref spam);

            try
            {
                var customCmds = customCommandManagerSession.GetAllCustomCommandsInfos();
                var cmdCount = customCmds.Count;
                var hasInfo = false;

                if (RespCommandsInfo.TryGetRespCommandsInfo(out var respCommandsInfo, true, logger))
                {
                    cmdCount += respCommandsInfo.Count;
                    hasInfo = true;
                }

                writer.WriteArrayLength(cmdCount);

                foreach (var customCmd in customCommandManagerSession.GetAllCustomCommandsInfos())
                {
                    customCmd.Value.ToRespFormat(ref writer);
                }

                if (hasInfo)
                {
                    foreach (var cmd in respCommandsInfo.Values)
                    {
                        cmd.ToRespFormat(ref writer);
                    }
                }
            }
            finally
            {
                writer.Dispose();
            }

            ProcessOutput(spam);
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
                while (!RespWriteUtils.TryWriteError(errorMsg, ref dcurr, dend))
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
                return AbortWithWrongNumberOfArguments("COMMAND|COUNT");
            }

            if (!RespCommandsInfo.TryGetRespCommandsInfoCount(out var respCommandCount, true, logger))
            {
                respCommandCount = 0;
            }

            var commandCount = customCommandManagerSession.GetCustomCommandInfoCount() + respCommandCount;

            while (!RespWriteUtils.TryWriteInt32(commandCount, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Processes COMMAND DOCS subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkCOMMAND_DOCS()
        {
            var count = parseState.Count;

            var spam = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var writer = new RespMemoryWriter(respProtocolVersion, ref spam);

            try
            {
                if (count == 0)
                {
                    if (RespCommandDocs.TryGetRespCommandsDocs(out var cmdsDocs, true, logger))
                    {
                        // Typical command docs output is larger than the default network buffer (1 << 17).
                        // Sizing in advance skips copying bytes later.
                        writer.Realloc(1 << 18);

                        var customCmds = customCommandManagerSession.GetAllCustomCommandsDocs();
                        writer.WriteMapLength(cmdsDocs.Count + customCmds.Count);

                        foreach (var cmdDocs in cmdsDocs.Values)
                        {
                            cmdDocs.ToRespFormat(ref writer);
                        }

                        foreach (var customCmd in customCmds)
                        {
                            customCmd.Value.ToRespFormat(ref writer);
                        }
                    }
                    else
                    {
                        writer.WriteEmptyMap();
                    }
                }
                else
                {
                    List<RespCommandDocs> docs = [];
                    for (var i = 0; i < count; i++)
                    {
                        var cmdName = parseState.GetString(i);
                        if (RespCommandDocs.TryGetRespCommandDocs(cmdName, out var cmdDocs, true, true, logger) ||
                            customCommandManagerSession.TryGetCustomCommandDocs(cmdName, out cmdDocs))
                        {
                            docs.Add(cmdDocs);
                        }
                    }

                    writer.WriteMapLength(docs.Count);
                    foreach (var cmdDocs in docs)
                    {
                        cmdDocs.ToRespFormat(ref writer);
                    }
                }
            }
            finally
            {
                writer.Dispose();
            }

            ProcessOutput(spam);
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
                var spam = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                var writer = new RespMemoryWriter(respProtocolVersion, ref spam);

                try
                {
                    writer.WriteArrayLength(count);

                    for (var i = 0; i < count; i++)
                    {
                        var cmdName = parseState.GetString(i);

                        if (RespCommandsInfo.TryGetRespCommandInfo(cmdName, out var cmdInfo, true, true, logger) ||
                            customCommandManagerSession.TryGetCustomCommandInfo(cmdName, out cmdInfo))
                        {
                            cmdInfo.ToRespFormat(ref writer);
                        }
                        else
                        {
                            writer.WriteNull();
                        }
                    }
                }
                finally
                {
                    writer.Dispose();
                }

                ProcessOutput(spam);
            }

            return true;
        }

        /// <summary>
        /// Processes COMMAND GETKEYS subcommand.
        /// </summary>
        private bool NetworkCOMMAND_GETKEYS()
        {
            if (parseState.Count == 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.COMMAND_GETKEYS));
            }

            var cmdName = parseState.GetString(0);

            // Try to parse command and get its simplified info
            if (!TryGetSimpleCommandInfo(cmdName, out var simpleCmdInfo))
                return AbortWithErrorMessage(CmdStrings.RESP_INVALID_COMMAND_SPECIFIED);

            // If command has no key specifications, abort with error
            if (simpleCmdInfo.KeySpecs == null || simpleCmdInfo.KeySpecs.Length == 0)
                return AbortWithErrorMessage(CmdStrings.RESP_COMMAND_HAS_NO_KEY_ARGS);

            // Extract command keys from parse state and key specification
            // An offset is applied to the parse state, as the command (and possibly subcommand) are included in the parse state.
            var slicedParseState = parseState.Slice(simpleCmdInfo.IsSubCommand ? 2 : 1);
            var keys = slicedParseState.ExtractCommandKeys(simpleCmdInfo);

            while (!RespWriteUtils.TryWriteArrayLength(keys.Length, ref dcurr, dend))
                SendAndReset();

            foreach (var key in keys)
            {
                while (!RespWriteUtils.TryWriteBulkString(key.Span, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Processes COMMAND GETKEYSANDFLAGS subcommand.
        /// </summary>
        private bool NetworkCOMMAND_GETKEYSANDFLAGS()
        {
            if (parseState.Count == 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.COMMAND_GETKEYSANDFLAGS));
            }

            var cmdName = parseState.GetString(0);

            // Try to parse command and get its simplified info
            if (!TryGetSimpleCommandInfo(cmdName, out var simpleCmdInfo))
                return AbortWithErrorMessage(CmdStrings.RESP_INVALID_COMMAND_SPECIFIED);

            // If command has no key specifications, abort with error
            if (simpleCmdInfo.KeySpecs == null || simpleCmdInfo.KeySpecs.Length == 0)
                return AbortWithErrorMessage(CmdStrings.RESP_COMMAND_HAS_NO_KEY_ARGS);

            // Extract command keys from parse state and key specification
            // An offset is applied to the parse state, as the command (and possibly subcommand) are included in the parse state.
            var slicedParseState = parseState.Slice(simpleCmdInfo.IsSubCommand ? 2 : 1);
            var keysAndFlags = slicedParseState.ExtractCommandKeysAndFlags(simpleCmdInfo);

            while (!RespWriteUtils.TryWriteArrayLength(keysAndFlags.Length, ref dcurr, dend))
                SendAndReset();

            for (var i = 0; i < keysAndFlags.Length; i++)
            {
                while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteBulkString(keysAndFlags[i].Item1.Span, ref dcurr, dend))
                    SendAndReset();

                var flags = EnumUtils.GetEnumDescriptions(keysAndFlags[i].Item2);
                WriteSetLength(flags.Length);

                foreach (var flag in flags)
                {
                    while (!RespWriteUtils.TryWriteBulkString(Encoding.ASCII.GetBytes(flag), ref dcurr, dend))
                        SendAndReset();
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
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER);
                }

                if (localRespProtocolVersion is < 2 or > 3)
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION);
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

                        if (!parseState.TryGetClientName(tokenIdx++, out tmpClientName))
                        {
                            return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_CLIENT_NAME);
                        }
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
                while (!RespWriteUtils.TryWriteError(errorMsg, ref dcurr, dend))
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

            while (!RespWriteUtils.TryWriteAsciiDirect(response, ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteError("ERR Client sent AUTH, but configured authenticator does not accept passwords"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // XXX: There should be high-level AuthenticatorException
            if (this.AuthenticateUser(username, password))
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (username.IsEmpty)
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_WRONGPASS_INVALID_PASSWORD, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_WRONGPASS_INVALID_USERNAME_PASSWORD, ref dcurr, dend))
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
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }

                // Validate samples count
                if (!parseState.TryGetInt(2, out var samples))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }

                if (samples < 0)
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }
            }

            var status = storageApi.MemoryUsageForKey(key, out var memoryUsage);

            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.TryWriteInt32((int)memoryUsage, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                WriteNull();
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
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_NOT_SUPPORTED_RESP2);
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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Process the HELLO command
        /// </summary>
        void ProcessHelloCommand(byte? respProtocolVersion, ReadOnlySpan<byte> username, ReadOnlySpan<byte> password, string clientName)
        {
            // Per RESP3 specifications and observed reference behaviour,
            // every failure in validation or authentication should prevent
            // the entire command from being run. For example,
            // "HELLO 3 AUTH user password SETNAME name" should not change protocol or name if
            // the user authentication fails for any reason.
            //
            // So we must do things in a particular order:
            // First, validations, if passes then user authentication, and only then if it's fine,
            // protocol and name changes.
            if (respProtocolVersion.HasValue && (respProtocolVersion.Value != this.respProtocolVersion) &&
                (asyncCompleted < asyncStarted))
            {
                WriteError(CmdStrings.RESP_ERR_ASYNC_PROTOCOL_CHANGE);
                return;
            }

            if (!username.IsEmpty)
            {
                if (!this.AuthenticateUser(username, password))
                {
                    if (username.IsEmpty)
                    {
                        WriteError(CmdStrings.RESP_WRONGPASS_INVALID_PASSWORD);
                    }
                    else
                    {
                        WriteError(CmdStrings.RESP_WRONGPASS_INVALID_USERNAME_PASSWORD);
                    }
                    return;
                }
            }

            if (respProtocolVersion.HasValue && (respProtocolVersion.Value != this.respProtocolVersion))
            {
                UpdateRespProtocolVersion(respProtocolVersion.Value);
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
                    ("proto", (long)this.respProtocolVersion),
                    ("id", Id),
                    ("mode", storeWrapper.serverOptions.EnableCluster ? "cluster" : "standalone"),
                    ("role", storeWrapper.serverOptions.EnableCluster && storeWrapper.clusterProvider.IsReplica() ? "replica" : "master"),
                ];

            WriteMapLength(helloResult.Length + 1);
            for (var i = 0; i < helloResult.Length; i++)
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(helloResult[i].Item1, ref dcurr, dend))
                    SendAndReset();
                if (helloResult[i].Item2 is long value)
                {
                    while (!RespWriteUtils.TryWriteInt64(value, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteAsciiBulkString(helloResult[i].Item2.ToString(), ref dcurr, dend))
                        SendAndReset();
                }
            }
            while (!RespWriteUtils.TryWriteAsciiBulkString("modules", ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();
                return;
            }

            if (async)
                Task.Run(() => ExecuteFlushDb(cmd, unsafeTruncateLog)).ConfigureAwait(false);
            else
                ExecuteFlushDb(cmd, unsafeTruncateLog);

            logger?.LogInformation($"Running {nameof(cmd)} {{async}} {{mode}}", async ? "async" : "sync", unsafeTruncateLog ? " with unsafetruncatelog." : string.Empty);
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
        }

        void ExecuteFlushDb(RespCommand cmd, bool unsafeTruncateLog)
        {
            switch (cmd)
            {
                case RespCommand.FLUSHDB:
                    storeWrapper.FlushDatabase(unsafeTruncateLog, activeDbId);
                    break;
                case RespCommand.FLUSHALL:
                    storeWrapper.FlushAllDatabases(unsafeTruncateLog);
                    break;
            }
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
            var user = targetSession._userHandle.User;
            var db = targetSession.activeDbId;
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

            into.Append($" db={db}");
            into.Append($" resp={resp}");
            into.Append($" lib-name={targetSession.clientLibName}");
            into.Append($" lib-ver={targetSession.clientLibVersion}");
        }

        bool ParseGETAndKey(ref SpanByte key)
        {
            var oldEndReadHead = readHead = endReadHead;
            var cmd = ParseCommand(writeErrorOnFailure: true, out var success);
            if (!success || cmd != RespCommand.GET)
            {
                // If we either find no command or a different command, we back off
                endReadHead = readHead = oldEndReadHead;
                return false;
            }
            key = parseState.GetArgSliceByRef(0).SpanByte;
            return true;
        }

        private bool TryGetSimpleCommandInfo(string cmdName, out SimpleRespCommandInfo simpleCmdInfo)
        {
            simpleCmdInfo = SimpleRespCommandInfo.Default;

            // Try to parse known command from name and obtain its command info
            if (!Enum.TryParse<RespCommand>(cmdName, true, out var cmd) ||
                !RespCommandsInfo.TryGetSimpleRespCommandInfo(cmd, out simpleCmdInfo, logger))
            {
                // If we no known command or info was found, attempt to find custom command
                if (storeWrapper.customCommandManager.TryGetCustomCommandInfo(cmdName, out var cmdInfo))
                {
                    cmdInfo.PopulateSimpleCommandInfo(ref simpleCmdInfo);
                }
                else
                {
                    // No matching command was found
                    return false;
                }
            }

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