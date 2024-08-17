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

            var key = parseState.GetArgSliceByRef(0).SpanByte;
            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            SpanByte input = default;
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
            var h = new RespInputHeader { cmd = RespCommand.ASYNC };
            var input = SpanByte.FromPinnedStruct(&h);
            var status = storageApi.GET_WithPending(ref key, ref input, ref o, asyncStarted, out bool pending);

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
            SpanByte input = default;
            long ctx = default;
            int firstPending = -1;
            (GarnetStatus, SpanByteAndMemory)[] outputArr = null;
            SpanByteAndMemory o = new(dcurr, (int)(dend - dcurr));
            int c = 0;

            for (; ; c++)
            {
                if (c > 0 && !ParseGETAndKey(ref key))
                    break;

                // Store index in context, since completions are not in order
                ctx = firstPending == -1 ? 0 : c - firstPending;

                var status = storageApi.GET_WithPending(ref key, ref input, ref o, ctx,
                    out bool isPending);

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
                for (int i = 0; i < c - firstPending; i++)
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

        bool ParseGETAndKey(ref SpanByte key)
        {
            var oldEndReadHead = readHead = endReadHead;
            var cmd = ParseCommand(out bool success);
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

        /// <summary>
        /// SET
        /// </summary>
        private bool NetworkSET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(parseState.count == 2);
            var key = parseState.GetArgSliceByRef(0).SpanByte;
            var value = parseState.GetArgSliceByRef(1).SpanByte;

            var status = storageApi.SET(ref key, ref value);

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// SETRANGE
        /// </summary>
        private bool NetworkSetRange<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var key = parseState.GetArgSliceByRef(0);

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

            var value = parseState.GetArgSliceByRef(2);

            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatInt64Length];
            var output = ArgSlice.FromPinnedSpan(outputBuffer);

            storageApi.SETRANGE(key, value, offset, ref output);

            while (!RespWriteUtils.WriteIntegerFromBytes(outputBuffer.Slice(0, output.Length), ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkGetRange<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var key = parseState.GetArgSliceByRef(0);
            var sbKey = key.SpanByte;


            if (!parseState.TryGetInt(1, out var sliceStart) || !parseState.TryGetInt(2, out var sliceLength))
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var keyPtr = sbKey.ToPointer() - sizeof(int); // length header
            *(int*)keyPtr = sbKey.Length;

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            var status = storageApi.GETRANGE(ref Unsafe.AsRef<SpanByte>(keyPtr), sliceStart, sliceLength, ref o);

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

            var val = parseState.GetArgSliceByRef(2).SpanByte;
            var valPtr = val.ToPointer() - (sizeof(int) + sizeof(long));
            var vSize = val.Length;

            // Save prior state on network buffer
            var save1 = *(int*)valPtr;
            var save2 = *(long*)(valPtr + sizeof(int));

            *(int*)valPtr = vSize + sizeof(long); // expiry info
            SpanByte.Reinterpret(valPtr).ExtraMetadata = DateTimeOffset.UtcNow.Ticks +
                                                         (highPrecision
                                                             ? TimeSpan.FromMilliseconds(expiry).Ticks
                                                             : TimeSpan.FromSeconds(expiry).Ticks);

            _ = storageApi.SET(ref key, ref Unsafe.AsRef<SpanByte>(valPtr));

            // Restore prior state on network buffer
            *(int*)valPtr = save1;
            *(long*)(valPtr + sizeof(int)) = save2;

            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
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
        /// SET EX NX
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
            var getValue = false;

            var tokenIdx = 2;
            Span<byte> nextOpt = default;
            var optUpperCased = false;
            while (tokenIdx < parseState.count || optUpperCased)
            {
                if (!optUpperCased)
                {
                    nextOpt = parseState.GetArgSliceByRef(tokenIdx++).Span;
                }

                if (nextOpt.SequenceEqual(CmdStrings.EX))
                {
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
                    tokenIdx++;
                    getValue = true;
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

            // Make space for key header
            var keyPtr = sbKey.ToPointer() - sizeof(int);

            // Set key length
            *(int*)keyPtr = sbKey.Length;

            // Make space for value header
            var valPtr = sbVal.ToPointer() - sizeof(int);
            var vSize = sbVal.Length;

            switch (expOption)
            {
                case ExpirationOption.None:
                case ExpirationOption.EX:
                    switch (existOptions)
                    {
                        case ExistOptions.None:
                            return getValue
                                ? NetworkSET_Conditional(RespCommand.SET, expiry, keyPtr, valPtr, vSize, true,
                                    false, ref storageApi)
                                : NetworkSET_EX(RespCommand.SET, expiry, keyPtr, valPtr, vSize, false,
                                    ref storageApi); // Can perform a blind update
                        case ExistOptions.XX:
                            return NetworkSET_Conditional(RespCommand.SETEXXX, expiry, keyPtr, valPtr, vSize,
                                getValue, false, ref storageApi);
                        case ExistOptions.NX:
                            return NetworkSET_Conditional(RespCommand.SETEXNX, expiry, keyPtr, valPtr, vSize,
                                getValue, false, ref storageApi);
                    }

                    break;
                case ExpirationOption.PX:
                    switch (existOptions)
                    {
                        case ExistOptions.None:
                            return getValue
                                ? NetworkSET_Conditional(RespCommand.SET, expiry, keyPtr, valPtr, vSize, true,
                                    true, ref storageApi)
                                : NetworkSET_EX(RespCommand.SET, expiry, keyPtr, valPtr, vSize, true,
                                    ref storageApi); // Can perform a blind update
                        case ExistOptions.XX:
                            return NetworkSET_Conditional(RespCommand.SETEXXX, expiry, keyPtr, valPtr, vSize,
                                getValue, true, ref storageApi);
                        case ExistOptions.NX:
                            return NetworkSET_Conditional(RespCommand.SETEXNX, expiry, keyPtr, valPtr, vSize,
                                getValue, true, ref storageApi);
                    }

                    break;

                case ExpirationOption.KEEPTTL:
                    Debug.Assert(expiry == 0); // no expiration if KEEPTTL
                    switch (existOptions)
                    {
                        case ExistOptions.None:
                            // We can never perform a blind update due to KEEPTTL
                            return NetworkSET_Conditional(RespCommand.SETKEEPTTL, expiry, keyPtr, valPtr, vSize,
                                getValue, false, ref storageApi);
                        case ExistOptions.XX:
                            return NetworkSET_Conditional(RespCommand.SETKEEPTTLXX, expiry, keyPtr, valPtr, vSize,
                                getValue, false, ref storageApi);
                        case ExistOptions.NX:
                            return NetworkSET_Conditional(RespCommand.SETEXNX, expiry, keyPtr, valPtr, vSize,
                                getValue, false, ref storageApi);
                    }

                    break;
            }

            while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_UNK_CMD, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkSET_EX<TGarnetApi>(RespCommand cmd, int expiry, byte* keyPtr, byte* valPtr,
            int vsize, bool highPrecision, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(cmd == RespCommand.SET);

            if (expiry == 0) // no expiration provided - this code path will not be currently hit as TrySET is used
            {
                *(int*)valPtr = vsize;
            }
            else
            {
                // Move payload forward to make space for metadata
                Buffer.MemoryCopy(valPtr + sizeof(int), valPtr + sizeof(int) + sizeof(long), vsize, vsize);
                *(int*)valPtr = vsize + sizeof(long);
                SpanByte.Reinterpret(valPtr).ExtraMetadata = DateTimeOffset.UtcNow.Ticks +
                                                             (highPrecision
                                                                 ? TimeSpan.FromMilliseconds(expiry).Ticks
                                                                 : TimeSpan.FromSeconds(expiry).Ticks);
            }

            storageApi.SET(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(valPtr));
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkSET_Conditional<TGarnetApi>(RespCommand cmd, int expiry, byte* keyPtr,
            byte* inputPtr, int isize, bool getValue, bool highPrecision, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // Make space for RespCommand in input
            inputPtr -= RespInputHeader.Size;

            if (expiry == 0) // no expiration provided
            {
                *(int*)inputPtr = RespInputHeader.Size + isize;
                ((RespInputHeader*)(inputPtr + sizeof(int)))->cmd = cmd;
                ((RespInputHeader*)(inputPtr + sizeof(int)))->flags = 0;
                if (getValue)
                    ((RespInputHeader*)(inputPtr + sizeof(int)))->SetSetGetFlag();
            }
            else
            {
                // Move payload forward to make space for metadata
                Buffer.MemoryCopy(inputPtr + sizeof(int) + RespInputHeader.Size,
                    inputPtr + sizeof(int) + sizeof(long) + RespInputHeader.Size, isize, isize);
                *(int*)inputPtr = sizeof(long) + RespInputHeader.Size + isize;
                ((RespInputHeader*)(inputPtr + sizeof(int) + sizeof(long)))->cmd = cmd;
                ((RespInputHeader*)(inputPtr + sizeof(int) + sizeof(long)))->flags = 0;
                if (getValue)
                    ((RespInputHeader*)(inputPtr + sizeof(int) + sizeof(long)))->SetSetGetFlag();
                SpanByte.Reinterpret(inputPtr).ExtraMetadata = DateTimeOffset.UtcNow.Ticks +
                                                               (highPrecision
                                                                   ? TimeSpan.FromMilliseconds(expiry).Ticks
                                                                   : TimeSpan.FromSeconds(expiry).Ticks);
            }

            if (getValue)
            {
                var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                var status = storageApi.SET_Conditional(ref Unsafe.AsRef<SpanByte>(keyPtr),
                    ref Unsafe.AsRef<SpanByte>(inputPtr), ref o);

                // Status tells us whether an old image was found during RMW or not
                if (status == GarnetStatus.NOTFOUND)
                {
                    Debug.Assert(o.IsSpanByte);
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    if (!o.IsSpanByte)
                        SendAndReset(o.Memory, o.Length);
                    else
                        dcurr += o.Length;
                }
            }
            else
            {
                var status = storageApi.SET_Conditional(ref Unsafe.AsRef<SpanByte>(keyPtr),
                    ref Unsafe.AsRef<SpanByte>(inputPtr));

                bool ok = status != GarnetStatus.NOTFOUND;

                // Status tells us whether an old image was found during RMW or not
                // For a "set if not exists", NOTFOUND means the operation succeeded
                // So we invert the ok flag
                if (cmd == RespCommand.SETEXNX)
                    ok = !ok;
                if (!ok)
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }

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

            var key = parseState.GetArgSliceByRef(0);
            var sbKey = key.SpanByte;

            ArgSlice input = default;
            if (cmd == RespCommand.INCRBY || cmd == RespCommand.DECRBY)
            {
                // Parse value argument
                // NOTE: Parse empty strings for better error messages through storageApi.Increment
                var sbVal = parseState.GetArgSliceByRef(1).SpanByte;
                var valPtr = sbVal.ToPointer() - RespInputHeader.Size;
                var vSize = sbVal.Length + RespInputHeader.Size;
                ((RespInputHeader*)valPtr)->cmd = cmd;
                ((RespInputHeader*)valPtr)->flags = 0;
                input = new ArgSlice(valPtr, vSize);
            }
            else if (cmd == RespCommand.INCR)
            {
                var vSize = RespInputHeader.Size + 1;
                var valPtr = stackalloc byte[vSize];
                ((RespInputHeader*)valPtr)->cmd = cmd;
                ((RespInputHeader*)valPtr)->flags = 0;
                *(valPtr + RespInputHeader.Size) = (byte)'1';
                input = new ArgSlice(valPtr, vSize);
            }
            else if (cmd == RespCommand.DECR)
            {
                var vSize = RespInputHeader.Size + 2;
                var valPtr = stackalloc byte[vSize];
                ((RespInputHeader*)valPtr)->cmd = cmd;
                ((RespInputHeader*)valPtr)->flags = 0;
                *(valPtr + RespInputHeader.Size) = (byte)'-';
                *(valPtr + RespInputHeader.Size + 1) = (byte)'1';
                input = new ArgSlice(valPtr, vSize);
            }

            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatInt64Length + 1];
            var output = ArgSlice.FromPinnedSpan(outputBuffer);

            storageApi.Increment(key, input, ref output);
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
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr,
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
            var sbVal = parseState.GetArgSliceByRef(1).SpanByte;

            var keyPtr = sbKey.ToPointer() - sizeof(int);
            var valPtr = sbVal.ToPointer() - sizeof(int);
            *(int*)keyPtr = sbKey.Length;
            *(int*)valPtr = sbVal.Length;

            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatInt64Length];
            var output = SpanByteAndMemory.FromPinnedSpan(outputBuffer);

            storageApi.APPEND(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(valPtr),
                ref output);

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
            if (parseState.count > 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.FLUSHDB), parseState.count);
            }

            FlushDb(RespCommand.FLUSHDB);

            return true;
        }

        /// <summary>
        /// FLUSHALL [ASYNC|SYNC] [UNSAFETRUNCATELOG]
        /// </summary>
        private bool NetworkFLUSHALL()
        {
            if (parseState.count > 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.FLUSHALL), parseState.count);
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
            if (parseState.count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.STRLEN), parseState.count);
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

            foreach (var customCmd in storeWrapper.customCommandManager.CustomCommandsInfo.Values)
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
        /// <param name="count">The number of arguments remaining in command buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkCOMMAND(int count)
        {
            // No additional args allowed
            if (count != 0)
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
        /// <param name="count">The number of arguments remaining in command buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkCOMMAND_COUNT(int count)
        {
            // No additional args allowed
            if (count != 0)
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
        /// <param name="count">The number of arguments remaining in command buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkCOMMAND_INFO(int count)
        {
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

                    if (RespCommandsInfo.TryGetRespCommandInfo(cmdName, out var cmdInfo, logger) ||
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

        private bool NetworkECHO(int count)
        {
            if (count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ECHO), count);
            }

            WriteDirectLarge(new ReadOnlySpan<byte>(recvBufferPtr + readHead, endReadHead - readHead));
            return true;
        }

        // HELLO [protover [AUTH username password] [SETNAME clientname]]
        private bool NetworkHELLO(int count)
        {
            if (count > 6)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.HELLO), count);
            }

            byte? tmpRespProtocolVersion = null;
            ReadOnlySpan<byte> authUsername = default, authPassword = default;
            string tmpClientName = null;
            string errorMsg = default;

            if (count > 0)
            {
                var tokenIdx = 0;
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

        private bool NetworkTIME(int count)
        {
            if (count != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.TIME), count);
            }

            var utcTime = DateTimeOffset.UtcNow;
            var seconds = utcTime.ToUnixTimeSeconds();
            var uSeconds = utcTime.ToString("ffffff");
            var response = $"*2\r\n${seconds.ToString().Length}\r\n{seconds}\r\n${uSeconds.Length}\r\n{uSeconds}\r\n";

            while (!RespWriteUtils.WriteAsciiDirect(response, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkAUTH(int count)
        {
            // AUTH [<username>] <password>
            if (count < 1 || count > 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.AUTH), count);
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
        private bool NetworkMemoryUsage<TGarnetApi>(int count, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (count != 1 && count != 3)
            {
                return AbortWithWrongNumberOfArguments(
                    $"{nameof(RespCommand.MEMORY)}|{Encoding.ASCII.GetString(CmdStrings.USAGE)}", count);
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
        private bool NetworkASYNC(int count)
        {
            if (respProtocolVersion <= 2)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_NOT_SUPPORTED_RESP2, ref dcurr, dend))
                    SendAndReset();

                return true;
            }

            if (count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.ASYNC), count);
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

            (string, string)[] helloResult =
                [
                    ("server", "redis"),
                    ("version", storeWrapper.redisProtocolVersion),
                    ("garnet_version", storeWrapper.version),
                    ("proto", $"{this.respProtocolVersion}"),
                    ("id", "63"),
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
                while (!RespWriteUtils.WriteAsciiBulkString(helloResult[i].Item2, ref dcurr, dend))
                    SendAndReset();
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

            var count = parseState.count;
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
    }
}