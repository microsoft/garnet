﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - array commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// MGET
        /// </summary>
        private bool NetworkMGET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (storeWrapper.serverOptions.EnableScatterGatherGet)
                return NetworkMGET_SG(ref storageApi);

            while (!RespWriteUtils.TryWriteArrayLength(parseState.Count, ref dcurr, dend))
                SendAndReset();

            RawStringInput input = default;

            for (var c = 0; c < parseState.Count; c++)
            {
                var key = parseState.GetArgSliceByRef(c).SpanByte;
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
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }
            return true;
        }

        /// <summary>
        /// MGET - scatter gather version
        /// </summary>
        private bool NetworkMGET_SG<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetAdvancedApi
        {
            var firstPending = -1;
            (GarnetStatus, SpanByteAndMemory)[] outputArr = null;

            // Write array length header
            while (!RespWriteUtils.TryWriteArrayLength(parseState.Count, ref dcurr, dend))
                SendAndReset();

            RawStringInput input = default;
            SpanByteAndMemory o = new(dcurr, (int)(dend - dcurr));

            for (var c = 0; c < parseState.Count; c++)
            {
                var key = parseState.GetArgSliceByRef(c).SpanByte;

                // Store index in context, since completions are not in order
                long ctx = c;

                var status = storageApi.GET_WithPending(ref key, ref input, ref o, ctx, out var isPending);

                if (isPending)
                {
                    if (firstPending == -1)
                    {
                        outputArr = new (GarnetStatus, SpanByteAndMemory)[parseState.Count];
                        firstPending = c;
                    }
                    outputArr[c] = (status, default);
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
                            outputArr[c] = (status, o);
                            o = new SpanByteAndMemory();
                        }
                    }
                    else
                    {
                        if (firstPending == -1)
                        {
                            // Realized not-found without IO, and no earlier pending, so we can add directly to the output
                            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                                SendAndReset();
                            o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                        }
                        else
                        {
                            outputArr[c] = (status, o);
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
                for (var i = firstPending; i < parseState.Count; i++)
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
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// MSET
        /// </summary>
        private bool NetworkMSET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count == 0 || parseState.Count % 2 != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.MSET));
            }

            for (int c = 0; c < parseState.Count; c += 2)
            {
                var key = parseState.GetArgSliceByRef(c).SpanByte;
                var val = parseState.GetArgSliceByRef(c + 1).SpanByte;
                _ = storageApi.SET(ref key, ref val);
            }
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// MSETNX
        /// </summary>
        private bool NetworkMSETNX<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count == 0 || parseState.Count % 2 != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.MSETNX));
            }

            var input = new RawStringInput(RespCommand.MSETNX, ref parseState);
            var status = storageApi.MSET_Conditional(ref input);

            // For a "set if not exists", NOTFOUND means that the operation succeeded
            while (!RespWriteUtils.TryWriteInt32(status == GarnetStatus.NOTFOUND ? 1 : 0, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkDEL<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            int keysDeleted = 0;
            for (int c = 0; c < parseState.Count; c++)
            {
                var key = parseState.GetArgSliceByRef(c).SpanByte;
                var status = storageApi.DELETE(ref key, StoreType.All);

                // This is only an approximate count because the deletion of a key on disk is performed as a blind tombstone append
                if (status == GarnetStatus.OK)
                    keysDeleted++;
            }

            while (!RespWriteUtils.TryWriteInt32(keysDeleted, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// SELECT
        /// </summary>
        /// <returns></returns>
        private bool NetworkSELECT()
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SELECT));
            }

            // Validate index
            if (!parseState.TryGetInt(0, out var index))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (index < storeWrapper.databaseNum)
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (storeWrapper.serverOptions.EnableCluster)
                {
                    // Cluster mode does not allow DBID
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_SELECT_CLUSTER_MODE, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_SELECT_INVALID_INDEX, ref dcurr, dend))
                        SendAndReset();
                }
            }
            return true;
        }

        private bool NetworkDBSIZE<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.DBSIZE));
            }

            while (!RespWriteUtils.TryWriteInt32(storageApi.GetDbSize(), ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkKEYS<TGarnetApi>(ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.KEYS));
            }

            // Read pattern for keys filter
            var patternSlice = parseState.GetArgSliceByRef(0);

            var keys = storageApi.GetDbKeys(patternSlice);

            if (keys.Count > 0)
            {
                // Write size of the array
                while (!RespWriteUtils.TryWriteArrayLength(keys.Count, ref dcurr, dend))
                    SendAndReset();

                // Write the keys matching the pattern
                foreach (var item in keys)
                {
                    while (!RespWriteUtils.TryWriteBulkString(item, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        private bool NetworkSCAN<TGarnetApi>(ref TGarnetApi storageApi)
              where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
                return AbortWithWrongNumberOfArguments("SCAN");

            // Validate scan cursor
            if (!parseState.TryGetLong(0, out var cursorFromInput))
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_INVALIDCURSOR, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var pattern = "*"u8;
            var patternArgSlice = ArgSlice.FromPinnedSpan(pattern);
            var allKeys = true;
            long countValue = 10;
            ReadOnlySpan<byte> typeParameterValue = default;

            var tokenIdx = 1;
            while (tokenIdx < parseState.Count)
            {
                var parameterWord = parseState.GetArgSliceByRef(tokenIdx++).ReadOnlySpan;

                if (parameterWord.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MATCH))
                {
                    // Read pattern for keys filter
                    patternArgSlice = parseState.GetArgSliceByRef(tokenIdx++);
                    pattern = patternArgSlice.ReadOnlySpan;

                    allKeys = pattern.Length == 1 && pattern[0] == '*';
                }
                else if (parameterWord.EqualsUpperCaseSpanIgnoringCase(CmdStrings.COUNT))
                {
                    // Validate count
                    if (!parseState.TryGetLong(tokenIdx++, out countValue))
                    {
                        while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                }
                else if (parameterWord.EqualsUpperCaseSpanIgnoringCase(CmdStrings.TYPE))
                {
                    typeParameterValue = parseState.GetArgSliceByRef(tokenIdx++).ReadOnlySpan;
                }
            }

            storageApi.DbScan(patternArgSlice, allKeys, cursorFromInput, out var cursor, out var keys,
                !typeParameterValue.IsEmpty ? long.MaxValue : countValue, typeParameterValue);

            // Prepare values for output
            if (keys.Count == 0)
            {
                while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                    SendAndReset();

                // Number of keys "0"
                while (!RespWriteUtils.TryWriteInt32AsBulkString(0, ref dcurr, dend))
                    SendAndReset();

                // Empty array
                while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                // The response is two elements: the value of the cursor and the array of keys found matching the pattern
                while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                    SendAndReset();

                if (keys.Count > 0)
                    WriteOutputForScan(cursor, keys);
            }

            return true;
        }

        private bool NetworkTYPE<TGarnetApi>(ref TGarnetApi storageApi)
              where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 1)
                return AbortWithWrongNumberOfArguments("TYPE");

            // TYPE key
            var keySlice = parseState.GetArgSliceByRef(0);

            var status = storageApi.GetKeyType(keySlice, out var typeName);

            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.TryWriteSimpleString(typeName, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteSimpleString("none"u8, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Writes the keys in the cursor to the output
        /// </summary>
        /// <param name="cursorValue">Cursor value to write to the output</param>
        /// <param name="keys">Keys to write to the output</param>
        private void WriteOutputForScan(long cursorValue, List<byte[]> keys)
        {
            // The output is an array of two elements: cursor value and an array of keys
            // Note the cursor value should be formatted as bulk string ('$')
            while (!RespWriteUtils.TryWriteInt64AsBulkString(cursorValue, ref dcurr, dend, out _))
                SendAndReset();

            // Write size of the array
            while (!RespWriteUtils.TryWriteArrayLength(keys.Count, ref dcurr, dend))
                SendAndReset();

            // Write the keys matching the pattern
            for (int i = 0; i < keys.Count; i++)
            {
                WriteDirectLargeRespString(keys[i]);
            }
        }

        private bool NetworkArrayPING()
        {
            var count = parseState.Count;
            if (count > 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PING));
            }

            if (count == 0)
            {
                return NetworkPING();
            }

            var message = parseState.GetArgSliceByRef(0).ReadOnlySpan;
            WriteDirectLargeRespString(message);
            return true;
        }

        private bool NetworkLCS<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 2)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.LCS));

            var key1 = parseState.GetArgSliceByRef(0);
            var key2 = parseState.GetArgSliceByRef(1);

            // Parse options
            var lenOnly = false;
            var withIndices = false;
            var minMatchLen = 0;
            var withMatchLen = false;
            var tokenIdx = 2;
            while (tokenIdx < parseState.Count)
            {
                var option = parseState.GetArgSliceByRef(tokenIdx++).ReadOnlySpan;

                if (option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.LEN))
                {
                    lenOnly = true;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.IDX))
                {
                    withIndices = true;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.MINMATCHLEN))
                {
                    if (tokenIdx + 1 > parseState.Count)
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                    }

                    if (!parseState.TryGetInt(tokenIdx++, out var minLen))
                    {
                        return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                    }

                    if (minLen < 0)
                    {
                        minLen = 0;
                    }

                    minMatchLen = minLen;
                }
                else if (option.EqualsUpperCaseSpanIgnoringCase(CmdStrings.WITHMATCHLEN))
                {
                    withMatchLen = true;
                }
                else
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_SYNTAX_ERROR);
                }
            }

            if (lenOnly && withIndices)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_LENGTH_AND_INDEXES);
            }

            var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            var status = storageApi.LCS(key1, key2, ref output, lenOnly, withIndices, withMatchLen, minMatchLen);

            if (!output.IsSpanByte)
                SendAndReset(output.Memory, output.Length);
            else
                dcurr += output.Length;

            return true;
        }
    }
}