// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
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

            while (!RespWriteUtils.WriteArrayLength(parseState.Count, ref dcurr, dend))
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
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
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
            while (!RespWriteUtils.WriteArrayLength(parseState.Count, ref dcurr, dend))
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
                            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
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
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
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
            Debug.Assert(parseState.Count % 2 == 0);

            for (int c = 0; c < parseState.Count; c += 2)
            {
                var key = parseState.GetArgSliceByRef(c).SpanByte;
                var val = parseState.GetArgSliceByRef(c + 1).SpanByte;
                _ = storageApi.SET(ref key, ref val);
            }
            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// MSETNX
        /// </summary>
        private bool NetworkMSETNX<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var anyValuesSet = false;
            var input = new RawStringInput(RespCommand.SETEXNX, ref parseState);

            for (var c = 0; c < parseState.Count; c += 2)
            {
                var key = parseState.GetArgSliceByRef(c).SpanByte;
                input.parseStateFirstArgIdx = c + 1;
                input.parseStateLastArgIdx = input.parseStateFirstArgIdx;

                var status = storageApi.SET_Conditional(ref key, ref input);

                // Status tells us whether an old image was found during RMW or not
                // For a "set if not exists", NOTFOUND means that the operation succeeded
                if (status == GarnetStatus.NOTFOUND)
                    anyValuesSet = true;
            }

            while (!RespWriteUtils.WriteInteger(anyValuesSet ? 1 : 0, ref dcurr, dend))
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

            while (!RespWriteUtils.WriteInteger(keysDeleted, ref dcurr, dend))
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
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (storeWrapper.serverOptions.EnableCluster)
            {
                // Cluster mode does not allow DBID
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SELECT_CLUSTER_MODE, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (index == 0)
                {
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_SELECT_INVALID_INDEX, ref dcurr, dend))
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

            while (!RespWriteUtils.WriteInteger(storageApi.GetDbSize(), ref dcurr, dend))
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
                while (!RespWriteUtils.WriteArrayLength(keys.Count, ref dcurr, dend))
                    SendAndReset();

                // Write the keys matching the pattern
                foreach (var item in keys)
                {
                    while (!RespWriteUtils.WriteBulkString(item, ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
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
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_INVALIDCURSOR, ref dcurr, dend))
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
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, ref dcurr, dend))
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
                while (!RespWriteUtils.WriteArrayLength(2, ref dcurr, dend))
                    SendAndReset();

                // Number of keys "0"
                while (!RespWriteUtils.WriteIntegerAsBulkString(0, ref dcurr, dend))
                    SendAndReset();

                // Empty array
                while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                // The response is two elements: the value of the cursor and the array of keys found matching the pattern
                while (!RespWriteUtils.WriteArrayLength(2, ref dcurr, dend))
                    SendAndReset();

                if (keys.Count > 0)
                    WriteOutputForScan(cursor, keys, ref dcurr, dend);
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
                while (!RespWriteUtils.WriteSimpleString(typeName, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteSimpleString("none"u8, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Writes the keys in the cursor to the output
        /// </summary>
        /// <param name="cursorValue">Cursor value to write to the output</param>
        /// <param name="keys">Keys to write to the output</param>
        /// <param name="curr">Pointer to output buffer</param>
        /// <param name="end">End of the output buffer</param>
        private void WriteOutputForScan(long cursorValue, List<byte[]> keys, ref byte* curr, byte* end)
        {
            // The output is an array of two elements: cursor value and an array of keys
            // Note the cursor value should be formatted as bulk string ('$')
            while (!RespWriteUtils.WriteIntegerAsBulkString(cursorValue, ref curr, end, out _))
                SendAndReset();

            // Write size of the array
            while (!RespWriteUtils.WriteArrayLength(keys.Count, ref curr, end))
                SendAndReset();

            // Write the keys matching the pattern
            for (int i = 0; i < keys.Count; i++)
            {
                while (!RespWriteUtils.WriteBulkString(keys[i], ref curr, end))
                    SendAndReset();
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

            WriteDirectLarge(new ReadOnlySpan<byte>(recvBufferPtr + readHead, endReadHead - readHead));
            return true;
        }
    }
}