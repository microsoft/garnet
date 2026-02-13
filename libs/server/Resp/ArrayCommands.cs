// Copyright (c) Microsoft Corporation.
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
            // Write array header
            while (!RespWriteUtils.TryWriteArrayLength(parseState.Count, ref dcurr, dend))
                SendAndReset();

            if (storeWrapper.serverOptions.EnableScatterGatherGet)
            {
                MGetReadArgBatch_SG batch = new(this);

                storageApi.ReadWithPrefetch(ref batch);
                batch.CompletePending(ref storageApi);
            }
            else
            {
                MGetReadArgBatch<TGarnetApi> batch = new(ref storageApi, this);
                storageApi.ReadWithPrefetch(ref batch);
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
                var key = parseState.GetArgSliceByRef(c);
                var val = parseState.GetArgSliceByRef(c + 1);
                _ = storageApi.SET(key, val);
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

            var input = new StringInput(RespCommand.MSETNX, ref parseState);
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
                var key = parseState.GetArgSliceByRef(c);
                var status = storageApi.DELETE(key);

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
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
            }

            if (index != 0 && storeWrapper.serverOptions.EnableCluster)
            {
                // Cluster mode does not allow non-zero DBID to be selected
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SELECT_CLUSTER_MODE);
            }

            if (index < 0 || index >= storeWrapper.serverOptions.MaxDatabases)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_DB_INDEX_OUT_OF_RANGE);
            }

            if (index == this.activeDbId || this.TrySwitchActiveDatabaseSession(index))
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                // Should never reach here
                Debug.Fail("Database SELECT should have succeeded.");
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_SELECT_UNSUCCESSFUL, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// SWAPDB index1 index2
        /// </summary>
        /// <returns></returns>
        private bool NetworkSWAPDB()
        {
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SWAPDB));
            }

            if (storeWrapper.serverOptions.EnableCluster)
            {
                // Cluster mode does not allow SWAPDB
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE);
            }

            // Validate index
            if (!parseState.TryGetInt(0, out var index1))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_FIRST_DB_INDEX);
            }

            if (!parseState.TryGetInt(1, out var index2))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_SECOND_DB_INDEX);
            }

            if (index1 < 0 ||
                index2 < 0 ||
                index1 >= storeWrapper.serverOptions.MaxDatabases ||
                index2 >= storeWrapper.serverOptions.MaxDatabases)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_DB_INDEX_OUT_OF_RANGE);
            }

            if (storeWrapper.TrySwapDatabases(index1, index2))
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_SWAPDB_UNSUPPORTED, ref dcurr, dend))
                    SendAndReset();
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
            if (!parseState.TryGetLong(0, out var cursorFromInput) || (cursorFromInput < 0))
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_INVALIDCURSOR);
            }

            var pattern = "*"u8;
            var patternArgSlice = PinnedSpanByte.FromPinnedSpan(pattern);
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

            // Prepare input
            var input = new UnifiedInput(RespCommand.TYPE);

            // Prepare UnifiedOutput output
            GetUnifiedOutput(out var output);

            var status = storageApi.TYPE(keySlice, ref input, ref output);

            if (status == GarnetStatus.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
            }
            else
            {
                while (!RespWriteUtils.TryWriteSimpleString(CmdStrings.none, ref dcurr, dend))
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

            GetStringOutput(out var output);
            var status = storageApi.LCS(key1, key2, ref output, lenOnly, withIndices, withMatchLen, minMatchLen);

            ProcessOutput(output.SpanByteAndMemory);

            return true;
        }
    }
}