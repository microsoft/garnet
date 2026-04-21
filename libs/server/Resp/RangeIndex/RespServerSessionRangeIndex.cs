// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server.BfTreeInterop;

namespace Garnet.server
{
    /// <summary>
    /// RESP network handler for RangeIndex commands (RI.CREATE, RI.SET, RI.GET, RI.DEL,
    /// RI.SCAN, RI.RANGE, RI.EXISTS, RI.CONFIG, RI.METRICS).
    ///
    /// <para>Each method parses RESP arguments from the network buffer, delegates to the
    /// corresponding <see cref="StorageSession"/> method, and writes the RESP response.</para>
    /// </summary>
    internal sealed unsafe partial class RespServerSession
    {
        /// <summary>
        /// Handles the RI.CREATE command.
        /// Syntax: RI.CREATE key [MEMORY | DISK] [CACHESIZE n] [MINRECORD n] [MAXRECORD n] [MAXKEYLEN n] [PAGESIZE n]
        /// </summary>
        /// <remarks>
        /// All numeric parameters must be greater than zero. MINRECORD must not exceed MAXRECORD.
        /// If PAGESIZE is not specified (or 0), it is auto-computed from MAXRECORD via
        /// <see cref="RangeIndexManager.ComputeLeafPageSize"/>.
        /// Duplicate RI.CREATE on the same key returns an error ("ERR index already exists").
        /// </remarks>
        /// <typeparam name="TGarnetApi">The Garnet API type.</typeparam>
        /// <param name="storageApi">Reference to the storage API.</param>
        /// <returns>Always <c>true</c> (command fully processed).</returns>
        private bool NetworkRICREATE<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storeWrapper.rangeIndexManager.IsEnabled)
                return AbortWithErrorMessage("ERR Range Index (preview) commands are not enabled");

            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments("RI.CREATE");
            }

            var key = parseState.GetArgSliceByRef(0);

            // Defaults
            byte storageBackend = 0; // Disk by default
            long cacheSize = 16 * 1024 * 1024; // 16 MiB
            long minRecordSize = 64;
            long maxRecordSize = 1024;
            long maxKeyLen = 128;
            long leafPageSize = 0; // 0 = auto-compute from maxRecordSize

            // Parse optional keyword arguments
            var idx = 1;
            while (idx < parseState.Count)
            {
                var arg = parseState.GetArgSliceByRef(idx).ReadOnlySpan;

                if (arg.EqualsUpperCaseSpanIgnoringCase("MEMORY"u8))
                {
                    storageBackend = 1;
                    idx++;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase("DISK"u8))
                {
                    storageBackend = 0;
                    idx++;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase("CACHESIZE"u8))
                {
                    idx++;
                    if (idx >= parseState.Count)
                    {
                        while (!RespWriteUtils.TryWriteError("ERR CACHESIZE requires a value"u8, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    cacheSize = parseState.GetLong(idx);
                    idx++;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase("MINRECORD"u8))
                {
                    idx++;
                    if (idx >= parseState.Count)
                    {
                        while (!RespWriteUtils.TryWriteError("ERR MINRECORD requires a value"u8, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    minRecordSize = parseState.GetLong(idx);
                    idx++;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase("MAXRECORD"u8))
                {
                    idx++;
                    if (idx >= parseState.Count)
                    {
                        while (!RespWriteUtils.TryWriteError("ERR MAXRECORD requires a value"u8, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    maxRecordSize = parseState.GetLong(idx);
                    idx++;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase("MAXKEYLEN"u8))
                {
                    idx++;
                    if (idx >= parseState.Count)
                    {
                        while (!RespWriteUtils.TryWriteError("ERR MAXKEYLEN requires a value"u8, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    maxKeyLen = parseState.GetLong(idx);
                    idx++;
                }
                else if (arg.EqualsUpperCaseSpanIgnoringCase("PAGESIZE"u8))
                {
                    idx++;
                    if (idx >= parseState.Count)
                    {
                        while (!RespWriteUtils.TryWriteError("ERR PAGESIZE requires a value"u8, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    leafPageSize = parseState.GetLong(idx);
                    idx++;
                }
                else
                {
                    while (!RespWriteUtils.TryWriteError("ERR unknown option"u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            // Validate numeric options
            if (cacheSize <= 0 || minRecordSize <= 0 || maxRecordSize <= 0 || maxKeyLen <= 0)
            {
                while (!RespWriteUtils.TryWriteError("ERR numeric options must be greater than zero"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (minRecordSize > maxRecordSize)
            {
                while (!RespWriteUtils.TryWriteError("ERR MINRECORD must not exceed MAXRECORD"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var status = storageApi.RangeIndexCreate(key, storageBackend,
                (ulong)cacheSize, (uint)minRecordSize, (uint)maxRecordSize, (uint)maxKeyLen, (uint)leafPageSize,
                out var result, out var errorMsg);

            if (result == RangeIndexResult.OK)
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (errorMsg.Length > 0)
                {
                    while (!RespWriteUtils.TryWriteError(errorMsg, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWriteError("ERR range index creation failed"u8, ref dcurr, dend))
                        SendAndReset();
                }
            }

            return true;
        }

        /// <summary>
        /// Handles the RI.SET command.
        /// Syntax: RI.SET key field value
        /// </summary>
        /// <remarks>
        /// Returns WRONGTYPE error if the key exists but is not a RangeIndex.
        /// Returns an error if the key doesn't exist or the field/value size exceeds limits.
        /// </remarks>
        /// <typeparam name="TGarnetApi">The Garnet API type.</typeparam>
        /// <param name="storageApi">Reference to the storage API.</param>
        /// <returns>Always <c>true</c> (command fully processed).</returns>
        private bool NetworkRISET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storeWrapper.rangeIndexManager.IsEnabled)
                return AbortWithErrorMessage("ERR Range Index (preview) commands are not enabled");

            if (parseState.Count != 3)
                return AbortWithWrongNumberOfArguments("RI.SET");

            var key = parseState.GetArgSliceByRef(0);
            var field = parseState.GetArgSliceByRef(1);
            var value = parseState.GetArgSliceByRef(2);

            var status = storageApi.RangeIndexSet(key, field, value, out var result, out var errorMsg);

            if (status == GarnetStatus.WRONGTYPE)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (result == RangeIndexResult.OK)
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (errorMsg.Length > 0)
                    while (!RespWriteUtils.TryWriteError(errorMsg, ref dcurr, dend))
                        SendAndReset();
                else
                    while (!RespWriteUtils.TryWriteError("ERR range index operation failed"u8, ref dcurr, dend))
                        SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Handles the RI.GET command.
        /// Syntax: RI.GET key field
        /// </summary>
        /// <remarks>
        /// Returns the value as a RESP bulk string if found, null bulk string if the field
        /// doesn't exist, or WRONGTYPE error if the key is not a RangeIndex.
        /// </remarks>
        /// <typeparam name="TGarnetApi">The Garnet API type.</typeparam>
        /// <param name="storageApi">Reference to the storage API.</param>
        /// <returns>Always <c>true</c> (command fully processed).</returns>
        private bool NetworkRIGET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storeWrapper.rangeIndexManager.IsEnabled)
                return AbortWithErrorMessage("ERR Range Index (preview) commands are not enabled");

            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments("RI.GET");

            var key = parseState.GetArgSliceByRef(0);
            var field = parseState.GetArgSliceByRef(1);

            var output = GetStringOutput();
            var status = storageApi.RangeIndexGet(key, field, ref output, out var result);

            if (status == GarnetStatus.WRONGTYPE)
            {
                SendAndReset();
                return true;
            }

            if (result == RangeIndexResult.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
            }
            else if (result == RangeIndexResult.NotFound)
            {
                WriteNull();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError("ERR range index not found"u8, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Handles the RI.DEL command.
        /// Syntax: RI.DEL key field
        /// </summary>
        /// <remarks>
        /// Returns :1 on success or WRONGTYPE error if the key is not a RangeIndex.
        /// Note: This deletes a field within the BfTree, not the entire RangeIndex key
        /// (use the standard DEL command to delete the key and free the BfTree).
        /// </remarks>
        /// <typeparam name="TGarnetApi">The Garnet API type.</typeparam>
        /// <param name="storageApi">Reference to the storage API.</param>
        /// <returns>Always <c>true</c> (command fully processed).</returns>
        private bool NetworkRIDEL<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storeWrapper.rangeIndexManager.IsEnabled)
                return AbortWithErrorMessage("ERR Range Index (preview) commands are not enabled");

            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments("RI.DEL");

            var key = parseState.GetArgSliceByRef(0);
            var field = parseState.GetArgSliceByRef(1);

            var status = storageApi.RangeIndexDel(key, field, out var result);

            if (status == GarnetStatus.WRONGTYPE)
            {
                SendAndReset();
                return true;
            }

            if (result == RangeIndexResult.OK)
            {
                while (!RespWriteUtils.TryWriteInt32(1, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteError("ERR range index not found"u8, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Handles the RI.SCAN command.
        /// Syntax: RI.SCAN key start COUNT n [FIELDS KEY|VALUE|BOTH]
        /// </summary>
        /// <remarks>
        /// Returns an array of results. Each element is either a bulk string (KEY or VALUE mode)
        /// or a 2-element array of [key, value] (BOTH mode, the default).
        /// </remarks>
        /// <typeparam name="TGarnetApi">The Garnet API type.</typeparam>
        /// <param name="storageApi">Reference to the storage API.</param>
        /// <returns>Always <c>true</c> (command fully processed).</returns>
        private bool NetworkRISCAN<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storeWrapper.rangeIndexManager.IsEnabled)
                return AbortWithErrorMessage("ERR Range Index (preview) commands are not enabled");

            if (parseState.Count < 4)
                return AbortWithWrongNumberOfArguments("RI.SCAN");

            var key = parseState.GetArgSliceByRef(0);
            var startKey = parseState.GetArgSliceByRef(1);

            if (!parseState.GetArgSliceByRef(2).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("COUNT"u8))
            {
                while (!RespWriteUtils.TryWriteError("ERR syntax error, expected COUNT"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (!parseState.TryGetInt(3, out var count) || count <= 0)
            {
                while (!RespWriteUtils.TryWriteError("ERR invalid count"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var returnField = ScanReturnField.KeyAndValue;
            if (parseState.Count >= 6 && parseState.GetArgSliceByRef(4).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("FIELDS"u8))
            {
                var fieldsVal = parseState.GetArgSliceByRef(5).ReadOnlySpan;
                if (fieldsVal.EqualsUpperCaseSpanIgnoringCase("KEY"u8))
                    returnField = ScanReturnField.Key;
                else if (fieldsVal.EqualsUpperCaseSpanIgnoringCase("VALUE"u8))
                    returnField = ScanReturnField.Value;
                else
                    returnField = ScanReturnField.KeyAndValue;
            }

            var output = GetStringOutput();
            var status = storageApi.RangeIndexScan(key, startKey, count, returnField,
                ref output, out _, out var result);

            if (status == GarnetStatus.WRONGTYPE)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (result == RangeIndexResult.MemoryModeNotSupported)
            {
                while (!RespWriteUtils.TryWriteError("ERR RI.SCAN is not supported for MEMORY-mode indexes"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (result != RangeIndexResult.OK)
            {
                while (!RespWriteUtils.TryWriteError("ERR range index not found"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        /// <summary>
        /// Handles the RI.RANGE command.
        /// Syntax: RI.RANGE key start end [FIELDS KEY|VALUE|BOTH]
        /// </summary>
        /// <remarks>
        /// Returns all entries in the closed range [start, end], ordered by key.
        /// Same response format as RI.SCAN.
        /// </remarks>
        /// <typeparam name="TGarnetApi">The Garnet API type.</typeparam>
        /// <param name="storageApi">Reference to the storage API.</param>
        /// <returns>Always <c>true</c> (command fully processed).</returns>
        private bool NetworkRIRANGE<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storeWrapper.rangeIndexManager.IsEnabled)
                return AbortWithErrorMessage("ERR Range Index (preview) commands are not enabled");

            if (parseState.Count < 3)
                return AbortWithWrongNumberOfArguments("RI.RANGE");

            var key = parseState.GetArgSliceByRef(0);
            var startKey = parseState.GetArgSliceByRef(1);
            var endKey = parseState.GetArgSliceByRef(2);

            var returnField = ScanReturnField.KeyAndValue;
            if (parseState.Count >= 5 && parseState.GetArgSliceByRef(3).ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase("FIELDS"u8))
            {
                var fieldsVal = parseState.GetArgSliceByRef(4).ReadOnlySpan;
                if (fieldsVal.EqualsUpperCaseSpanIgnoringCase("KEY"u8))
                    returnField = ScanReturnField.Key;
                else if (fieldsVal.EqualsUpperCaseSpanIgnoringCase("VALUE"u8))
                    returnField = ScanReturnField.Value;
                else
                    returnField = ScanReturnField.KeyAndValue;
            }

            var output = GetStringOutput();
            var status = storageApi.RangeIndexRange(key, startKey, endKey, returnField,
                ref output, out _, out var result);

            if (status == GarnetStatus.WRONGTYPE)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (result == RangeIndexResult.MemoryModeNotSupported)
            {
                while (!RespWriteUtils.TryWriteError("ERR RI.RANGE is not supported for MEMORY-mode indexes"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (result != RangeIndexResult.OK)
            {
                while (!RespWriteUtils.TryWriteError("ERR range index not found"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            ProcessOutput(output.SpanByteAndMemory);
            return true;
        }

        /// <summary>
        /// Handles the RI.EXISTS command.
        /// Syntax: RI.EXISTS key
        /// </summary>
        /// <remarks>
        /// Returns :1 if the key exists and is a RangeIndex, :0 otherwise.
        /// Unlike other RI commands, does NOT return WRONGTYPE for non-RI keys —
        /// it simply returns :0 for any key that is not an RI key.
        /// </remarks>
        /// <typeparam name="TGarnetApi">The Garnet API type.</typeparam>
        /// <param name="storageApi">Reference to the storage API.</param>
        /// <returns>Always <c>true</c> (command fully processed).</returns>
        private bool NetworkRIEXISTS<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storeWrapper.rangeIndexManager.IsEnabled)
                return AbortWithErrorMessage("ERR Range Index (preview) commands are not enabled");

            if (parseState.Count != 1)
                return AbortWithWrongNumberOfArguments("RI.EXISTS");

            var key = parseState.GetArgSliceByRef(0);

            storageApi.RangeIndexExists(key, out var exists);

            while (!RespWriteUtils.TryWriteInt32(exists ? 1 : 0, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Handles the RI.CONFIG command.
        /// Syntax: RI.CONFIG key
        /// </summary>
        /// <remarks>
        /// Returns the configuration of the index as 12 alternating field-value bulk strings:
        /// storage_backend, cache_size, min_record_size, max_record_size, max_key_len, leaf_page_size.
        /// </remarks>
        /// <typeparam name="TGarnetApi">The Garnet API type.</typeparam>
        /// <param name="storageApi">Reference to the storage API.</param>
        /// <returns>Always <c>true</c> (command fully processed).</returns>
        private bool NetworkRICONFIG<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storeWrapper.rangeIndexManager.IsEnabled)
                return AbortWithErrorMessage("ERR Range Index (preview) commands are not enabled");

            if (parseState.Count != 1)
                return AbortWithWrongNumberOfArguments("RI.CONFIG");

            var key = parseState.GetArgSliceByRef(0);

            var status = storageApi.RangeIndexConfig(key, out var storageBackend, out var cacheSize,
                out var minRecordSize, out var maxRecordSize, out var maxKeyLen, out var leafPageSize,
                out var result);

            if (status == GarnetStatus.WRONGTYPE)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (result != RangeIndexResult.OK)
            {
                while (!RespWriteUtils.TryWriteError("ERR range index not found"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // 6 fields × 2 (field name + value) = 12 elements
            while (!RespWriteUtils.TryWriteArrayLength(12, ref dcurr, dend))
                SendAndReset();

            // storage_backend
            while (!RespWriteUtils.TryWriteBulkString("storage_backend"u8, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteBulkString(storageBackend == 0 ? "DISK"u8 : "MEMORY"u8, ref dcurr, dend))
                SendAndReset();

            // cache_size
            while (!RespWriteUtils.TryWriteBulkString("cache_size"u8, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteAsciiBulkString(cacheSize.ToString(), ref dcurr, dend))
                SendAndReset();

            // min_record_size
            while (!RespWriteUtils.TryWriteBulkString("min_record_size"u8, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteAsciiBulkString(minRecordSize.ToString(), ref dcurr, dend))
                SendAndReset();

            // max_record_size
            while (!RespWriteUtils.TryWriteBulkString("max_record_size"u8, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteAsciiBulkString(maxRecordSize.ToString(), ref dcurr, dend))
                SendAndReset();

            // max_key_len
            while (!RespWriteUtils.TryWriteBulkString("max_key_len"u8, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteAsciiBulkString(maxKeyLen.ToString(), ref dcurr, dend))
                SendAndReset();

            // leaf_page_size
            while (!RespWriteUtils.TryWriteBulkString("leaf_page_size"u8, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteAsciiBulkString(leafPageSize.ToString(), ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Handles the RI.METRICS command.
        /// Syntax: RI.METRICS key
        /// </summary>
        /// <remarks>
        /// Returns 8 alternating field-value bulk strings:
        /// tree_handle (native pointer), is_live, is_flushed, is_recovered.
        /// Useful for diagnostics and testing the stub lifecycle.
        /// </remarks>
        /// <typeparam name="TGarnetApi">The Garnet API type.</typeparam>
        /// <param name="storageApi">Reference to the storage API.</param>
        /// <returns>Always <c>true</c> (command fully processed).</returns>
        private bool NetworkRIMETRICS<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (!storeWrapper.rangeIndexManager.IsEnabled)
                return AbortWithErrorMessage("ERR Range Index (preview) commands are not enabled");

            if (parseState.Count != 1)
                return AbortWithWrongNumberOfArguments("RI.METRICS");

            var key = parseState.GetArgSliceByRef(0);

            var status = storageApi.RangeIndexMetrics(key, out var treeHandle, out var isLive, out var isFlushed, out var isRecovered, out var result);

            if (status == GarnetStatus.WRONGTYPE)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (result != RangeIndexResult.OK)
            {
                while (!RespWriteUtils.TryWriteError("ERR range index not found"u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // 4 fields × 2 = 8 elements
            while (!RespWriteUtils.TryWriteArrayLength(8, ref dcurr, dend))
                SendAndReset();

            // tree_handle
            while (!RespWriteUtils.TryWriteBulkString("tree_handle"u8, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteAsciiBulkString(treeHandle.ToString(), ref dcurr, dend))
                SendAndReset();

            // is_live
            while (!RespWriteUtils.TryWriteBulkString("is_live"u8, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteBulkString(isLive ? "true"u8 : "false"u8, ref dcurr, dend))
                SendAndReset();

            // is_flushed
            while (!RespWriteUtils.TryWriteBulkString("is_flushed"u8, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteBulkString(isFlushed ? "true"u8 : "false"u8, ref dcurr, dend))
                SendAndReset();

            // is_recovered
            while (!RespWriteUtils.TryWriteBulkString("is_recovered"u8, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteBulkString(isRecovered ? "true"u8 : "false"u8, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}