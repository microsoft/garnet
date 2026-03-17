// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// RESP handler for RangeIndex commands.
    /// </summary>
    internal sealed unsafe partial class RespServerSession
    {
        /// <summary>
        /// Handles the RI.CREATE command.
        /// Syntax: RI.CREATE key [MEMORY | DISK path] [CACHESIZE n] [MINRECORD n] [MAXRECORD n] [MAXKEYLEN n] [PAGESIZE n]
        /// </summary>
        private bool NetworkRICREATE<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments("RI.CREATE");
            }

            var key = parseState.GetArgSliceByRef(0);

            // Defaults
            byte storageBackend = 1; // Memory by default
            string filePath = null;
            ulong cacheSize = 16 * 1024 * 1024; // 16 MiB
            uint minRecordSize = 64;
            uint maxRecordSize = 1024;
            uint maxKeyLen = 128;
            uint leafPageSize = 4096;

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
                    if (idx >= parseState.Count)
                    {
                        while (!RespWriteUtils.TryWriteError("ERR DISK requires a file path argument"u8, ref dcurr, dend))
                            SendAndReset();
                        return true;
                    }
                    filePath = parseState.GetArgSliceByRef(idx).ToString();
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
                    cacheSize = (ulong)parseState.GetLong(idx);
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
                    minRecordSize = (uint)parseState.GetLong(idx);
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
                    maxRecordSize = (uint)parseState.GetLong(idx);
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
                    maxKeyLen = (uint)parseState.GetLong(idx);
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
                    leafPageSize = (uint)parseState.GetLong(idx);
                    idx++;
                }
                else
                {
                    while (!RespWriteUtils.TryWriteError("ERR unknown option"u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            var status = storageApi.RangeIndexCreate(key, storageBackend, filePath,
                cacheSize, minRecordSize, maxRecordSize, maxKeyLen, leafPageSize,
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
        private bool NetworkRISET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 3)
                return AbortWithWrongNumberOfArguments("RI.SET");

            var key = parseState.GetArgSliceByRef(0);
            var field = parseState.GetArgSliceByRef(1);
            var value = parseState.GetArgSliceByRef(2);

            storageApi.RangeIndexSet(key, field, value, out var result, out var errorMsg);

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
        private bool NetworkRIGET<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments("RI.GET");

            var key = parseState.GetArgSliceByRef(0);
            var field = parseState.GetArgSliceByRef(1);

            storageApi.RangeIndexGet(key, field, out var valueBytes, out var result);

            if (result == RangeIndexResult.OK && valueBytes != null)
            {
                while (!RespWriteUtils.TryWriteBulkString(valueBytes, ref dcurr, dend))
                    SendAndReset();
            }
            else if (result == RangeIndexResult.NotFound)
            {
                while (!RespWriteUtils.TryWriteNull(ref dcurr, dend))
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
        /// Handles the RI.DEL command.
        /// Syntax: RI.DEL key field
        /// </summary>
        private bool NetworkRIDEL<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments("RI.DEL");

            var key = parseState.GetArgSliceByRef(0);
            var field = parseState.GetArgSliceByRef(1);

            storageApi.RangeIndexDel(key, field, out var result);

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
    }
}