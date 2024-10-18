﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Iterates over the associated items of a key,
        /// using a pattern to match and count to limit how many items to return.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="objectType">SortedSet, Hash or Set type</param>
        /// <param name="storageApi">The storageAPI object</param>
        /// <returns></returns>
        private unsafe bool ObjectScan<TGarnetApi>(GarnetObjectType objectType, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            // Check number of required parameters
            if (parseState.Count < 2)
            {
                var cmdName = objectType switch
                {
                    GarnetObjectType.Hash => nameof(HashOperation.HSCAN),
                    GarnetObjectType.Set => nameof(SetOperation.SSCAN),
                    GarnetObjectType.SortedSet => nameof(SortedSetOperation.ZSCAN),
                    GarnetObjectType.All => nameof(RespCommand.COSCAN),
                    _ => nameof(RespCommand.NONE)
                };

                return AbortWithWrongNumberOfArguments(cmdName);
            }

            // Read key for the scan
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyBytes = sbKey.ToByteArray();

            // Get cursor value
            if (!parseState.TryGetInt(1, out var cursorValue) || cursorValue < 0)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_GENERIC_CURSORVALUE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var header = new RespInputHeader(objectType);
            var input = new ObjectInput(header, ref parseState, 2, -1, cursorValue,
                storeWrapper.serverOptions.ObjectScanCountLimit);

            switch (objectType)
            {
                case GarnetObjectType.Hash:
                    input.header.HashOp = HashOperation.HSCAN;
                    break;
                case GarnetObjectType.Set:
                    input.header.SetOp = SetOperation.SSCAN;
                    break;
                case GarnetObjectType.SortedSet:
                    input.header.SortedSetOp = SortedSetOperation.ZSCAN;
                    break;
                case GarnetObjectType.All:
                    input.header.cmd = RespCommand.COSCAN;
                    break;
            }

            // Prepare GarnetObjectStore output
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(dcurr, (int)(dend - dcurr)) };
            var status = storageApi.ObjectScan(keyBytes, ref input, ref outputFooter);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    var objOutputHeader = ProcessOutputWithHeader(outputFooter.spanByteAndMemory);
                    // Validation for partial input reading or error
                    if (objOutputHeader.result1 == int.MinValue)
                        return false;
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteScanOutputHeader(0, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }
    }
}