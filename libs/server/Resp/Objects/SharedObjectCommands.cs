// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;

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
            var key = parseState.GetArgSliceByRef(0);

            // Get cursor value
            if (!parseState.TryGetLong(1, out var cursorValue) || cursorValue < 0)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_INVALIDCURSOR);
            }

            var header = new RespInputHeader(objectType);
            var input = new ObjectInput(header, ref parseState, startIdx: 1,
                arg2: storeWrapper.serverOptions.ObjectScanCountLimit);

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

            // Prepare output
            var output = GetObjectOutput();
            var status = storageApi.ObjectScan(key, ref input, ref output);

            switch (status)
            {
                case GarnetStatus.OK:
                    // Process output
                    ProcessOutput(output.SpanByteAndMemory);
                    // Validation for partial input reading or error
                    if (output.result1 == int.MinValue)
                        return false;
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.TryWriteArrayLength(2, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteInt32AsBulkString(0, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.WRONGTYPE:
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }
    }
}