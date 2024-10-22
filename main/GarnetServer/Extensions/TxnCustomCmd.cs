// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    class TxnCustomCmd : CustomTransactionProcedure
    {
        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output) => throw new System.NotImplementedException();
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput) => false;

        public override void Finalize<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            ArgSlice key = GetNextArg(ref procInput, ref offset);

            var cmdOutput = new SpanByteAndMemory(null);

            ArgSlice[] args = new ArgSlice[procInput.parseState.Count - 1];
            for (int i = 0; i < procInput.parseState.Count - 1; i++)
            {
                args[i] = GetNextArg(ref procInput, ref offset);
            }
            // id from registration of custom raw string cmd
            //garnetApi.CustomCommand(0, key, new ArgSlice(input.ptr + offset, input.length - offset), ref cmdOutput);
            //InvokeCustomRawStringCommand(garnetApi, "SETIFPM", key, args);

            //ExecuteCustomRawStringCommand(api, "SETIFPM", key, args, out var _output);

            ExecuteCustomObjectCommand(api, "MYDICTSET", key, args, out var _output);

            WriteSimpleString(ref output, "OK");
        }
    }
}
