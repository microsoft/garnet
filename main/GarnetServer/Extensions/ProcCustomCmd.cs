// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace Garnet
{
    class ProcCustomCmd : CustomProcedure
    {
        public override unsafe bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            ArgSlice key = GetNextArg(ref procInput, ref offset);

            //var cmdOutput = new SpanByteAndMemory(null);

            ArgSlice[] args = new ArgSlice[procInput.parseState.Count - 1];
            for (int i = 0; i < procInput.parseState.Count - 1; i++)
            {
                args[i] = GetNextArg(ref procInput, ref offset);
            }

            ExecuteCustomRawStringCommand(garnetApi, "SETIFPM", key, args, out var _output);
            return true;
        }
    }
}