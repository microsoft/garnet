// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    class ProcCustomCmd : CustomProcedure
    {
        public override unsafe bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
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
            InvokeCustomRawStringCommand(garnetApi, "SETIFPM", key, args);

            return true;
        }
    }
}