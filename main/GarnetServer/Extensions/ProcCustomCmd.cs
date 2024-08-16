// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    class ProcCustomCmd : CustomProcedure
    {
        public override unsafe bool Execute(IGarnetApi garnetApi, ArgSlice input, ref MemoryResult<byte> output)
        {
            var offset = 0;
            ArgSlice key = GetNextArg(input, ref offset);
            var cmdOutput = new SpanByteAndMemory(null);

            // id from registration of custom raw string cmd
            garnetApi.CustomCommand(0, key, new ArgSlice(input.ptr + offset, input.length - offset), ref cmdOutput);

            return true;
        }
    }
}