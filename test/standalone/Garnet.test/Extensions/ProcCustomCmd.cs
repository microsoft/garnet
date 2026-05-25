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
            var key = GetNextArg(ref procInput, ref offset);

            var args = new PinnedSpanByte[2];
            args[0] = GetNextArg(ref procInput, ref offset); // value to set
            args[1] = GetNextArg(ref procInput, ref offset); // prefix to match

            if (!ParseCustomRawStringCommand("SETIFPM", out var rawStringCommand))
                return false;

            ExecuteCustomRawStringCommand(garnetApi, rawStringCommand, key, args, out var _output);

            return true;
        }
    }
}