// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace NoOpModule
{
    public class NoOpProc : CustomProcedure
    {
        public override bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput,
            ref MemoryResult<byte> output)
        {
            WriteSimpleString(ref output, "OK");
            return true;
        }
    }
}