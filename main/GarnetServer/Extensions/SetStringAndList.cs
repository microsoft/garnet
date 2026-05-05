// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace Garnet
{
    class SetStringAndList : CustomProcedure
    {
        public override bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(ref procInput, ref offset);

            var input = new StringInput(RespCommand.SET, ref procInput.parseState, startIdx: offset++, count: 1);
            garnetApi.SET(key, ref input);

            // Create an object and set it
            var objKey = GetNextArg(ref procInput, ref offset);
            var objValue = GetNextArg(ref procInput, ref offset);
            garnetApi.ListRightPush(objKey, [objValue], out _);

            WriteSimpleString(ref output, "OK");
            return true;
        }
    }
}