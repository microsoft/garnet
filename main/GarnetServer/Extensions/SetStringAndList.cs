// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace Garnet
{
    class SetStringAndList : CustomProcedure
    {
        public override bool Execute(IGarnetApi garnetApi, ref SessionParseState parseState, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(ref parseState, ref offset);
            var value = GetNextArg(ref parseState, ref offset);
            garnetApi.SET(key, value);

            // Create an object and set it
            var objKey = GetNextArg(ref parseState, ref offset);
            var objValue = GetNextArg(ref parseState, ref offset);
            garnetApi.ListRightPush(objKey, [objValue], out _);

            WriteSimpleString(ref output, "OK");
            return true;
        }
    }
}