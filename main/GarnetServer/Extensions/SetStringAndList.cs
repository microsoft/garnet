// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace Garnet
{
    class SetStringAndList : CustomScriptProc
    {
        public override bool Execute(IGarnetApi garnetApi, ArgSlice input, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(input, ref offset);
            var value = GetNextArg(input, ref offset);
            garnetApi.SET(key, value);

            // Create an object and set it
            var objKey = GetNextArg(input, ref offset);
            var objValue = GetNextArg(input, ref offset);
            garnetApi.ListRightPush(objKey, [objValue], out _);

            WriteSimpleString(ref output, "OK");
            return true;
        }
    }
}