// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace Garnet
{
    class Sum : CustomScriptProc
    {
        public override bool Execute(IGarnetApi garnetApi, ArgSlice input, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var sum = 0;
            ArgSlice key;

            while ((key = GetNextArg(input, ref offset)).Length > 0)
            {
                if (garnetApi.GET(key, out var value) == GarnetStatus.OK)
                {
                    // Sum the values
                    if (int.TryParse(value.ToString(), out var intValue))
                    {
                        sum += intValue;
                    }
                }
            }

            WriteSimpleString(ref output, sum.ToString());
            return true;
        }
    }
}