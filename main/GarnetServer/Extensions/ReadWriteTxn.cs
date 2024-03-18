// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom tranasction READWRITE - read one key, write to two keys
    /// 
    /// Format: READWRITE 3 readkey writekey1 writekey2
    /// 
    /// Description: Update key to given value only if the given prefix matches the 
    /// existing value's prefix. If it does not match (or there is no existing value), 
    /// then do nothing.
    /// </summary>
    sealed class ReadWriteTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            int offset = 0;
            api.GET(GetNextArg(input, ref offset), out var key1);
            if (key1.Span.ToString() == "wrong_string")
                return false;
            AddKey(GetNextArg(input, ref offset), LockType.Exclusive, false);
            AddKey(GetNextArg(input, ref offset), LockType.Exclusive, false);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var key1 = GetNextArg(input, ref offset);
            var key2 = GetNextArg(input, ref offset);
            var key3 = GetNextArg(input, ref offset);

            var status = api.GET(key1, out var result);
            if (status == GarnetStatus.OK)
            {
                api.SET(key2, result);
                api.SET(key3, result);
            }
            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}