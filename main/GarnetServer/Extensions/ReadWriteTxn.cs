// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref SessionParseState parseState, int parseStateStartIdx)
        {
            int offset = 0;
            api.GET(GetNextArg(ref parseState, parseStateStartIdx, ref offset), out var key1);
            if (key1.ReadOnlySpan.SequenceEqual("wrong_string"u8))
                return false;
            AddKey(GetNextArg(ref parseState, parseStateStartIdx, ref offset), LockType.Exclusive, false);
            AddKey(GetNextArg(ref parseState, parseStateStartIdx, ref offset), LockType.Exclusive, false);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref SessionParseState parseState, int parseStateStartIdx, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key1 = GetNextArg(ref parseState, parseStateStartIdx, ref offset);
            var key2 = GetNextArg(ref parseState, parseStateStartIdx, ref offset);
            var key3 = GetNextArg(ref parseState, parseStateStartIdx, ref offset);

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