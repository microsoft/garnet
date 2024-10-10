﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom transaction Write With Expiry - Write with Expiry
    /// 
    /// Format: ObjectExpiryTxn 2 key expiry
    /// 
    /// Description: Update key with expiry
    /// </summary>
    sealed class ObjectExpiryTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref SessionParseState parseState, int parseStateFirstArgIdx)
        {
            var offset = 0;
            AddKey(GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset), LockType.Exclusive, true);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref SessionParseState parseState, int parseStateFirstArgIdx, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);
            var expiryMs = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);

            api.EXPIRE(key, expiryMs, out _, StoreType.Object);
            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}