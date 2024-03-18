// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom tranasction Write With Expiry - Write with Expiry
    /// 
    /// Format: ObjectExpiryTxn 2 key expiry
    /// 
    /// Description: Update key with expiry
    /// </summary>
    sealed class ObjectExpiryTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            int offset = 0;
            AddKey(GetNextArg(input, ref offset), LockType.Exclusive, true);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var key = GetNextArg(input, ref offset);
            var expiryMs = GetNextArg(input, ref offset);

            api.EXPIRE(key, expiryMs, out _, StoreType.Object);
            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}