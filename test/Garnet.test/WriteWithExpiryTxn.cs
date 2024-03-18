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
    /// Format: WriteWithExpiry 3 key value expiry
    /// 
    /// Description: Update key to given value with expiry
    /// </summary>
    sealed class WriteWithExpiryTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            int offset = 0;
            AddKey(GetNextArg(input, ref offset), LockType.Exclusive, false);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var key = GetNextArg(input, ref offset);
            var value = GetNextArg(input, ref offset);
            var expiryMs = GetNextArg(input, ref offset);

            api.SETEX(key, value, expiryMs);
            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}