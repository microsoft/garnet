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
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            int offset = 0;
            AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, StoreType.Main);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var key = GetNextArg(ref procInput, ref offset);
            var value = GetNextArg(ref procInput, ref offset);
            var expiryMs = GetNextArg(ref procInput, ref offset);

            api.SETEX(key, value, expiryMs);
            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}