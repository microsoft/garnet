// Copyright (c) Microsoft Corporation.
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
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            AddKey(GetNextArg(ref procInput.parseState, ref offset), LockType.Exclusive, StoreType.Object);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(ref procInput.parseState, ref offset);
            var expiryMs = GetNextArg(ref procInput.parseState, ref offset);

            api.EXPIRE(key, expiryMs, out _);
            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}