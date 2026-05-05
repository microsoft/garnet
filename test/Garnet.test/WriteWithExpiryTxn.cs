// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        public override unsafe void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var key = GetNextArg(ref procInput, ref offset);
            var input = new StringInput(RespCommand.SETEX, ref procInput.parseState, startIdx: offset++, count: 1);
            var expiryMs = GetNextArg(ref procInput, ref offset);

            var expiryTicks = DateTimeOffset.UtcNow.Ticks +
                TimeSpan.FromMilliseconds(NumUtils.ReadInt64(expiryMs.Length, expiryMs.ToPointer())).Ticks;
            input.arg1 = expiryTicks;

            api.SET(key, ref input);
            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}