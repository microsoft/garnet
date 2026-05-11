// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom tranasction Sorted set remove 
    /// 
    /// Format: SortedSetRemove 2 key member
    /// 
    /// Description: Remove member from key member
    /// </summary>
    sealed class SortedSetRemoveTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            var subscriptionContainerKey = GetNextArg(ref procInput.parseState, ref offset);

            AddKey(subscriptionContainerKey, LockType.Exclusive, StoreType.Object);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var subscriptionContainerKey = GetNextArg(ref procInput.parseState, ref offset);
            var subscriptionContainerEntry = GetNextArg(ref procInput.parseState, ref offset);

            api.SortedSetRemove(subscriptionContainerKey, subscriptionContainerEntry, out _);

            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}