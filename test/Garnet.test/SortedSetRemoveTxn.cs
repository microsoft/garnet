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
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref SessionParseState parseState, int parseStateStartIdx)
        {
            var offset = 0;
            var subscriptionContainerKey = GetNextArg(ref parseState, parseStateStartIdx, ref offset);

            AddKey(subscriptionContainerKey, LockType.Exclusive, true);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref SessionParseState parseState, int parseStateStartIdx, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var subscriptionContainerKey = GetNextArg(ref parseState, parseStateStartIdx, ref offset);
            var subscriptionContainerEntry = GetNextArg(ref parseState, parseStateStartIdx, ref offset);

            api.SortedSetRemove(subscriptionContainerKey, subscriptionContainerEntry, out _);

            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}