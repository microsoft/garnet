// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom transaction Delete key/object
    /// 
    /// Format: DeleteTxn 1 key
    /// 
    /// Description: Delete key
    /// </summary>
    sealed class DeleteTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref SessionParseState parseState, int parseStateStartIdx)
        {
            var offset = 0;
            AddKey(GetNextArg(ref parseState, parseStateStartIdx, ref offset), LockType.Exclusive, false);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref SessionParseState parseState, int parseStateStartIdx, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(ref parseState, parseStateStartIdx, ref offset);
            api.DELETE(key, StoreType.Main);
            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}