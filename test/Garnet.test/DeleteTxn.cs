// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom tranasction Delete key/object
    /// 
    /// Format: DeleteTxn 1 key
    /// 
    /// Description: Delete key
    /// </summary>
    sealed class DeleteTxn : CustomTransactionProcedure
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
            api.DELETE(key, StoreType.Main);
            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}