// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom transaction GETTWOKEYSNOTXN - it will use the Finalize method of a stored procedure
    /// to read two keys in a non-transactional way. The transaction itself is empty.
    /// 
    /// Format: GETTWOKEYSNOTXN 2 getkey1 getkey2
    /// 
    /// Description: Read two keys without any transactional guarantee, return the values as an array of bulk strings
    /// </summary>
    sealed class GetTwoKeysNoTxn : CustomTransactionProcedure
    {
        /// <summary>
        /// No transactional phase, skip Prepare
        /// </summary>
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
            => false;

        /// <summary>
        /// Main will not be called because Prepare returns false
        /// </summary>
        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
            => throw new InvalidOperationException();

        /// <summary>
        /// Finalize reads two keys (non-transactionally) and return their values as an array of bulk strings
        /// </summary>
        public override void Finalize<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var key1 = GetNextArg(input, ref offset);
            var key2 = GetNextArg(input, ref offset);

            api.GET(key1, out var value1);
            api.GET(key2, out var value2);

            // Return the two keys as an array of bulk strings
            WriteBulkStringArray(ref output, value1, value2);
        }
    }
}