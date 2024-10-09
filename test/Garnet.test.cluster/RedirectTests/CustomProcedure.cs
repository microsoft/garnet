// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    /// <summary>
    /// MKROTxn key1 key2 key3 key4
    /// </summary>
    sealed class MultiKeyTransaction : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            var offset = 0;
            var setA = GetNextArg(input, ref offset);
            var setB = GetNextArg(input, ref offset);
            var getA = GetNextArg(input, ref offset);
            var getB = GetNextArg(input, ref offset);

            AddKey(setA, LockType.Exclusive, true);
            AddKey(setB, LockType.Exclusive, true);

            AddKey(getA, LockType.Shared, true);
            AddKey(getB, LockType.Shared, true);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {

        }
    }
}