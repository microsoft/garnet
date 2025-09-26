// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    sealed class SortedSetCountTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput input)
        {
            int offset = 0;
            AddKey(GetNextArg(ref input, ref offset), LockType.Shared, StoreType.Object);
            return true;
        }

        public override unsafe void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput input, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var key = GetNextArg(ref input, ref offset);
            var minScore = GetNextArg(ref input, ref offset);
            var maxScore = GetNextArg(ref input, ref offset);

            var status = api.SortedSetCount(key, minScore, maxScore, out int numElements);

            WriteSimpleString(ref output, numElements.ToString());
        }
    }
}