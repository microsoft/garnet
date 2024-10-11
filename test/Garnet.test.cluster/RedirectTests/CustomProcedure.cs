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
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            var setA = GetNextArg(ref procInput, ref offset);
            var setB = GetNextArg(ref procInput, ref offset);
            var getA = GetNextArg(ref procInput, ref offset);
            var getB = GetNextArg(ref procInput, ref offset);

            AddKey(setA, LockType.Exclusive, true);
            AddKey(setB, LockType.Exclusive, true);

            AddKey(getA, LockType.Shared, true);
            AddKey(getB, LockType.Shared, true);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {

        }
    }
}