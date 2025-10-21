// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom tranasction READWRITE - read one key, write to two keys
    /// 
    /// Format: READWRITE 3 readkey writekey1 writekey2
    /// 
    /// Description: Update key to given value only if the given prefix matches the 
    /// existing value's prefix. If it does not match (or there is no existing value), 
    /// then do nothing.
    /// </summary>
    sealed class ReadWriteTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            int offset = 0;
            api.GET(GetNextArg(ref procInput, ref offset), out PinnedSpanByte key1);
            if (key1.ReadOnlySpan.SequenceEqual("wrong_string"u8))
                return false;
            AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, StoreType.Main);
            AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, StoreType.Main);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key1 = GetNextArg(ref procInput, ref offset);
            var key2 = GetNextArg(ref procInput, ref offset);
            var key3 = GetNextArg(ref procInput, ref offset);

            var status = api.GET(key1, out PinnedSpanByte result);
            if (status == GarnetStatus.OK)
            {
                api.SET(key2, result);
                api.SET(key3, result);
            }
            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}