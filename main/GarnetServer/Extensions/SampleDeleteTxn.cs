// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Sample stored procedure that deletes different keys in the main and object stores
    /// </summary>
    /*
    Sample Lua script for the same
    -- Delete main store key
    redis.call('DEL', @mainStoreKey)
    if @sortedSet1Key ~= '' then
        -- Remove the entry for the sorted set
        redis.call('ZREM', @sortedSet1Key, @sortedSet1Entry)
    end
        if @sortedSet2Key ~= '' then
        -- Remove the entry for the sorted set
        redis.call('ZREM', @sortedSet2Key, @sortedSet2Entry)
    end
    */
    sealed class SampleDeleteTxn : CustomTransactionProcedure
    {
        public override bool FailFastOnKeyLockFailure => true;

        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;

            var mainStoreKey = GetNextArg(ref procInput, ref offset);
            AddKey(mainStoreKey, LockType.Exclusive, StoreType.Main);

            var sortedSet1Key = GetNextArg(ref procInput, ref offset);
            if (sortedSet1Key.Length > 0)
                AddKey(sortedSet1Key, LockType.Exclusive, StoreType.Object);
            GetNextArg(ref procInput, ref offset); // sortedSet1Entry must be retrieved but is not used

            var sortedSet2Key = GetNextArg(ref procInput, ref offset);
            if (sortedSet2Key.Length > 0)
                AddKey(sortedSet2Key, LockType.Exclusive, StoreType.Object);
            // sortedSet2Entry is not used

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;

            var stringKey = GetNextArg(ref procInput, ref offset);
            api.DELETE(stringKey);

            var sortedSet1Key = GetNextArg(ref procInput, ref offset);
            var sortedSet1Entry = GetNextArg(ref procInput, ref offset);
            if (sortedSet1Key.Length > 0)
                api.SortedSetRemove(sortedSet1Key, sortedSet1Entry, out _);

            var sortedSet2Key = GetNextArg(ref procInput, ref offset);
            var sortedSet2Entry = GetNextArg(ref procInput, ref offset);
            if (sortedSet2Key.Length > 0)
                api.SortedSetRemove(sortedSet2Key, sortedSet2Entry, out _);

            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}