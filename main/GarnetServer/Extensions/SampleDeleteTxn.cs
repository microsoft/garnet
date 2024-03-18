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

        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            int offset = 0;

            ArgSlice mainStoreKey = GetNextArg(input, ref offset);
            AddKey(mainStoreKey, LockType.Exclusive, false);

            ArgSlice sortedSet1Key = GetNextArg(input, ref offset);
            if (sortedSet1Key.Length > 0)
            {
                AddKey(sortedSet1Key, LockType.Exclusive, true);
            }

            GetNextArg(input, ref offset); // sortedSet1Entry

            ArgSlice sortedSet2Key = GetNextArg(input, ref offset);
            if (sortedSet2Key.Length > 0)
            {
                AddKey(sortedSet2Key, LockType.Exclusive, true);
            }

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            int offset = 0;

            ArgSlice mainStoreKey = GetNextArg(input, ref offset);

            api.DELETE(mainStoreKey, StoreType.Main);

            ArgSlice sortedSet1Key = GetNextArg(input, ref offset);
            ArgSlice sortedSet1Entry = GetNextArg(input, ref offset);

            if (sortedSet1Key.Length > 0)
            {
                api.SortedSetRemove(sortedSet1Key, sortedSet1Entry, out _);
            }

            ArgSlice sortedSet2Key = GetNextArg(input, ref offset);
            ArgSlice sortedSet2Entry = GetNextArg(input, ref offset);

            if (sortedSet2Key.Length > 0)
            {
                api.SortedSetRemove(sortedSet2Key, sortedSet2Entry, out _);
            }

            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}