// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Sample stored procedure that updates different keys in the main and object stores
    /// </summary>
    /*
    Sample Lua script for the same
    -- Set main store key
    redis.call('SET', @mainStoreKey, @mainStoreValue)
    if @sortedSet1Key ~= '' then
        -- Add the entry in the sorted set
        redis.call('ZADD', @sortedSet1Key, @sortedSet1EntryScore, @sortedSet1Entry)
    end
        if @sortedSet2Key ~= '' then
        -- Add the entry in the sorted set
        redis.call('ZADD', @sortedSet2Key, @sortedSet2EntryScore, @sortedSet2Entry)
    end
    */
    sealed class SampleUpdateTxn : CustomTransactionProcedure
    {
        public override bool FailFastOnKeyLockFailure => true;

        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            int offset = 0;

            ArgSlice mainStoreKey = GetNextArg(input, ref offset);
            GetNextArg(input, ref offset); // mainStoreValue

            AddKey(mainStoreKey, LockType.Exclusive, false);

            ArgSlice sortedSet1Key = GetNextArg(input, ref offset);
            if (sortedSet1Key.Length > 0)
            {
                AddKey(sortedSet1Key, LockType.Exclusive, true);
            }

            GetNextArg(input, ref offset); // sortedSet1Entry
            GetNextArg(input, ref offset); // sortedSetScore

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
            ArgSlice mainStoreValue = GetNextArg(input, ref offset);

            api.SET(mainStoreKey, mainStoreValue);

            ArgSlice sortedSet1Key = GetNextArg(input, ref offset);
            ArgSlice sortedSet1Entry = GetNextArg(input, ref offset);
            ArgSlice sortedSet1EntryScore = GetNextArg(input, ref offset);


            if (sortedSet1Key.Length > 0)
            {
                api.SortedSetAdd(sortedSet1Key, sortedSet1EntryScore, sortedSet1Entry, out _);
            }

            ArgSlice sortedSet2Key = GetNextArg(input, ref offset);
            ArgSlice sortedSet2Entry = GetNextArg(input, ref offset);
            ArgSlice sortedSet2EntryScore = GetNextArg(input, ref offset);

            if (sortedSet2Key.Length > 0)
            {
                api.SortedSetAdd(sortedSet2Key, sortedSet2EntryScore, sortedSet2Entry, out _);
            }

            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}