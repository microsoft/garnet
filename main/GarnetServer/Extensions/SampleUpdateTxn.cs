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

        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref SessionParseState parseState)
        {
            var offset = 0;

            var mainStoreKey = GetNextArg(ref parseState, ref offset);
            GetNextArg(ref parseState, ref offset); // mainStoreValue

            AddKey(mainStoreKey, LockType.Exclusive, false);

            var sortedSet1Key = GetNextArg(ref parseState, ref offset);
            if (sortedSet1Key.Length > 0)
            {
                AddKey(sortedSet1Key, LockType.Exclusive, true);
            }

            GetNextArg(ref parseState, ref offset); // sortedSet1Entry
            GetNextArg(ref parseState, ref offset); // sortedSetScore

            var sortedSet2Key = GetNextArg(ref parseState, ref offset);
            if (sortedSet2Key.Length > 0)
            {
                AddKey(sortedSet2Key, LockType.Exclusive, true);
            }

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref SessionParseState parseState, ref MemoryResult<byte> output)
        {
            var offset = 0;

            var mainStoreKey = GetNextArg(ref parseState, ref offset);
            var mainStoreValue = GetNextArg(ref parseState, ref offset);

            api.SET(mainStoreKey, mainStoreValue);

            var sortedSet1Key = GetNextArg(ref parseState, ref offset);
            var sortedSet1Entry = GetNextArg(ref parseState, ref offset);
            var sortedSet1EntryScore = GetNextArg(ref parseState, ref offset);


            if (sortedSet1Key.Length > 0)
            {
                api.SortedSetAdd(sortedSet1Key, sortedSet1EntryScore, sortedSet1Entry, out _);
            }

            var sortedSet2Key = GetNextArg(ref parseState, ref offset);
            var sortedSet2Entry = GetNextArg(ref parseState, ref offset);
            var sortedSet2EntryScore = GetNextArg(ref parseState, ref offset);

            if (sortedSet2Key.Length > 0)
            {
                api.SortedSetAdd(sortedSet2Key, sortedSet2EntryScore, sortedSet2Entry, out _);
            }

            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}