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

        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;

            var mainStoreKey = GetNextArg(ref procInput, ref offset);
            GetNextArg(ref procInput, ref offset); // mainStoreValue must be retrieved but is not used
            AddKey(mainStoreKey, LockType.Exclusive, StoreType.Main);

            var sortedSet1Key = GetNextArg(ref procInput, ref offset);
            if (sortedSet1Key.Length > 0)
                AddKey(sortedSet1Key, LockType.Exclusive, StoreType.Object);
            GetNextArg(ref procInput, ref offset); // sortedSet1Entry must be retrieved but is not used
            GetNextArg(ref procInput, ref offset); // sortedSet1Score  must be retrieved but is not used

            var sortedSet2Key = GetNextArg(ref procInput, ref offset);
            if (sortedSet2Key.Length > 0)
                AddKey(sortedSet2Key, LockType.Exclusive, StoreType.Object);
            // sortedSet2Entry and sortedSet2Score are not used

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;

            var mainStoreKey = GetNextArg(ref procInput, ref offset);
            var mainStoreValue = GetNextArg(ref procInput, ref offset);
            api.SET(mainStoreKey, mainStoreValue);

            var sortedSet1Key = GetNextArg(ref procInput, ref offset);
            var sortedSet1Entry = GetNextArg(ref procInput, ref offset);
            var sortedSet1EntryScore = GetNextArg(ref procInput, ref offset);
            if (sortedSet1Key.Length > 0)
                api.SortedSetAdd(sortedSet1Key, sortedSet1EntryScore, sortedSet1Entry, out _);

            var sortedSet2Key = GetNextArg(ref procInput, ref offset);
            var sortedSet2Entry = GetNextArg(ref procInput, ref offset);
            var sortedSet2EntryScore = GetNextArg(ref procInput, ref offset);
            if (sortedSet2Key.Length > 0)
                api.SortedSetAdd(sortedSet2Key, sortedSet2EntryScore, sortedSet2Entry, out _);

            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}