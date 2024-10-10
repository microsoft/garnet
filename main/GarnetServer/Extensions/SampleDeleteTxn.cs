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

        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref SessionParseState parseState, int parseStateFirstArgIdx)
        {
            var offset = 0;

            var mainStoreKey = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);
            AddKey(mainStoreKey, LockType.Exclusive, false);

            var sortedSet1Key = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);
            if (sortedSet1Key.Length > 0)
            {
                AddKey(sortedSet1Key, LockType.Exclusive, true);
            }

            GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset); // sortedSet1Entry

            var sortedSet2Key = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);
            if (sortedSet2Key.Length > 0)
            {
                AddKey(sortedSet2Key, LockType.Exclusive, true);
            }

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref SessionParseState parseState, int parseStateFirstArgIdx, ref MemoryResult<byte> output)
        {
            var offset = 0;

            var mainStoreKey = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);

            api.DELETE(mainStoreKey, StoreType.Main);

            var sortedSet1Key = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);
            var sortedSet1Entry = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);

            if (sortedSet1Key.Length > 0)
            {
                api.SortedSetRemove(sortedSet1Key, sortedSet1Entry, out _);
            }

            var sortedSet2Key = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);
            var sortedSet2Entry = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);

            if (sortedSet2Key.Length > 0)
            {
                api.SortedSetRemove(sortedSet2Key, sortedSet2Entry, out _);
            }

            WriteSimpleString(ref output, "SUCCESS");
        }
    }
}