// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Test procedure to use ZADD/ZREM in Garnet API
    /// 
    /// Format: OBJPROCTEST ssA 1 "item1" 2 "item2" 3 "item3" 4 "item4" 5 "item5" 6 "item6" 7 "item7" 8 "item8" 9 "item9" 10 "item10" "1" "9" "*em*"
    /// 
    /// Description: Execute different sorted set commands with GarnetApi
    /// </summary>
    sealed class TestProcedureSortedSets : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref SessionParseState parseState, int parseStateFirstArgIdx)
        {
            var offset = 0;
            var ssA = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);

            if (ssA.Length == 0)
                return false;

            AddKey(ssA, LockType.Exclusive, true);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref SessionParseState parseState, int parseStateFirstArgIdx, ref MemoryResult<byte> output)
        {
            var result = TestAPI(api, ref parseState, parseStateFirstArgIdx);
            WriteSimpleString(ref output, result ? "SUCCESS" : "ERROR");
        }

        private static bool TestAPI<TGarnetApi>(TGarnetApi api, ref SessionParseState parseState, int parseStateFirstArgIdx) where TGarnetApi : IGarnetApi
        {
            var offset = 0;
            var ssItems = new (ArgSlice score, ArgSlice member)[10];
            var ssMembers = new ArgSlice[10];

            var ssA = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);

            for (var i = 0; i < ssItems.Length; i++)
            {
                ssItems[i].score = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);
                ssItems[i].member = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);
                ssMembers[i] = ssItems[i].member;
            }

            var minRange = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);
            var maxRange = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);
            var match = GetNextArg(ref parseState, parseStateFirstArgIdx, ref offset);

            var ssB = new ArgSlice();
            api.SortedSetAdd(ssB, ssItems[0].score, ssItems[0].member, out int count);
            if (count != 0)
                return false;

            api.SortedSetAdd(ssA, ssItems[0].score, ssItems[0].member, out count);
            if (count == 0)
                return false;

            api.SortedSetAdd(ssA, ssItems, out count);
            if (count != 9)
                return false;

            var strMatch = Encoding.ASCII.GetString(match.ReadOnlySpan);

            // Exercise SortedSetScan
            api.SortedSetScan(ssA, 0, strMatch, ssItems.Length, out ArgSlice[] itemsInScan);

            // The pattern "*em*" should match all items
            if (itemsInScan.Length != (ssItems.Length * 2) + 1)
                return false;

            // Exercise SortedSetScan no match
            api.SortedSetScan(ssA, 0, "*q*", ssItems.Length, out itemsInScan);

            // Only return the value of the cursor
            if (itemsInScan.Length != 1)
                return false;

            // Exercise SortedSetRemove
            var status = api.SortedSetRemove(ssA, ssMembers[0], out count);
            if (status != GarnetStatus.OK || count != 1)
                return false;

            // Exercise SortedSetRank
            status = api.SortedSetRank(ssA, ssMembers[1], true, out var rank);
            if (status != GarnetStatus.OK || rank != 8)
                return false;

            // Exercise SortedSetPop
            status = api.SortedSetPop(ssA, out var pairs, 1, false);
            if (status != GarnetStatus.OK || pairs.Length != 1 || !pairs[0].member.ReadOnlySpan.SequenceEqual(ssMembers[9].ReadOnlySpan))
                return false;

            // Exercise SortedSetRange
            status = api.SortedSetRange(ssA, minRange, maxRange,
                sortedSetOrderOperation: SortedSetOrderOperation.ByScore, out var elements, out var error, false, false,
                limit: ("1", 5));
            if (status != GarnetStatus.OK || error != default || elements.Length != 5 || !elements.Zip(ssItems.Skip(2).Take(5),
                    (e, i) => e.ReadOnlySpan.SequenceEqual(i.member.ReadOnlySpan)).All(t => t))
                return false;

            // Exercise SortedSetIncrement
            status = api.SortedSetIncrement(ssA, 12345, ssMembers[0], out var newScore);
            if (status != GarnetStatus.OK || newScore != 12345)
                return false;

            // Exercise SortedSetRemoveRangeByScore
            status = api.SortedSetRemoveRangeByScore(ssA, "12345", "12345", out var countRemoved);
            if (status != GarnetStatus.OK || countRemoved != 1)
                return false;

            // Exercise SortedSetRemoveRangeByLex
            status = api.SortedSetRemoveRangeByLex(ssA, "(item7", "[item9", out countRemoved);
            if (status != GarnetStatus.OK || countRemoved != 2)
                return false;

            // Exercise SortedSetRemoveRangeByRank
            status = api.SortedSetRemoveRangeByRank(ssA, 1, 3, out countRemoved);
            if (status != GarnetStatus.OK || countRemoved != 3)
                return false;

            // Exercise SortedSetRemove
            status = api.SortedSetRemove(ssA, ssMembers[..6], out count);
            if (status != GarnetStatus.OK || count != 2)
                return false;

            // Exercise SortedSetLength 
            status = api.SortedSetLength(ssA, out var length);
            if (status != GarnetStatus.OK || length != 1)
                return false;

            return true;
        }
    }
}