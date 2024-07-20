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
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            int offset = 0;
            var ssA = GetNextArg(input, ref offset);

            if (ssA.Length == 0)
                return false;

            AddKey(ssA, LockType.Exclusive, true);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var ssItems = new (ArgSlice score, ArgSlice member)[10];
            var ssMembers = new ArgSlice[10];

            var ssA = GetNextArg(input, ref offset);

            for (int i = 0; i < ssItems.Length; i++)
            {
                ssItems[i].score = GetNextArg(input, ref offset);
                ssItems[i].member = GetNextArg(input, ref offset);
                ssMembers[i] = ssItems[i].member;
            }

            var minRange = GetNextArg(input, ref offset);
            var maxRange = GetNextArg(input, ref offset);
            var match = GetNextArg(input, ref offset);

            bool result = true;
            ArgSlice ssB = new ArgSlice();
            api.SortedSetAdd(ssB, ssItems[0].score, ssItems[0].member, out int count);
            if (count != 0)
                result = false;

            api.SortedSetAdd(ssA, ssItems[0].score, ssItems[0].member, out count);
            if (count == 0)
                result = false;

            api.SortedSetAdd(ssA, ssItems, out count);
            if (count != 9)
                result = false;

            var strMatch = Encoding.ASCII.GetString(match.ReadOnlySpan);

            // Exercise SortedSetScan
            api.SortedSetScan(ssA, 0, strMatch, ssItems.Length, out ArgSlice[] itemsInScan);

            // The pattern "*em*" should match all items
            if (itemsInScan.Length != (ssItems.Length * 2) + 1)
            {
                result = false;
                goto returnToMain;
            }

            // Exercise SortedSetScan no match
            api.SortedSetScan(ssA, 0, "*q*", ssItems.Length, out itemsInScan);

            // Only return the value of the cursor
            if (itemsInScan.Length != 1)
            {
                result = false;
                goto returnToMain;
            }

            // Exercise SortedSetRemove
            var status = api.SortedSetRemove(ssA, ssMembers[0], out count);
            if (status != GarnetStatus.OK || count != 1)
            {
                result = false;
                goto returnToMain;
            }

            // Exercise SortedSetRank
            status = api.SortedSetRank(ssA, ssMembers[1], true, out var rank);
            if (status != GarnetStatus.OK || rank != 8)
            {
                result = false;
                goto returnToMain;
            }

            // Exercise SortedSetPop
            status = api.SortedSetPop(ssA, out var pairs, 1, false);
            if (status != GarnetStatus.OK || pairs.Length != 1 || !pairs[0].member.ReadOnlySpan.SequenceEqual(ssMembers[9].ReadOnlySpan))
            {
                result = false;
                goto returnToMain;
            }

            // Exercise SortedSetRange
            status = api.SortedSetRange(ssA, minRange, maxRange,
                sortedSetOrderOperation: SortedSetOrderOperation.ByScore, out var elements, out var error, false, false,
                limit: ("1", 5));
            if (status != GarnetStatus.OK || error != default || elements.Length != 5 || !elements.Zip(ssItems.Skip(2).Take(5),
                    (e, i) => e.ReadOnlySpan.SequenceEqual(i.member.ReadOnlySpan)).All(t => t))
            {
                result = false;
                goto returnToMain;
            }

            // Exercise SortedSetIncrement
            status = api.SortedSetIncrement(ssA, 12345, ssMembers[0], out var newScore);
            if (status != GarnetStatus.OK || newScore != 12345)
            {
                result = false;
                goto returnToMain;
            }

            // Exercise SortedSetRemoveRangeByScore
            status = api.SortedSetRemoveRangeByScore(ssA, "12345", "12345", out var countRemoved);
            if (status != GarnetStatus.OK || countRemoved != 1)
            {
                result = false;
                goto returnToMain;
            }

            // Exercise SortedSetRemoveRangeByLex
            status = api.SortedSetRemoveRangeByLex(ssA, "(item7", "[item9", out countRemoved);
            if (status != GarnetStatus.OK || countRemoved != 2)
            {
                result = false;
                goto returnToMain;
            }

            // Exercise SortedSetRemoveRangeByRank
            status = api.SortedSetRemoveRangeByRank(ssA, 1, 3, out countRemoved);
            if (status != GarnetStatus.OK || countRemoved != 3)
            {
                result = false;
                goto returnToMain;
            }

            // Exercise SortedSetRemove
            status = api.SortedSetRemove(ssA, ssMembers[..6], out count);
            if (status != GarnetStatus.OK || count != 2)
            {
                result = false;
                goto returnToMain;
            }

            // Exercise SortedSetLength 
            status = api.SortedSetLength(ssA, out var length);
            if (status != GarnetStatus.OK || length != 1)
            {
                result = false;
                goto returnToMain;
            }

        returnToMain:
            WriteSimpleString(ref output, result ? "SUCCESS" : "ERROR");
        }
    }
}