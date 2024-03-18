// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
            api.SortedSetRemove(ssA, ssMembers[0], out count);
            if (count == 0)
            {
                result = false;
                goto returnToMain;
            }

            var status = api.SortedSetRange(ssA, minRange, maxRange, sortedSetOrderOperation: SortedSetOrderOperation.ByScore, out var elements, out string error, false, false, limit: ("1", 5));
            if (status == GarnetStatus.OK && error == default)
            {
                if (!elements[0].Span.SequenceEqual(ssItems[2].member.Bytes))
                    result = false;
                else
                {
                    status = api.SortedSetIncrement(ssA, 12345, ssMembers[0], out double newScore);
                    if (newScore != 12345)
                        result = false;
                    else
                    {
                        status = api.SortedSetRemoveRangeByScore(ssA, "12345", "12345", out int countRemoved);
                        if (countRemoved != 1)
                            result = false;
                        else
                        {
                            api.SortedSetRemove(ssA, ssMembers[0..5], out count);
                            if (count != 4)
                                result = false;
                        }
                    }
                }
            }
        returnToMain:
            WriteSimpleString(ref output, result ? "SUCCESS" : "ERROR");
        }
    }
}