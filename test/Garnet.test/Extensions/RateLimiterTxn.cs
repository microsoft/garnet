// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    sealed class RateLimiterTxn : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            int offset = 0;
            AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, StoreType.Object);
            return true;
        }

        public override unsafe void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var timeStamp = DateTime.Now.Ticks;
            var unixTimeInMilliSecond = timeStamp / TimeSpan.TicksPerMillisecond;

            var key = GetNextArg(ref procInput, ref offset);
            var slidingWindowInMilliSecondsSlice = GetNextArg(ref procInput, ref offset);
            var maxRequestsSlice = GetNextArg(ref procInput, ref offset);

            long slidingWindowInMilliSeconds;

            if (NumUtils.TryParse(slidingWindowInMilliSecondsSlice.ReadOnlySpan, out slidingWindowInMilliSeconds))
            {
                var trimTime = unixTimeInMilliSecond - slidingWindowInMilliSeconds;
                var status = api.SortedSetRemoveRangeByScore(key, 0.ToString(), trimTime.ToString(), out var _);

                if (status == GarnetStatus.OK || status == GarnetStatus.NOTFOUND)
                {
                    int sortedSetLength;
                    api.SortedSetLength(key, out sortedSetLength);
                    long maxRequestsVal;

                    if (NumUtils.TryParse(maxRequestsSlice.ReadOnlySpan, out maxRequestsVal))
                    {
                        if (sortedSetLength < maxRequestsVal)
                        {
                            var unixTimeInMilliSecondBytes = Encoding.ASCII.GetBytes(unixTimeInMilliSecond.ToString());

                            fixed (byte* unixTimeInMilliSecondPtr = unixTimeInMilliSecondBytes)
                            {
                                var timeInMicroSecond = timeStamp / (TimeSpan.TicksPerMillisecond / 1000);
                                var timeInMicroSecondBytes = Encoding.ASCII.GetBytes(timeInMicroSecond.ToString());
                                fixed (byte* timeInMicroSecondBytesPtr = timeInMicroSecondBytes)
                                {
                                    api.SortedSetAdd(key, PinnedSpanByte.FromPinnedPointer(unixTimeInMilliSecondPtr, unixTimeInMilliSecondBytes.Length), PinnedSpanByte.FromPinnedPointer(timeInMicroSecondBytesPtr, timeInMicroSecondBytes.Length), out var _);
                                    api.EXPIRE(key, TimeSpan.FromMilliseconds(slidingWindowInMilliSeconds), out _);
                                }
                            }

                            WriteSimpleString(ref output, "ALLOWED");
                            return;
                        }
                    }
                    else
                    {
                        WriteSimpleString(ref output, "FAILED");
                        return;
                    }
                }
            }
            else
            {
                WriteSimpleString(ref output, "FAILED");
                return;
            }

            WriteSimpleString(ref output, "THROTTLED");
        }
    }
}