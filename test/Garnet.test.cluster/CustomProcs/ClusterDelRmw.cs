// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    sealed class ClusterDelRmw : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, isObject: false);
            return true;
        }

        public override unsafe void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var timeStamp = DateTime.Now.Ticks;
            var unixTimeInMilliSecond = timeStamp / TimeSpan.TicksPerMillisecond;

            var key = GetNextArg(ref procInput, ref offset);
            var value = GetNextArg(ref procInput, ref offset);

            // Issue DELETE
            var status = api.DELETE(key);
            Debug.Assert(status == GarnetStatus.OK);

            var parsed = ParseUtils.TryReadLong(ref value, out var valueToIncrement);
            Debug.Assert(parsed, "Value to increment must be a valid long integer.");

            var input = new RawStringInput(RespCommand.INCRBY, 0, valueToIncrement);
            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatInt64Length + 1];
            var outputArgSlice = ArgSlice.FromPinnedSpan(outputBuffer);
            // Increment key
            status = api.Increment(key, ref input, ref outputArgSlice);
            Debug.Assert(status == GarnetStatus.OK);

            WriteSimpleString(ref output, "OK");
        }
    }
}