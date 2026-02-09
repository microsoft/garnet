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
            AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, StoreType.Main);
            return true;
        }

        public override unsafe void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;

            var key = GetNextArg(ref procInput, ref offset);
            var value = GetNextArg(ref procInput, ref offset);

            // Issue DELETE
            var status = api.DELETE(key);
            Debug.Assert(status == GarnetStatus.OK);

            var parsed = ParseUtils.TryReadLong(value, out var valueToIncrement);
            Debug.Assert(parsed, "Value to increment must be a valid long integer.");

            var input = new StringInput(RespCommand.INCRBY, 0, valueToIncrement);
            Span<byte> outputBuffer = stackalloc byte[NumUtils.MaximumFormatInt64Length + 1];
            var outputSpan = PinnedSpanByte.FromPinnedSpan(outputBuffer);
            StringOutput stringOutput = new(new SpanByteAndMemory(outputSpan));

            // Increment key
            _ = api.Increment(key, ref input, ref stringOutput);
            Debug.Assert(!stringOutput.HasError());

            WriteSimpleString(ref output, "OK");
        }
    }
}