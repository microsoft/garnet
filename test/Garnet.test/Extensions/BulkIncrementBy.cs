// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    sealed class BulkIncrementBy : CustomTransactionProcedure
    {
        // BULKINCRBY 2 a 10 [b 15] [c 25] ...
        public static readonly RespCommandsInfo CommandInfo = new() { Arity = -4 };

        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;

            var arg = GetNextArg(ref procInput, ref offset);
            if (!NumUtils.TryReadInt64(arg.ReadOnlySpan, out var count))
                return false;

            for (var i = 0; i < count; i++)
            {
                AddKey(GetNextArg(ref procInput, ref offset), LockType.Exclusive, storeType: StoreType.Main);
                GetNextArg(ref procInput, ref offset);
            }

            return true;
        }

        public override unsafe void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var arg = GetNextArg(ref procInput, ref offset);
            if (!NumUtils.TryReadInt64(arg.ReadOnlySpan, out var count))
            {
                WriteSimpleString(ref output, "FAILED parsing count parameter");
                return;
            }

            for (var i = 0; i < count; i++)
            {
                var key = GetNextArg(ref procInput, ref offset);
                arg = GetNextArg(ref procInput, ref offset);
                if (!NumUtils.TryReadInt64(arg.ReadOnlySpan, out var incrBy))
                {
                    WriteSimpleString(ref output, "FAILED parsing incrBy parameter");
                    return;
                }
                _ = api.Increment(key, out _, incrBy);
            }

            WriteSimpleString(ref output, "OK");
        }
    }
}