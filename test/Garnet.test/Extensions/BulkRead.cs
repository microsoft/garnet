// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    sealed class BulkRead : CustomTransactionProcedure
    {
        // BULKREAD 3 a [b] [c]
        public static readonly RespCommandsInfo CommandInfo = new() { Arity = -3 };

        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;

            var arg = GetNextArg(ref procInput, ref offset);
            if (!NumUtils.TryReadInt64(arg.ReadOnlySpan, out var count))
                return false;

            for (var i = 0; i < count; i++)
                AddKey(GetNextArg(ref procInput, ref offset), LockType.Shared, storeType: StoreType.Main);

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

            var result = new PinnedSpanByte[count];

            for (var i = 0; i < count; i++)
            {
                var key = GetNextArg(ref procInput, ref offset);
                _ = api.GET(key, out result[i]);
            }
            WriteBulkStringArray(ref output, result);
        }
    }
}