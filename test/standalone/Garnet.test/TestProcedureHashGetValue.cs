// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Test procedure that returns the value <c>IGarnetApi.HashGet</c> produces, so that replies
    /// larger than the stack buffer it supplies can be checked.
    ///
    /// Format: HASHGETVALUE myhash field
    /// </summary>
    sealed class TestProcedureHashGetValue : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            var key = GetNextArg(ref procInput.parseState, ref offset);

            if (key.Length == 0)
                return false;

            AddKey(key, LockType.Exclusive, StoreType.Object);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(ref procInput.parseState, ref offset);
            var field = GetNextArg(ref procInput.parseState, ref offset);

            if (api.HashGet(key, field, out var value) != GarnetStatus.OK)
            {
                WriteError(ref output, "NOTFOUND");
                return;
            }

            WriteBulkString(ref output, value.Span);
        }
    }
}