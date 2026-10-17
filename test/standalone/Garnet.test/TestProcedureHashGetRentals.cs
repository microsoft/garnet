// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Test procedure that issues a fixed number of calls to one <c>IGarnetApi</c> hash operation
    /// so that the pooled output-buffer rentals it causes can be counted.
    ///
    /// Format: HASHRENTALS op myhash field iterations
    /// </summary>
    sealed class TestProcedureHashGetRentals : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            _ = GetNextArg(ref procInput.parseState, ref offset);
            var key = GetNextArg(ref procInput.parseState, ref offset);

            if (key.Length == 0)
                return false;

            AddKey(key, LockType.Exclusive, StoreType.Object);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var op = GetNextArg(ref procInput.parseState, ref offset).ToString();
            var key = GetNextArg(ref procInput.parseState, ref offset);
            var field = GetNextArg(ref procInput.parseState, ref offset);

            if (!NumUtils.TryReadInt64(GetNextArg(ref procInput.parseState, ref offset).ReadOnlySpan, out var iterations))
            {
                WriteSimpleString(ref output, "ERROR");
                return;
            }

            var before = RespMemoryWriter.TotalOutputRentals;

            for (var i = 0; i < iterations; i++)
            {
                var status = op switch
                {
                    "HGET" => api.HashGet(key, field, out _),
                    "HLEN" => api.HashLength(key, out _),
                    "HEXISTS" => api.HashExists(key, field, out _),
                    "HGETALL" => api.HashGetAll(key, out _),
                    "HRANDFIELD" => api.HashRandomField(key, out _),
                    "HRANDFIELDCOUNT" => api.HashRandomField(key, 1, false, out _),
                    _ => GarnetStatus.NOTFOUND
                };

                if (status != GarnetStatus.OK)
                {
                    WriteSimpleString(ref output, "ERROR:" + op);
                    return;
                }
            }

            WriteSimpleString(ref output, (RespMemoryWriter.TotalOutputRentals - before).ToString());
        }
    }
}