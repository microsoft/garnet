// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Test procedure that keeps the results of the <c>IGarnetApi</c> hash reads while it issues further
    /// hash reads, then checks that the kept results are intact.
    ///
    /// Format: HASHRETAINED myhash otherhash otherfield field1 field2
    /// where myhash holds exactly field1 and field2 (with values value1 and value2).
    /// </summary>
    sealed class TestProcedureHashRetainedReads : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            var key = GetNextArg(ref procInput.parseState, ref offset);
            var other = GetNextArg(ref procInput.parseState, ref offset);

            if (key.Length == 0 || other.Length == 0)
                return false;

            AddKey(key, LockType.Exclusive, StoreType.Object);
            AddKey(other, LockType.Exclusive, StoreType.Object);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var key = GetNextArg(ref procInput.parseState, ref offset);
            var other = GetNextArg(ref procInput.parseState, ref offset);
            var otherField = GetNextArg(ref procInput.parseState, ref offset);
            var field1 = GetNextArg(ref procInput.parseState, ref offset);
            var field2 = GetNextArg(ref procInput.parseState, ref offset);

            if (api.HashGet(key, field1, out var single) != GarnetStatus.OK ||
                api.HashRandomField(key, out var singleRandom) != GarnetStatus.OK ||
                api.HashGetAll(key, out var all) != GarnetStatus.OK ||
                api.HashGetMultiple(key, [field1, field2], out var multiple) != GarnetStatus.OK ||
                api.HashRandomField(key, 2, false, out var random) != GarnetStatus.OK)
            {
                WriteSimpleString(ref output, "ERROR");
                return;
            }

            // Reads that reuse the stack space of the calls above
            for (var i = 0; i < 8; i++)
            {
                _ = api.HashGet(other, otherField, out _);
                _ = api.HashRandomField(other, out _);
                _ = api.HashGetMultiple(other, [otherField], out _);
                _ = api.HashGetAll(other, out _);
                _ = api.HashRandomField(other, 1, true, out _);
            }

            var actualSingle = single.ToString();
            var actualSingleRandom = singleRandom.ToString();
            var actualAll = Join(all);
            var actualMultiple = Join(multiple);

            // HRANDFIELD does not return its fields in a fixed order
            var actualRandom = string.Join(",", random.Select(static item => item.ToString()).OrderBy(static item => item, StringComparer.Ordinal));

            var ok = actualSingle == "value1" &&
                actualSingleRandom is "field1" or "field2" &&
                actualAll == "field1,value1,field2,value2" &&
                actualMultiple == "value1,value2" &&
                actualRandom == "field1,field2";

            WriteSimpleString(ref output, ok ? "OK" : $"CORRUPT:{actualSingle}|{actualSingleRandom}|{actualAll}|{actualMultiple}|{actualRandom}");
        }

        private static string Join(PinnedSpanByte[] items)
        {
            var parts = items.Select(static item => item.ToString()).ToArray();
            return string.Join(",", parts);
        }
    }
}