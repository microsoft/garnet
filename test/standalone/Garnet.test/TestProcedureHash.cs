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
    /// Test procedure to use Hash Commands in Garnet API
    /// 
    /// Format: HASHPROC myhash field1 foo field2 faa field3 fii field4 fee field5 foo age 25, field1
    /// 
    /// Description: Exercise HSET, HSETNX, HGET, HGETALL, HLEN, HEXISTS, HDEL
    /// </summary>

    sealed class TestProcedureHash : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            var setA = GetNextArg(ref procInput.parseState, ref offset);

            if (setA.Length == 0)
                return false;

            AddKey(setA, LockType.Exclusive, StoreType.Object);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var result = TestAPI(api, ref procInput);
            WriteSimpleString(ref output, result ? "SUCCESS" : "ERROR");
        }

        private static bool TestAPI<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput) where TGarnetApi : IGarnetApi
        {
            var offset = 0;
            var pairs = new (PinnedSpanByte field, PinnedSpanByte value)[6];
            var fields = new PinnedSpanByte[pairs.Length];

            var myHash = GetNextArg(ref procInput.parseState, ref offset);

            if (myHash.Length == 0)
                return false;

            for (var i = 0; i < pairs.Length; i++)
            {
                pairs[i].field = GetNextArg(ref procInput.parseState, ref offset);
                pairs[i].value = GetNextArg(ref procInput.parseState, ref offset);
                fields[i] = pairs[i].field;
            }

            // HSET
            var status = api.HashSet(myHash, [.. pairs.Take(pairs.Length - 2)], out var count);
            if (status != GarnetStatus.OK || count != pairs.Length - 2)
                return false;

            // HSET
            status = api.HashSet(myHash, pairs[^2].field, pairs[^2].value, out count);
            if (status != GarnetStatus.OK || count != 1)
                return false;

            // HSETNX
            status = api.HashSetWhenNotExists(myHash, pairs[0].field, pairs[0].value, out count);
            if (status != GarnetStatus.OK || count != 0)
                return false;

            // HSETNX
            status = api.HashSetWhenNotExists(myHash, pairs[^1].field, pairs[^1].value, out count);
            if (status != GarnetStatus.OK || count != 1)
                return false;

            // HGET
            status = api.HashGet(myHash, pairs[0].field, out var value);
            if (status != GarnetStatus.OK || !value.ReadOnlySpan.SequenceEqual(pairs[0].value.ReadOnlySpan))
                return false;

            // HGETALL
            status = api.HashGetAll(myHash, out var values);
            if (status != GarnetStatus.OK || !values[3].ReadOnlySpan.SequenceEqual(pairs[1].value.ReadOnlySpan))
                return false;

            // HMGET
            status = api.HashGetMultiple(myHash, fields[0..2], out values);
            if (status != GarnetStatus.OK || values.Length != 2)
                return false;

            // HLEN
            status = api.HashLength(myHash, out count);
            if (status != GarnetStatus.OK || count != 6)
                return false;

            // HEXISTS
            status = api.HashExists(myHash, pairs[0].field, out var exists);
            if (status != GarnetStatus.OK || !exists)
                return false;

            // HRANDFIELD
            status = api.HashRandomField(myHash, out var field);
            if (status != GarnetStatus.OK || field.Length == 0)
                return false;

            // HRANDFIELD
            status = api.HashRandomField(myHash, 2, true, out var randFields);
            if (status != GarnetStatus.OK || randFields.Length != 4)
                return false;

            // HDEL
            var elementRemove = GetNextArg(ref procInput.parseState, ref offset);
            status = api.HashDelete(myHash, elementRemove, out count);
            if (status != GarnetStatus.OK || count != 1)
                return false;

            // HSCAN
            status = api.HashScan(myHash, 0, "age", 5, out var items);
            if (status != GarnetStatus.OK || items.Length != 3 || !items[1].ReadOnlySpan.StartsWith("age"u8))
                return false;

            // HGET (hashobject exists, field not found)
            status = api.HashGet(myHash, PinnedSpanByte.FromPinnedSpan("nonexistingfield"u8), out value);
            if (status != GarnetStatus.OK || value.Length != 0)
                return false;

            // HGET (hashobject not found)
            status = api.HashGet(PinnedSpanByte.FromPinnedSpan("nonexistinghash"u8), pairs[0].field, out value);
            if (status != GarnetStatus.NOTFOUND || value.Length != 0)
                return false;

            return true;
        }
    }
}