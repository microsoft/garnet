// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using System.Text;
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
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            int offset = 0;
            var setA = GetNextArg(input, ref offset);

            if (setA.Length == 0)
                return false;

            AddKey(setA, LockType.Exclusive, true);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var pairs = new (ArgSlice field, ArgSlice value)[6];
            var fields = new ArgSlice[pairs.Length];
            bool result = true;

            var myHash = GetNextArg(input, ref offset);

            if (myHash.Length == 0)
                result = false;

            if (result)
            {
                for (int i = 0; i < pairs.Length; i++)
                {
                    pairs[i].field = GetNextArg(input, ref offset);
                    pairs[i].value = GetNextArg(input, ref offset);
                    fields[i] = pairs[i].field;
                }
                int count;
                api.HashSet(myHash, pairs, out count);
                if (count != pairs.Length)
                    result = false;
                else
                {
                    //HSETNX
                    api.HashSetWhenNotExists(myHash, pairs[0].field, pairs[0].value, out count);
                    if (count == 1)
                        result = false;
                    if (result)
                    {
                        //HGET
                        api.HashGet(myHash, pairs[0].field, out var value);
                        if (!value.Bytes.SequenceEqual(pairs[0].value.Bytes))
                            result = false;
                        if (result)
                        {
                            //HGETALL
                            api.HashGetAll(myHash, out var values);
                            if (!values[3].Bytes.SequenceEqual(pairs[1].value.Bytes))
                                result = false;
                            api.HashGet(myHash, fields[0..2], out values);
                            if (values.Length != 2)
                                result = false;
                            api.HashLength(myHash, out count);
                            if (count != 6)
                                result = false;
                            api.HashExists(myHash, pairs[0].field, out var exists);
                            if (!exists)
                                result = false;
                            api.HashRandomField(myHash, out var field);
                            if (field.Bytes.Length == 0)
                                result = false;
                            api.HashRandomField(myHash, 2, true, out var randFields);
                            if (randFields.Length != 4)
                                result = false;
                            ArgSlice elementremove = GetNextArg(input, ref offset);
                            api.HashDelete(myHash, elementremove, out count);
                            if (count != 1)
                                result = false;
                            api.HashScan(myHash, 0, "age", 5, out var items);
                            if (items.Length != 3 || !Encoding.ASCII.GetString(items[1].Bytes).StartsWith("age"))
                                result = false;
                        }
                    }
                }
            }

            WriteSimpleString(ref output, result ? "SUCCESS" : "ERROR");
        }
    }
}