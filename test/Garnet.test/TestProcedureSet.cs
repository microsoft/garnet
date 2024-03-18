// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Test procedure to use Set Commands in Garnet API
    /// 
    /// Format: SETPROC setA item1 item2 item3 item4 item5 item6 item7 item8 item9 item10 item3
    /// 
    /// Description: Exercise SADD SREM SCARD
    /// </summary>

    sealed class TestProcedureSet : CustomTransactionProcedure
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
            var elements = new ArgSlice[10];
            bool result = true;

            var setA = GetNextArg(input, ref offset);

            if (setA.Length == 0)
                result = false;

            if (result)
            {
                for (int i = 0; i < elements.Length; i++)
                {
                    elements[i] = GetNextArg(input, ref offset);
                }

                int count;
                api.SetAdd(setA, elements, out count);
                if (count != 10)
                    result = false;
                else
                {
                    ArgSlice elementremove = GetNextArg(input, ref offset);
                    api.SetRemove(setA, elementremove, out count);
                    if (count == 0)
                        result = false;
                    else
                    {
                        api.SetRemove(setA, elements[0..5], out count);
                        if (count != 4)
                            result = false;
                        api.SetRemove(setA, elements[0..5], out count);
                        if (count != 0)
                            result = false;
                        api.SetLength(setA, out count);
                        if (count != 5)
                            result = false;
                        api.SetMembers(setA, out var memberssetA);
                        if (memberssetA.Length != 5)
                            result = false;
                        api.SetPop(setA, out var _);
                        api.SetPop(setA, 2, out var _);
                        api.SetLength(setA, out count);
                        if (count != 2)
                            result = false;
                    }
                    api.SetScan(setA, 0, "*", 100, out var setItems);
                    if (setItems.Length != 3)
                        result = false;
                }
            }

            WriteSimpleString(ref output, result ? "SUCCESS" : "ERROR");
        }
    }
}