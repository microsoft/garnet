// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Test procedure to use List Commands in Garnet API
    /// 
    /// Format: LISTPROC listNameA listNameB item1 item2 item3 item4 item5 item6 item7 item8 item9 item10
    /// 
    /// Description: Exercise LPUSH LPOP RPUSH RPOP
    /// </summary>

    sealed class TestProcedureLists : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            int offset = 0;
            var lstKey = GetNextArg(input, ref offset);
            var lstKeyB = GetNextArg(input, ref offset);

            if (lstKey.Length == 0 || lstKeyB.Length == 0)
                return false;

            AddKey(lstKey, LockType.Exclusive, true);
            AddKey(lstKeyB, LockType.Exclusive, true);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            int offset = 0;
            var elements = new ArgSlice[10];
            bool result = true;

            var lstKey = GetNextArg(input, ref offset);
            var lstKeyB = GetNextArg(input, ref offset);

            if (lstKey.length == 0 || lstKeyB.Length == 0)
                result = false;

            if (result)
            {
                for (int i = 0; i < elements.Length; i++)
                {
                    elements[i] = GetNextArg(input, ref offset);
                }

                api.ListLeftPush(lstKey, elements, out int count);
                if (count != 10)
                    result = false;
                else
                {
                    api.ListLeftPop(lstKey, out ArgSlice elementPopped);
                    if (elementPopped.Length == 0)
                        result = false;
                    else
                    {
                        api.ListRightPush(lstKeyB, elements, out count);
                        if (count != 10)
                            result = false;
                        api.ListLeftPop(lstKeyB, 2, out ArgSlice[] _);
                        //remove right
                        api.ListRightPop(lstKeyB, out elementPopped);
                        api.ListLength(lstKeyB, out count);
                        if (elementPopped.length == 0 || count != 7)
                            result = false;
                        result = api.ListMove(lstKey, lstKeyB, OperationDirection.Left, OperationDirection.Right, out _);
                        if (result)
                        {
                            result = api.ListTrim(lstKeyB, 1, 3);
                        }
                    }
                }
            }

            WriteSimpleString(ref output, result ? "SUCCESS" : "ERROR");
        }
    }
}