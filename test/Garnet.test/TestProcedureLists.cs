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
    /// Test procedure to use List Commands in Garnet API
    /// 
    /// Format: LISTPROC listNameA listNameB item1 item2 item3 item4 item5 item6 item7 item8 item9 item10
    /// 
    /// Description: Exercise LPUSH LPOP RPUSH RPOP
    /// </summary>

    sealed class TestProcedureLists : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            var lstKey = GetNextArg(ref procInput, ref offset);
            var lstKeyB = GetNextArg(ref procInput, ref offset);
            var lstKeyC = GetNextArg(ref procInput, ref offset);

            if (lstKey.Length == 0 || lstKeyB.Length == 0 || lstKeyC.Length == 0)
                return false;

            AddKey(lstKey, LockType.Exclusive, StoreType.Object);
            AddKey(lstKeyB, LockType.Exclusive, StoreType.Object);
            AddKey(lstKeyC, LockType.Exclusive, StoreType.Object);

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
            var elements = new PinnedSpanByte[10];

            var lstKeyA = GetNextArg(ref procInput, ref offset);
            var lstKeyB = GetNextArg(ref procInput, ref offset);
            var lstKeyC = GetNextArg(ref procInput, ref offset);

            if (lstKeyA.Length == 0 || lstKeyB.Length == 0 || lstKeyC.Length == 0)
                return false;

            for (var i = 0; i < elements.Length; i++)
            {
                elements[i] = GetNextArg(ref procInput, ref offset);
            }

            var status = api.ListLeftPush(lstKeyA, elements, out var count);
            if (status != GarnetStatus.OK || count != 10)
                return false;

            status = api.ListLeftPop(lstKeyA, out var elementPopped);
            if (status != GarnetStatus.OK || !elementPopped.ReadOnlySpan.SequenceEqual(elements[9].ReadOnlySpan))
                return false;

            status = api.ListRightPush(lstKeyB, elements, out count);
            if (status != GarnetStatus.OK || count != 10)
                return false;

            status = api.ListLeftPop(lstKeyB, 2, out var elementsPopped);
            if (status != GarnetStatus.OK || elementsPopped.Length != 2 || !elementsPopped[0].ReadOnlySpan.SequenceEqual(elements[0].ReadOnlySpan)
                    || !elementsPopped[1].ReadOnlySpan.SequenceEqual(elements[1].ReadOnlySpan))
                return false;

            status = api.ListRightPop(lstKeyB, out elementPopped);
            if (status != GarnetStatus.OK || !elementPopped.ReadOnlySpan.SequenceEqual(elements[9].ReadOnlySpan))
                return false;

            status = api.ListLength(lstKeyB, out count);
            if (status != GarnetStatus.OK || count != 7)
                return false;

            status = api.ListMove(lstKeyA, lstKeyB, OperationDirection.Left, OperationDirection.Right, out var element);
            if (status != GarnetStatus.OK || !element.SequenceEqual(elements[8].ReadOnlySpan.ToArray()))
                return false;

            if (!api.ListTrim(lstKeyB, 1, 3))
                return false;

            status = api.ListLength(lstKeyB, out count);
            if (status != GarnetStatus.OK || count != 3)
                return false;

            // LPOP when list is empty
            status = api.ListLeftPop(lstKeyC, out var elementNotFound);
            if (status != GarnetStatus.NOTFOUND || elementNotFound.Length != 0)
                return false;

            return true;
        }
    }
}