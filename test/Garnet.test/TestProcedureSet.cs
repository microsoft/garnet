// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
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
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref SessionParseState parseState)
        {
            var offset = 0;
            var setA = GetNextArg(ref parseState, ref offset);

            if (setA.Length == 0)
                return false;

            AddKey(setA, LockType.Exclusive, true);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref SessionParseState parseState, ref MemoryResult<byte> output)
        {
            var result = TestAPI(api, ref parseState);
            WriteSimpleString(ref output, result ? "SUCCESS" : "ERROR");
        }

        private static bool TestAPI<TGarnetApi>(TGarnetApi api, ref SessionParseState parseState) where TGarnetApi : IGarnetApi
        {
            var offset = 0;
            var elements = new ArgSlice[10];

            var setA = GetNextArg(ref parseState, ref offset);

            if (setA.Length == 0)
                return false;

            for (var i = 0; i < elements.Length; i++)
            {
                elements[i] = GetNextArg(ref parseState, ref offset);
            }

            var status = api.SetAdd(setA, elements.Take(9).ToArray(), out var count);
            if (status != GarnetStatus.OK || count != 9)
                return false;

            status = api.SetAdd(setA, elements[9], out count);
            if (status != GarnetStatus.OK || count != 1)
                return false;

            var toRemove = GetNextArg(ref parseState, ref offset);
            status = api.SetRemove(setA, toRemove, out count);
            if (status != GarnetStatus.OK || count == 0)
                return false;

            status = api.SetRemove(setA, elements[0..5], out count);
            if (status != GarnetStatus.OK || count != 4)
                return false;

            status = api.SetRemove(setA, elements[0..5], out count);
            if (status != GarnetStatus.OK || count != 0)
                return false;

            status = api.SetLength(setA, out count);
            if (status != GarnetStatus.OK || count != 5)
                return false;

            status = api.SetMembers(setA, out var members);
            if (status != GarnetStatus.OK || members.Length != 5)
                return false;

            status = api.SetPop(setA, out var member);
            if (status != GarnetStatus.OK)
                return false;

            status = api.SetPop(setA, 2, out members);
            if (status != GarnetStatus.OK || members.Length != 2)
                return false;

            status = api.SetLength(setA, out count);
            if (status != GarnetStatus.OK || count != 2)
                return false;

            status = api.SetScan(setA, 0, "*", 100, out var setItems);
            if (status != GarnetStatus.OK || setItems.Length != 3)
                return false;

            return true;
        }
    }
}