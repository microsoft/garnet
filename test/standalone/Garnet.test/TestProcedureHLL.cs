// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Test procedure to use HyperLogLog Commands in Garnet API
    /// 
    /// Format: HLLPROC hll e1 e2 e3 e4 e5 e6 e7
    /// 
    /// Description: Exercise PFADD PFCOUNT
    /// </summary>

    sealed class TestProcedureHLL : CustomTransactionProcedure
    {
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            var hll = GetNextArg(ref procInput.parseState, ref offset);

            if (hll.Length == 0)
                return false;

            AddKey(hll, LockType.Exclusive, StoreType.Main);
            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var elements = new string[7];
            var result = true;

            var hll = GetNextArg(ref procInput.parseState, ref offset);

            if (hll.Length == 0)
                result = false;

            if (result)
            {
                for (var i = 0; i < elements.Length; i++)
                {
                    elements[i] = Encoding.ASCII.GetString(GetNextArg(ref procInput.parseState, ref offset).ToArray());
                }
                api.HyperLogLogAdd(hll, elements, out var resultPfAdd);
                result = resultPfAdd;
                api.HyperLogLogLength([hll], out var count);
                if (count != 7)
                {
                    result = false;
                }
            }
            WriteSimpleString(ref output, result ? "SUCCESS" : "ERROR");
        }
    }
}