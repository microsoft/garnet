// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace BDN.benchmark.CustomProcs
{
    class CustomProcSet : CustomProcedure
    {
        /// <summary>
        /// Parameters including command
        /// </summary>
        public const int Arity = 9;

        /// <summary>
        /// Command name
        /// </summary>
        public const string CommandName = "CPROCSET";

        /// <summary>
        /// CPROCSET key1 key2 key3 key4 value1 value2 value3 value4
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="api"></param>
        /// <param name="procInput"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        public override bool Execute<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var setA = GetNextArg(ref procInput, ref offset);
            var setB = GetNextArg(ref procInput, ref offset);
            var setC = GetNextArg(ref procInput, ref offset);
            var setD = GetNextArg(ref procInput, ref offset);

            var input = new StringInput(RespCommand.SET, ref procInput.parseState, startIdx: offset++, count: 1);
            _ = api.SET(setA, ref input);
            input.parseState = procInput.parseState.Slice(offset++, 1);
            _ = api.SET(setB, ref input);
            input.parseState = procInput.parseState.Slice(offset++, 1);
            _ = api.SET(setC, ref input);
            input.parseState = procInput.parseState.Slice(offset++, 1);
            _ = api.SET(setD, ref input);

            return true;
        }
    }
}