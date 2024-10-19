// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace BDN.benchmark
{
    sealed class CustomProcSet : CustomTransactionProcedure
    {
        /// <summary>
        /// Parameters including command
        /// </summary>
        public const int Arity = 9;

        /// <summary>
        /// Command name
        /// </summary>
        public const string CommandName = "CPBSET";

        ArgSlice setA;
        ArgSlice setB;
        ArgSlice setC;
        ArgSlice setD;

        ArgSlice valueA;
        ArgSlice valueB;
        ArgSlice valueC;
        ArgSlice valueD;

        /// <summary>
        ///  CPBSET key1 key2 key3 key4 value1 value2 value3 value4
        /// </summary>
        /// <typeparam name="TGarnetReadApi"></typeparam>
        /// <param name="api"></param>
        /// <param name="procInput"></param>
        /// <returns></returns>
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            var offset = 0;
            setA = GetNextArg(ref procInput, ref offset);
            setB = GetNextArg(ref procInput, ref offset);
            setC = GetNextArg(ref procInput, ref offset);
            setD = GetNextArg(ref procInput, ref offset);

            valueA = GetNextArg(ref procInput, ref offset);
            valueB = GetNextArg(ref procInput, ref offset);
            valueC = GetNextArg(ref procInput, ref offset);
            valueD = GetNextArg(ref procInput, ref offset);

            AddKey(setA, LockType.Exclusive, isObject: false);
            AddKey(setB, LockType.Exclusive, isObject: false);
            AddKey(setC, LockType.Exclusive, isObject: false);
            AddKey(setD, LockType.Exclusive, isObject: false);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            _ = api.SET(setA, valueA);
            _ = api.SET(setB, valueB);
            _ = api.SET(setC, valueC);
            _ = api.SET(setD, valueD);
        }
    }
}