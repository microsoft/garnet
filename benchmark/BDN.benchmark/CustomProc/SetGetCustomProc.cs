// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace BDN.benchmark
{
    sealed class CustomProcSetBench : CustomTransactionProcedure
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
        ///  CUSTOMPROCSETBENCH key1 key2 key3 key4 value1 value2 value3 value4
        /// </summary>
        /// <typeparam name="TGarnetReadApi"></typeparam>
        /// <param name="api"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ArgSlice input)
        {
            var offset = 0;
            setA = GetNextArg(input, ref offset);
            setB = GetNextArg(input, ref offset);
            setC = GetNextArg(input, ref offset);
            setD = GetNextArg(input, ref offset);

            valueA = GetNextArg(input, ref offset);
            valueB = GetNextArg(input, ref offset);
            valueC = GetNextArg(input, ref offset);
            valueD = GetNextArg(input, ref offset);

            AddKey(setA, LockType.Exclusive, isObject: false);
            AddKey(setB, LockType.Exclusive, isObject: false);
            AddKey(setC, LockType.Exclusive, isObject: false);
            AddKey(setD, LockType.Exclusive, isObject: false);

            return true;
        }

        public override void Main<TGarnetApi>(TGarnetApi api, ArgSlice input, ref MemoryResult<byte> output)
        {
            _ = api.SET(setA, valueA);
            _ = api.SET(setB, valueB);
            _ = api.SET(setC, valueC);
            _ = api.SET(setD, valueD);
        }
    }
}