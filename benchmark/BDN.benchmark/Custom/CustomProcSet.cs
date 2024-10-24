﻿// Copyright (c) Microsoft Corporation.
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

        public override bool Execute<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var offset = 0;
            var setA = GetNextArg(ref procInput, ref offset);
            var setB = GetNextArg(ref procInput, ref offset);
            var setC = GetNextArg(ref procInput, ref offset);
            var setD = GetNextArg(ref procInput, ref offset);

            var valueA = GetNextArg(ref procInput, ref offset);
            var valueB = GetNextArg(ref procInput, ref offset);
            var valueC = GetNextArg(ref procInput, ref offset);
            var valueD = GetNextArg(ref procInput, ref offset);

            _ = api.SET(setA, valueA);
            _ = api.SET(setB, valueB);
            _ = api.SET(setC, valueC);
            _ = api.SET(setD, valueD);

            return true;
        }
    }
}