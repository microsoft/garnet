﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//#define HLL_SINGLE_PFADD_ENABLED

using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Adds one element to the HyperLogLog data structure stored at the variable name specified.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool HyperLogLogAdd<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PFADD), parseState.count);
            }

            // 4 byte length of input
            // 1 byte RespCommand
            // 1 byte RespInputFlags
            // 4 byte count of value to insert
            // 8 byte hash value
            var inputSize = sizeof(int) + RespInputHeader.Size + sizeof(int) + sizeof(long);
            var pbCmdInput = stackalloc byte[inputSize];

            ///////////////
            //Build Input//
            ///////////////
            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            //1. header
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.PFADD;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;
            //2. cmd args
            *(int*)pcurr = 1; pcurr += sizeof(int);
            var output = stackalloc byte[1];

            byte pfaddUpdated = 0;
            var key = parseState.GetArgSliceByRef(0).SpanByte;
            for (var i = 1; i < parseState.count; i++)
            {
                var currSlice = parseState.GetArgSliceByRef(i);
                *(long*)pcurr = (long)HashUtils.MurmurHash2x64A(currSlice.ptr, currSlice.Length);

                var o = new SpanByteAndMemory(output, 1);
                storageApi.HyperLogLogAdd(ref key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

                //Invalid HLL Type
                if (*output == (byte)0xFF)
                {
                    while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE_HLL, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                pfaddUpdated |= *output;
            }

            if (pfaddUpdated > 0)
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_1, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        /// <summary>
        /// Returns the approximated cardinality computed by the HyperLogLog data structure stored at the specified key,
        /// or 0 if the key does not exist.
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        private bool HyperLogLogLength<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PFCOUNT), parseState.count);
            }

            // 4 byte length of input
            // 1 byte RespCommand
            // 1 byte RespInputFlags
            var inputSize = sizeof(int) + RespInputHeader.Size;
            var pbCmdInput = stackalloc byte[inputSize];

            /////////////////
            ////Build Input//
            /////////////////
            var pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            (*(RespInputHeader*)pcurr).cmd = RespCommand.PFCOUNT;
            (*(RespInputHeader*)pcurr).flags = 0;

            var status = storageApi.HyperLogLogLength(parseState.Parameters, ref Unsafe.AsRef<SpanByte>(pbCmdInput), out long cardinality, out bool error);
            if (error)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE_HLL, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.WriteInteger(cardinality, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Merge multiple HyperLogLog values into an unique value that will approximate the cardinality 
        /// of the union of the observed Sets of the source HyperLogLog structures.
        /// </summary>
        private bool HyperLogLogMerge<TGarnetApi>(ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (parseState.count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PFMERGE), parseState.count);
            }

            var status = storageApi.HyperLogLogMerge(parseState.Parameters, out bool error);
            // Invalid Type
            if (error)
            {
                while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE_HLL, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            else if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }
    }
}