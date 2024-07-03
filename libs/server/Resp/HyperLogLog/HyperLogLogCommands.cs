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
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool HyperLogLogAdd<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ArgSlice[] argSlices = new ArgSlice[count];

            //Read pfadd dstKey and input values
            for (int i = 0; i < argSlices.Length; i++)
            {
                argSlices[i] = new();
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref argSlices[i].ptr, ref argSlices[i].length, ref ptr, recvBufferPtr + bytesRead))
                    return false;
            }

            readHead = (int)(ptr - recvBufferPtr);
            if (NetworkMultiKeySlotVerify(readOnly: false, firstKey: 0, lastKey: 0))
                return true;

            //4 byte length of input
            //1 byte RespCommand
            //1 byte RespInputFlags
            //4 byte count of value to insert
            //8 byte hash value
            int inputSize = sizeof(int) + RespInputHeader.Size + sizeof(int) + sizeof(long);
            byte* pbCmdInput = stackalloc byte[inputSize];

            ///////////////
            //Build Input//
            ///////////////
            byte* pcurr = pbCmdInput;
            *(int*)pcurr = inputSize - sizeof(int);
            pcurr += sizeof(int);
            //1. header
            (*(RespInputHeader*)(pcurr)).cmd = RespCommand.PFADD;
            (*(RespInputHeader*)(pcurr)).flags = 0;
            pcurr += RespInputHeader.Size;
            //2. cmd args
            *(int*)pcurr = 1; pcurr += sizeof(int);
            byte* output = stackalloc byte[1];

            byte pfaddUpdated = 0;
            SpanByte key = argSlices[0].SpanByte;
            for (int i = 1; i < argSlices.Length; i++)
            {
                *(long*)pcurr = (long)HashUtils.MurmurHash2x64A(argSlices[i].ptr, argSlices[i].Length);

                var o = new SpanByteAndMemory(output, 1);
                var status = storageApi.HyperLogLogAdd(ref key, ref Unsafe.AsRef<SpanByte>(pbCmdInput), ref o);

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
        /// <param name="count"></param>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        /// <exception cref="GarnetException"></exception>
        private bool HyperLogLogLength<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (NetworkMultiKeySlotVerify(readOnly: true))
                return true;

            // 4 byte length of input
            // 1 byte RespCommand
            // 1 byte RespInputFlags
            int inputSize = sizeof(int) + RespInputHeader.Size;
            byte* pbCmdInput = stackalloc byte[inputSize];

            /////////////////
            ////Build Input//
            /////////////////
            byte* pcurr = pbCmdInput;
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
        private bool HyperLogLogMerge<TGarnetApi>(int count, byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            if (NetworkMultiKeySlotVerify(readOnly: false))
                return true;

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