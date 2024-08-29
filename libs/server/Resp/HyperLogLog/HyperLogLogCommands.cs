// Copyright (c) Microsoft Corporation.
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
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PFADD));
            }

            var countBytes = stackalloc byte[1];
            countBytes[0] = (byte)'1';
            var countSlice = new ArgSlice(countBytes, 1);

            ArgSlice[] currParseStateBuffer = default;
            var currParseState = new SessionParseState();
            currParseState.Initialize(ref currParseStateBuffer, 2);
            currParseStateBuffer[0] = countSlice;

            var input = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.PFADD },
                parseState = currParseState,
                parseStateStartIdx = 0
            };

            var output = stackalloc byte[1];
            byte pfaddUpdated = 0;
            var key = parseState.GetArgSliceByRef(0).SpanByte;

            for (var i = 1; i < parseState.Count; i++)
            {
                var currElementSlice = parseState.GetArgSliceByRef(i);
                currParseStateBuffer[1] = currElementSlice;

                var o = new SpanByteAndMemory(output, 1);
                storageApi.HyperLogLogAdd(ref key, ref input, ref o);

                //Invalid HLL Type
                if (*output == 0xFF)
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
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PFCOUNT));
            }

            var inputHeader = new RawStringInput
            {
                header = new RespInputHeader { cmd = RespCommand.PFCOUNT },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var status = storageApi.HyperLogLogLength(ref inputHeader, out long cardinality, out bool error);
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
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PFMERGE));
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