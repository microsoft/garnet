// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

//#define HLL_SINGLE_PFADD_ENABLED

using Garnet.common;

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

            var input = new StringInput(RespCommand.PFADD, ref metaCommandInfo, ref parseState);

            var output = stackalloc byte[1];
            byte pfaddUpdated = 0;
            var key = parseState.GetArgSliceByRef(0);

            for (var i = 1; i < parseState.Count; i++)
            {
                input.parseState = parseState.Slice(i, 1);
                var o = StringOutput.FromPinnedPointer(output, 1);
                storageApi.HyperLogLogAdd(key, ref input, ref o);

                // Invalid HLL Type
                if (*output == 0xFF)
                {
                    while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE_HLL, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                pfaddUpdated |= *output;
            }

            if (pfaddUpdated > 0)
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_1, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_RETURN_VAL_0, ref dcurr, dend))
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

            var input = new StringInput(RespCommand.PFCOUNT, ref metaCommandInfo, ref parseState);

            storageApi.HyperLogLogLength(ref input, out var cardinality, out var error);
            if (error)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE_HLL, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteInt64(cardinality, ref dcurr, dend))
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

            var input = new StringInput(RespCommand.PFMERGE, ref metaCommandInfo, ref parseState);

            var status = storageApi.HyperLogLogMerge(ref input, out var error);

            // Invalid Type
            if (error)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE_HLL, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            if (status == GarnetStatus.OK)
            {
                while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }
    }
}