// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - ETag associated commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// GETETAG key 
        /// Given a key get the ETag
        /// </summary>
        private bool NetworkGETETAG<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(parseState.Count == 1);

            var key = parseState.GetArgSliceByRef(0);

            // Prepare input
            var input = new UnifiedStoreInput(RespCommand.GETETAG);

            // Prepare GarnetUnifiedStoreOutput output
            var output = GarnetUnifiedStoreOutput.FromPinnedPointer(dcurr, (int)(dend - dcurr));

            var status = storageApi.GETETAG(key, ref input, ref output);

            if (status == GarnetStatus.OK)
            {
                ProcessOutput(output.SpanByteAndMemory);
            }
            else
            {
                WriteNull();
            }

            return true;
        }

        /// <summary>
        /// DELIFGREATER key etag
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkDELIFGREATER<TGarnetApi>(ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (parseState.Count != 2)
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.DELIFGREATER));

            var key = parseState.GetArgSliceByRef(0);
            if (!parseState.TryGetLong(1, out long givenEtag) || givenEtag < 0)
            {
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_INVALID_ETAG);
            }

            // Conditional delete is not natively supported for records in the stable region.
            // To achieve this, we use a conditional DEL command to gain RMW (Read-Modify-Write) access, enabling deletion based on conditions.

            RawStringInput input = new RawStringInput(RespCommand.DELIFGREATER, ref parseState, startIdx: 1, metaCommand, ref metaCommandParseState);
            input.header.metaCmd = RespMetaCommand.ExecWithEtag;

            GarnetStatus status = storageApi.DEL_Conditional(key, ref input);

            int keysDeleted = status == GarnetStatus.OK ? 1 : 0;

            while (!RespWriteUtils.TryWriteInt32(keysDeleted, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}