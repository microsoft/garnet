// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

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
            var input = new UnifiedInput(RespCommand.GETETAG, ref metaCommandInfo, ref parseState);

            var output = GetUnifiedOutput();

            var status = storageApi.GETETAG(key, ref input, ref output);
            etag = output.ETag;

            if (status != GarnetStatus.OK || output.IsOperationSkipped)
                WriteNull();
            else
                ProcessOutput(output.SpanByteAndMemory);

            return true;
        }
    }
}