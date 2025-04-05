// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace NoOpModule
{
    /// <summary>
    /// Represents a no-op procedure
    /// </summary>
    public class NoOpProc : CustomProcedure
    {
        /// <inheritdoc />
        public override bool Execute<TGarnetApi>(TGarnetApi garnetApi, ref CustomProcedureInput procInput,
            ref MemoryResult<byte> output)
        {
            return true;
        }
    }
}