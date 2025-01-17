// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;

namespace NoOpModule
{
    /// <summary>
    /// Represents a no-op transaction
    /// </summary>
    public class NoOpTxn : CustomTransactionProcedure
    {
        /// <inheritdoc />
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
        {
            return true;
        }

        /// <inheritdoc />
        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
        }
    }
}