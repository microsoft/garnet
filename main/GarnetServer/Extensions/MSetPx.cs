// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Garnet.server;
using Tsavorite.core;

namespace Garnet
{
    /// <summary>
    /// Functions to implement custom transaction MSETPX - set multiple keys with given expiration in milliseconds
    /// 
    /// Format: MSETPX 60000 key1 value1 key2 value2 ...
    /// 
    /// Description: Perform a non-transactional multi-set with expiry for the given set of key-value pairs
    /// </summary>
    sealed class MSetPxTxn : CustomTransactionProcedure
    {
        /// <summary>
        /// No transactional phase, skip Prepare
        /// </summary>
        public override bool Prepare<TGarnetReadApi>(TGarnetReadApi api, ref CustomProcedureInput procInput)
            => false;

        /// <summary>
        /// Main will not be called because Prepare returns false
        /// </summary>
        public override void Main<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
            => throw new InvalidOperationException();

        /// <summary>
        /// Perform the MSETPX operation
        /// </summary>
        public override void Finalize<TGarnetApi>(TGarnetApi api, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            int offset = 0;

            // Read expiry
            var expiryMs = GetNextArg(ref procInput, ref offset);

            // Read and set key-value pairs with expiry
            PinnedSpanByte key, value;
            while ((key = GetNextArg(ref procInput, ref offset)).Length > 0)
            {
                value = GetNextArg(ref procInput, ref offset);
                api.SETEX(key, value, expiryMs);
            }
            WriteSimpleString(ref output, "OK");
        }
    }
}