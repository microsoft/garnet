// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Resp.benchmark
{
    /// <summary>
    /// A pre-generated RESP request batch ready to send over the wire.
    /// </summary>
    public struct Request
    {
        /// <summary>
        /// The serialized RESP protocol bytes for this batch.
        /// </summary>
        public byte[] RespData;

        /// <summary>
        /// Number of valid bytes in RespData.
        /// </summary>
        public int ByteCount;

        /// <summary>
        /// Number of RESP commands in this batch (i.e., expected response count).
        /// For GET/SET: equals the batch size (one response per command).
        /// For MGET/MSET: equals 1 (single multi-key command, single response).
        /// </summary>
        public int CommandCount;
    }
}
