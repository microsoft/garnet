// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session partial containing the <c>PSYNC</c> command handler.
    ///
    /// <para><c>PSYNC &lt;replid&gt; &lt;offset&gt;</c> is the standard Redis primary/replica
    /// handshake command. It is spoken by stock Redis replicas (and, transitively,
    /// by anything that drives Redis-style replication, including Sentinel) when
    /// they first attach to a primary or reconnect after a transient drop.</para>
    ///
    /// <para>Phase 1 scope: parse the two arguments, always reply
    /// <c>+FULLRESYNC &lt;replid&gt; 0</c>, and ship a valid empty-database RDB body
    /// (56 bytes: 48-byte body + 8-byte CRC64 little-endian) so a stock replica
    /// can complete its handshake against a Garnet primary with an empty database.</para>
    ///
    /// <para>What Phase 1 does <em>not</em> yet do:</para>
    /// <list type="bullet">
    ///   <item><c>+CONTINUE</c> for partial resync (requires a replication backlog,
    ///         wired in Phase 2).</item>
    ///   <item>Streaming the actual database state — we reply with an empty RDB. A
    ///         stock replica will end up with an empty database, which is the same
    ///         end-state as a fresh primary that has never received a write.</item>
    /// </list>
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Implements <c>PSYNC &lt;replid&gt; &lt;offset&gt;</c>.
        ///
        /// <para>The two arguments are:</para>
        /// <list type="bullet">
        ///   <item><c>replid</c>: the replica's known primary replication
        ///         ID, or <c>?</c> when the replica has no prior history.</item>
        ///   <item><c>offset</c>: the replica's known replication offset,
        ///         or <c>-1</c> for "no history" (which forces a full resync).</item>
        /// </list>
        ///
        /// <para>The response is always <c>+FULLRESYNC &lt;primaryReplId&gt; 0</c>
        /// followed by a $N\r\n&lt;RDB&gt;\r\n frame containing the empty-DB RDB body.</para>
        /// </summary>
        private bool NetworkPSYNC()
        {
            // PSYNC requires exactly two arguments: <replid> <offset>.
            // We don't actually parse their values in Phase 1 (we always reply
            // FULLRESYNC), but we enforce the arity so a misbehaving client gets
            // a clean error instead of an inconsistent reply.
            if (parseState.Count != 2)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PSYNC));
            }

            // Source the primary's replid. Phase 1 always mints a fresh 40-char
            // hex per PSYNC call (matching what ClusterProvider.cs:225-226 emits
            // for INFO replication in non-cluster mode). Phase 2 will thread this
            // through the cluster-managed PrimaryReplId so replicas see the same
            // value across restarts of a cluster node. Avoiding the cluster
            // interface here keeps Phase 1 decoupled from IClusterProvider and
            // means PSYNC works correctly in non-cluster mode without any
            // conditional wiring.
            var primaryReplId = Generator.DefaultHexId();

            // Build the RDB body once per call (it's tiny — 56 bytes). Doing it on
            // each PSYNC keeps the code obvious and lets future phases replace the
            // body with a real snapshot stream without changing this call site.
            var rdbBody = CmdStrings.EmptyRdbBody;
            var crc = Crc64.Hash(rdbBody);
            var rdbWithCrc = new byte[rdbBody.Length + crc.Length];
            Buffer.BlockCopy(rdbBody, 0, rdbWithCrc, 0, rdbBody.Length);
            Buffer.BlockCopy(crc, 0, rdbWithCrc, rdbBody.Length, crc.Length);

            // Write +FULLRESYNC <replid> 0\r\n
            // Phase 1 always reports offset 0 since the empty-DB RDB has no records
            // to ack. A stock replica is happy with this; it just transitions to
            // "connected" and starts sending REPLCONF ACK 0 periodically.
            while (!RespWriteUtils.TryWriteDirect(
                "+"u8, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.FULLRESYNC, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteDirect(" "u8, ref dcurr, dend))
                SendAndReset();
            var replIdBytes = System.Text.Encoding.ASCII.GetBytes(primaryReplId);
            while (!RespWriteUtils.TryWriteDirect(replIdBytes, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteDirect(" 0\r\n"u8, ref dcurr, dend))
                SendAndReset();

            // Write $<len>\r\n<RDB>\r\n
            while (!RespWriteUtils.TryWriteBulkString(rdbWithCrc, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}
