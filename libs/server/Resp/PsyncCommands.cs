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
    /// <para>The command parses the two arguments, replies
    /// <c>+FULLRESYNC &lt;replid&gt; 0</c>, and ship a valid empty-database RDB body
    /// (56 bytes: 48-byte body + 8-byte CRC64 little-endian). When standalone
    /// replication and AOF are enabled, new Garnet AOF records are streamed after
    /// the RDB body.</para>
    ///
    /// <para>What this does <em>not</em> yet do:</para>
    /// <list type="bullet">
    ///   <item><c>+CONTINUE</c> for partial resync (requires a replication backlog,
    ///         wired in Phase 2).</item>
    ///   <item>Streaming the initial database state — the RDB remains empty, so only
    ///         writes made after attachment are replicated.</item>
    /// </list>
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        StandaloneSyncDriver standaloneSyncDriver;
        bool supportsGarnetSnapshot;

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
        /// followed by a <c>$N\r\n&lt;RDB&gt;</c> frame containing the empty-DB RDB body.
        /// Note that this frame is <em>not</em> a normal RESP bulk string: the transfer is
        /// length-delimited and carries no trailing CRLF after the RDB bytes.</para>
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

            if (storeWrapper.serverOptions.EnableStandaloneReplication)
            {
                if (storeWrapper.appendOnlyFile == null)
                {
                    while (!RespWriteUtils.TryWriteError("ERR standalone replication requires AOF"u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                if (storeWrapper.serverOptions.MultiLogEnabled)
                {
                    while (!RespWriteUtils.TryWriteError("ERR standalone replication does not support multi-log AOF"u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }
            }

            // Source the primary's replid from the store so it matches what INFO
            // replication advertises. A replica records the replid it is given and
            // presents it on subsequent PSYNC attempts; if the two surfaces disagreed,
            // every reconnect would look like a brand-new primary. Phase 2 will thread
            // this through the cluster-managed PrimaryReplId as well.
            var primaryReplId = storeWrapper.GetOrCreatePrimaryReplId();
            long? syncStartAddress = null;
            if (supportsGarnetSnapshot &&
                storeWrapper.serverOptions.EnableStandaloneReplication &&
                storeWrapper.appendOnlyFile != null &&
                !storeWrapper.serverOptions.MultiLogEnabled)
            {
                // Capture the boundary before emitting FULLRESYNC so writes concurrent
                // with the handshake cannot fall between the empty snapshot and stream.
                syncStartAddress = storeWrapper.appendOnlyFile.Log.GetTailAddress(0);
            }

            // Record this connection as an attached replica, and mark its handshake as
            // complete so INFO replication reports it as "online". Sentinel discovers a
            // primary's replicas solely from the slave<N> lines of INFO replication, so
            // without this a replica would complete a sync but remain invisible to any
            // orchestrator. RemoteEndpointName is "<ip>:<port>" on TCP transports and
            // empty for in-process senders, in which case no replica is registered.
            {
                var endpoint = networkSender?.RemoteEndpointName;
                var lastColon = endpoint?.LastIndexOf(':') ?? -1;
                if (lastColon > 0 && NumUtils.TryParse(System.Text.Encoding.ASCII.GetBytes(endpoint[(lastColon + 1)..]), out int sourcePort))
                {
                    var addressPart = endpoint[..lastColon].Trim('[', ']');
                    var entry = storeWrapper.replicaRegistry.GetOrAdd(sourcePort, addressPart);
                    entry.SyncCompleted = true;
                }
            }

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

            // Write $<len>\r\n<RDB> — with NO trailing CRLF.
            //
            // The replication transfer is length-delimited: Redis sends
            // "$<len>\r\n" followed by exactly <len> bytes of RDB and nothing else.
            // We therefore cannot use RespWriteUtils.TryWriteBulkString, which
            // appends a trailing CRLF as part of the normal bulk-string encoding and
            // would leave two stray bytes at the head of the replication command
            // stream. Verified against a live 7.4.11 primary (172-byte RDB, zero
            // bytes following the body).
            while (!RespWriteUtils.TryWriteBulkStringLength(rdbWithCrc.Length, ref dcurr, dend))
                SendAndReset();
            while (!RespWriteUtils.TryWriteDirect(rdbWithCrc, ref dcurr, dend))
                SendAndReset();

            standaloneSyncDriver?.Dispose();
            standaloneSyncDriver = null;
            if (syncStartAddress.HasValue)
            {
                standaloneSyncDriver = new StandaloneSyncDriver(storeWrapper, networkSender, syncStartAddress.Value, logger);
            }

            return true;
        }
    }
}