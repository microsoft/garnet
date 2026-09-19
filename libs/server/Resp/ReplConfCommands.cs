// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session partial containing the <c>REPLCONF</c> command handler.
    ///
    /// <para><c>REPLCONF &lt;key&gt; &lt;value&gt; [&lt;key&gt; &lt;value&gt; ...]</c> is used by Redis
    /// primary/replica handshakes and by external orchestrators such as Redis Sentinel.</para>
    ///
    /// <para>Behaviour mirrors Redis 7.4 (verified against a live 7.4.11 server):</para>
    /// <list type="bullet">
    ///   <item><c>REPLCONF</c> with no arguments succeeds and replies <c>+OK</c>.</item>
    ///   <item>An odd number of option arguments is malformed (arity is <c>-1</c>,
    ///         but the implementation only rejects odd counts).</item>
    ///   <item><c>REPLCONF ACK &lt;offset&gt;</c> and <c>REPLCONF GETACK *</c> are
    ///         <em>no-reply</em> forms: Redis deliberately sends nothing back, because
    ///         the reply would otherwise inject unsolicited bytes into the
    ///         replication command stream.</item>
    ///   <item>An unrecognised option is an error:
    ///         <c>-ERR Unrecognized REPLCONF option: &lt;key&gt;</c>.</item>
    /// </list>
    ///
    /// <para>Phase 1 scope: validate and acknowledge. The handler does not yet persist
    /// <c>listening-port</c> / <c>ip-address</c> / <c>capa</c> values, nor does it act
    /// on <c>ack</c> offsets; those are wired in Phase 2 alongside
    /// <c>replica-announce-*</c> config and the replica-side offset-tracking changes.</para>
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Implements <c>REPLCONF &lt;key&gt; &lt;value&gt; ...</c>, matching Redis 7.4 semantics.
        /// </summary>
        private bool NetworkREPLCONF()
        {
            // Identity of the replica on this connection. RemoteEndpointName is
            // "<ip>:<port>" for TCP transports, and empty for in-process senders (e.g.
            // Lua scratch buffers), in which case no replica is registered.
            var (sourcePort, remoteAddress) = GetReplicaIdentity();

            // Redis accepts a bare REPLCONF (arity -1) and only rejects an odd number
            // of option arguments. Two behaviours verified against a live 7.4.11:
            //   REPLCONF            -> +OK
            //   REPLCONF <key>      -> -ERR syntax error
            // Note the second is "syntax error", not an arity error: because arity is
            // -1 the command is dispatched, and the odd pairing is rejected during
            // option parsing.
            if ((parseState.Count & 1) != 0)
            {
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_SYNTAX_ERROR, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // parseState arguments are already ASCII-uppercased by the parser, so we
            // compare against uppercase constants. Hyphenated keys need
            // allowNonAlphabeticChars: true (same as LIB_NAME / LIB_VER in ClientCommands).
            for (var i = 0; i < parseState.Count; i += 2)
            {
                var keySlice = parseState.GetArgSliceByRef(i).ReadOnlySpan;
                var valueSlice = parseState.GetArgSliceByRef(i + 1).ReadOnlySpan;

                // ACK and GETACK are no-reply forms. Redis emits nothing at all for
                // these, and we must do the same: a reply here would be interpreted by
                // the replica as the first bytes of the replication stream.
                if (keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ACK))
                {
                    // Record the replica's acknowledged offset. It is surfaced as the
                    // "offset" field of the slave<N> line in INFO replication.
                    if (sourcePort > 0 && NumUtils.TryParse(valueSlice, out long ackOffset))
                    {
                        var entry = storeWrapper.replicaRegistry.GetOrAdd(sourcePort, remoteAddress);
                        entry.AckOffset = ackOffset;
                        entry.LastInteractionUtc = DateTime.UtcNow;
                    }

                    return true;
                }

                if (keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETACK))
                {
                    // Phase 2 will reply with an offset once the replication stream is
                    // wired; Redis also emits nothing here in the interim.
                    _ = valueSlice;
                    return true;
                }

                // Known, replied-to options. Redis 7.4 recognises: listening-port,
                // ip-address, capa, rdb-only and rdb-filter-only. We record the two that
                // identify the replica (so it can be reported to Sentinel) and accept the
                // rest without persisting them.
                if (keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.LISTENING_PORT, allowNonAlphabeticChars: true))
                {
                    // This is the port Sentinel will connect to in order to promote this
                    // replica, so it must be recorded rather than discarded.
                    if (sourcePort > 0 && NumUtils.TryParse(valueSlice, out int listeningPort))
                    {
                        var entry = storeWrapper.replicaRegistry.GetOrAdd(sourcePort, remoteAddress);
                        entry.ListeningPort = listeningPort;
                        entry.LastInteractionUtc = DateTime.UtcNow;
                    }

                    continue;
                }

                if (keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.IP_ADDRESS, allowNonAlphabeticChars: true))
                {
                    if (sourcePort > 0)
                    {
                        var entry = storeWrapper.replicaRegistry.GetOrAdd(sourcePort, remoteAddress);
                        entry.IpAddress = Encoding.ASCII.GetString(valueSlice);
                        entry.LastInteractionUtc = DateTime.UtcNow;
                    }

                    continue;
                }

                if (keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.CAPA) ||
                    keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.RDB_ONLY, allowNonAlphabeticChars: true))
                {
                    // Register the connection on first contact even when the replica
                    // sends neither listening-port nor ip-address, so the handshake is
                    // still visible in INFO replication.
                    if (sourcePort > 0)
                        storeWrapper.replicaRegistry.GetOrAdd(sourcePort, remoteAddress);

                    continue;
                }

                // Unknown options are an error in Redis, not a silent success:
                //   -ERR Unrecognized REPLCONF option: <key>
                // (verified against 7.4.11). We echo the key as it was sent, not the
                // uppercased form.
                //
                // The key is attacker-controlled, so it is sanitised before being placed
                // in the error line: TryWriteError documents that the message must not
                // contain CR or LF, which would otherwise let a client inject additional
                // protocol lines.
                var keySpan = parseState.GetArgSliceByRef(i).ReadOnlySpan;
                var keyText = Encoding.ASCII.GetString(keySpan).Replace("\r", string.Empty).Replace("\n", string.Empty);
                while (!RespWriteUtils.TryWriteError($"ERR Unrecognized REPLCONF option: {keyText}", ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// Derives this connection's replica identity: the remote source port (used as a
        /// stable key for the connection) and the remote IP address.
        ///
        /// <para>Returns <c>(0, null)</c> when the transport does not expose a TCP endpoint
        /// (for example an in-process sender), in which case the caller skips registration
        /// rather than recording a bogus replica.</para>
        /// </summary>
        private (int SourcePort, string Address) GetReplicaIdentity()
        {
            var endpoint = networkSender?.RemoteEndpointName;
            if (string.IsNullOrEmpty(endpoint))
                return (0, null);

            // Format is "<address>:<port>". IPv6 addresses can themselves contain ':'
            // (and may be bracketed), so split on the LAST colon to isolate the port.
            var lastColon = endpoint.LastIndexOf(':');
            if (lastColon <= 0)
                return (0, null);

            var addressPart = endpoint[..lastColon].Trim('[', ']');
            if (!NumUtils.TryParse(Encoding.ASCII.GetBytes(endpoint[(lastColon + 1)..]), out int port))
                return (0, null);

            return (port, addressPart);
        }
    }
}
