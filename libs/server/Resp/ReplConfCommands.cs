// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
                if (keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ACK) ||
                    keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETACK))
                {
                    // Phase 2 will route the ACK offset to the replication manager so it
                    // can track replica progress; the value is intentionally unused here.
                    _ = valueSlice;
                    return true;
                }

                // Known, replied-to options. Phase 1 validates arity only and discards
                // the value; Phase 2 will persist listening-port / ip-address / capa.
                //
                // Redis 7.4 recognises: listening-port, ip-address, capa, rdb-only and
                // rdb-filter-only. We accept the first four; rdb-filter-only has its own
                // value validation in Redis and is deliberately left to Phase 2, so it
                // currently falls through to the Unrecognized error below.
                if (keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.LISTENING_PORT, allowNonAlphabeticChars: true) ||
                    keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.IP_ADDRESS, allowNonAlphabeticChars: true) ||
                    keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.CAPA) ||
                    keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.RDB_ONLY, allowNonAlphabeticChars: true))
                {
                    _ = valueSlice;
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
    }
}
