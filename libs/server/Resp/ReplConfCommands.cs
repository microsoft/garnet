// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session partial containing the <c>REPLCONF</c> command handler.
    ///
    /// <para><c>REPLCONF &lt;key&gt; &lt;value&gt; [&lt;key&gt; &lt;value&gt; ...]</c> is used by Redis
    /// primary/replica handshakes and by external orchestrators such as Redis Sentinel.
    /// It accepts an arbitrary number of key/value pairs and replies <c>+OK</c> to any
    /// of them — including keys we don't recognise — for forward compatibility with
    /// future Redis versions and Sentinel-specific extensions.</para>
    ///
    /// <para>Phase 1 scope: validate arity, accept and acknowledge all key/value pairs
    /// (known or unknown). The handler does not yet persist <c>listening-port</c> /
    /// <c>ip-address</c> / <c>capa</c> values, nor does it act on <c>ack</c> offsets;
    /// those are wired in Phase 2 alongside <c>replica-announce-*</c> config and the
    /// replica-side offset-tracking changes.</para>
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Implements <c>REPLCONF &lt;key&gt; &lt;value&gt; ...</c>.
        ///
        /// <para>The argument list must be a non-empty sequence of key/value pairs
        /// (even length, ≥ 2). Keys are matched case-insensitively against the
        /// small known set (<c>listening-port</c>, <c>ip-address</c>, <c>capa</c>,
        /// <c>ack</c>, <c>getack</c>, <c>no-one-connects</c>). Unknown keys are
        /// not errors — we always reply <c>+OK</c>.</para>
        /// </summary>
        private bool NetworkREPLCONF()
        {
            // REPLCONF requires at least one key/value pair and any number of additional
            // pairs. An odd number of arguments (other than zero) is malformed.
            if (parseState.Count == 0 || (parseState.Count & 1) != 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.REPLCONF));
            }

            // Walk the pairs. We deliberately do not surface a rejection for unknown
            // keys (matches Redis / Sentinel forward-compatibility behaviour).
            //
            // parseState arguments are already ASCII-uppercased by the parser, so we
            // compare against uppercase constants. Hyphenated keys need
            // allowNonAlphabeticChars: true (same as LIB_NAME / LIB_VER in ClientCommands).
            for (var i = 0; i < parseState.Count; i += 2)
            {
                var keySlice = parseState.GetArgSliceByRef(i).ReadOnlySpan;
                var valueSlice = parseState.GetArgSliceByRef(i + 1).ReadOnlySpan;

                if (keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.LISTENING_PORT, allowNonAlphabeticChars: true) ||
                    keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.IP_ADDRESS, allowNonAlphabeticChars: true) ||
                    keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.CAPA) ||
                    keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.NO_ONE_CONNECTS, allowNonAlphabeticChars: true) ||
                    keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.ACK) ||
                    keySlice.EqualsUpperCaseSpanIgnoringCase(CmdStrings.GETACK))
                {
                    // Known key. Phase 1: value is intentionally discarded. Phase 2
                    // will persist listening-port / ip-address / capa and will route
                    // ack offsets to the replication manager.
                    _ = valueSlice;
                }
                // else: unknown key — still +OK, no error.
            }

            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }
    }
}
