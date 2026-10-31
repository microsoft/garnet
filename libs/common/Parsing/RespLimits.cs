// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.common.Parsing
{
    /// <summary>
    /// Resource limits enforced when parsing RESP messages and structured values.
    /// </summary>
    public static class RespLimits
    {
        /// <summary>
        /// Maximum size a single RESP element can be.
        ///
        /// This matches the default Redis max string size of 512MB.
        /// </summary>
        public const int MaxArgumentLengthBytes = 512 * 1_024 * 1_024;

        /// <summary>
        /// Maximum nesting depth of a structured value: a nested RESP array reply, or a Lua table converted
        /// to or from JSON.
        ///
        /// This matches the Redis maximum JSON nesting depth of 1000.
        /// </summary>
        public const int MaxNestingDepth = 1000;

        /// <summary>
        /// Largest size a connection's receive buffer grows to; a message that does not fit in it is a protocol
        /// error that closes the connection.
        ///
        /// This matches the default Redis client-query-buffer-limit of 1GB.
        /// </summary>
        public const int MaxReceiveBufferBytes = 1 << 30;

        /// <summary>
        /// Smallest encoding of an array element ("+\r\n" or ":\r\n").
        /// </summary>
        public const int MinArrayElementBytes = 3;

        /// <summary>
        /// Largest element count an array can declare: the most elements a full receive buffer can hold.
        /// </summary>
        public const int MaxArrayLength = MaxReceiveBufferBytes / MinArrayElementBytes;

        /// <summary>
        /// Maximum number of arguments (excluding the command name) allowed in a single RESP command.
        /// Prevents pre-auth memory exhaustion from oversized RESP array headers.
        /// </summary>
        public const int MaxRequestArrayLength = 1 << 20;
    }
}