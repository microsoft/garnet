// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.common
{
    /// <summary>
    /// Serializer interface for keys, needed for pub-sub
    /// </summary>
    public unsafe interface IKeySerializer
    {
        /// <summary>
        /// Read key from given location pointer and increment that pointer past the key
        /// </summary>
        /// <param name="src">Memory location</param>
        /// <returns>Key</returns>
        SpanByte ReadKey(ref byte* src);

        /// <summary>
        /// Match pattern with key used for pub-sub
        /// </summary>
        /// <param name="key">key to be published</param>
        /// <param name="asciiKey">whether key is ascii</param>
        /// <param name="pattern">pattern to check</param>
        /// <param name="asciiPattern">whether pattern is ascii</param>
        bool Match(SpanByte key, bool asciiKey, SpanByte pattern, bool asciiPattern);
    }
}