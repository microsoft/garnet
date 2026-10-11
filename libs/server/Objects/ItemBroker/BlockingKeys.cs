// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Keys handed to <see cref="CollectionItemBroker"/> by a blocking collection command.
    /// </summary>
    /// <remarks>
    /// The broker resolves keys on its own thread, after the calling session has suspended and its stack has
    /// unwound, so a key may not point into the session's network receive buffer or its scratch buffers. Keys are
    /// copied onto the pinned object heap instead, which lets the broker view a key as a
    /// <see cref="PinnedSpanByte"/> directly rather than copying it into the session's scratch buffer on every
    /// attempt.
    /// </remarks>
    internal static class BlockingKeys
    {
        /// <summary>
        /// Copies a key onto the pinned object heap.
        /// </summary>
        /// <param name="key">Key to copy.</param>
        /// <returns>The copied key.</returns>
        internal static byte[] Copy(ReadOnlySpan<byte> key)
        {
            var copy = GC.AllocateArray<byte>(key.Length, pinned: true);
            key.CopyTo(copy);
            return copy;
        }

        /// <summary>
        /// Views a key returned by <see cref="Copy"/> as a <see cref="PinnedSpanByte"/>.
        /// </summary>
        /// <param name="key">Key to view.</param>
        /// <returns>A view over the key.</returns>
        internal static PinnedSpanByte AsSpanByte(byte[] key) => PinnedSpanByte.FromPinnedSpan(key);
    }
}