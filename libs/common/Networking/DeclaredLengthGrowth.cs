// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;

namespace Garnet.networking
{
    /// <summary>
    /// How large a receive buffer grows, and is kept, for a message whose declared length runs past the bytes received so far. A declared
    /// length costs nothing to send, so the buffer stays within <see cref="FastGrowthFactor"/> of the bytes the client has actually sent.
    /// </summary>
    internal static class DeclaredLengthGrowth
    {
        /// <summary>
        /// Largest factor by which a full receive buffer grows at once toward the size a message declares.
        /// </summary>
        internal const long FastGrowthFactor = 8;

        /// <summary>
        /// Size up to which a receive buffer grows by <see cref="FastGrowthFactor"/> toward the size a message declares; beyond it, the buffer
        /// doubles as the bytes arrive.
        /// </summary>
        internal const long FastGrowthLimit = 128L << 20;

        /// <summary>
        /// Largest size a full receive buffer of <paramref name="size"/> bytes grows to at once toward the size a message declares.
        /// </summary>
        internal static long Limit(long size)
            => size < FastGrowthLimit ? Math.Min(FastGrowthFactor * size, FastGrowthLimit) : 2 * size;

        /// <summary>
        /// Size below which a receive buffer holding the start of a message is not shrunk: the power of two that holds the message, but no more
        /// than growth would have reached from the largest power-of-two buffer the <paramref name="buffered"/> bytes fill. A buffer grown
        /// toward the message from full keeps its size, while the start of a message that has barely arrived cannot keep a large buffer.
        /// </summary>
        /// <param name="required">Bytes the message needs, at least one.</param>
        /// <param name="buffered">Bytes of it buffered, at least one.</param>
        internal static long PendingMessageFloor(long required, int buffered)
            => Math.Min((long)BitOperations.RoundUpToPowerOf2((ulong)required), Limit(1L << BitOperations.Log2((uint)buffered)));
    }
}