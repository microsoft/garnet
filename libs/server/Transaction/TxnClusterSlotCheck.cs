// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    sealed unsafe partial class TransactionManager
    {
        readonly bool clusterEnabled;

        /// <summary>
        /// Record a key accessed by a queued command, for the cluster slot verification performed at EXEC.
        /// </summary>
        /// <remarks>
        /// The key bytes are copied into <see cref="txnScratchBufferAllocator"/> as they are recorded, rather than
        /// referenced where they sit in the network receive buffer. A queued transaction spans however many network
        /// reads its commands take, and a receive buffer that fills is grown by <c>DoubleNetworkReceiveBuffer</c>,
        /// which copies the bytes into a larger buffer and then <b>disposes</b> the old one back to its pool — so a
        /// retained pointer is left reading recycled memory. <c>ShrinkNetworkReceiveBuffer</c> disposes the old buffer
        /// the same way, and <c>ShiftNetworkReceiveBuffer</c> compacts in place, moving the bytes without the buffer's
        /// address changing at all. Copying while the bytes are still live is independent of all three.
        /// </remarks>
        /// <param name="keySlice"></param>
        public void SaveKeyArgSlice(PinnedSpanByte keySlice)
        {
            // Execute method only if clusterEnabled
            if (!clusterEnabled) return;

            var count = txnKeysParseState.Count;

            // Grow the buffer if needed (EnsureCapacity handles safe resize with proper GC rooting)
            txnKeysParseState.EnsureCapacity(count + 1);

            txnKeysParseState.Count = count + 1;
            txnKeysParseState.SetArgument(count, txnScratchBufferAllocator.CreateArgSlice(keySlice.ReadOnlySpan));
        }
    }
}