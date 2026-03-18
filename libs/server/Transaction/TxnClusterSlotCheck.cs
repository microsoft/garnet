// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    sealed unsafe partial class TransactionManager
    {
        readonly bool clusterEnabled;

        /// <summary>
        /// Keep track of actual key accessed by command
        /// </summary>
        /// <param name="argSlice"></param>
        public void SaveKeyArgSlice(PinnedSpanByte argSlice)
        {
            // Execute method only if clusterEnabled
            if (!clusterEnabled) return;

            var count = clusterKeyParseState.Count;

            // Grow the buffer with doubling if we've run out of capacity
            if (count >= clusterKeyParseState.Capacity)
            {
                var newCapacity = Math.Max(count * 2, initialKeyBufferSize);
                var oldParams = clusterKeyParseState.Parameters;
                clusterKeyParseState.Initialize(newCapacity);
                for (var i = 0; i < count; i++)
                    clusterKeyParseState.SetArgument(i, oldParams[i]);
            }

            // Copy key bytes into dedicated txn scratch buffer (independent of per-message scratch buffer lifecycle)
            var stableKey = txnKeyScratchBuffer.CreateArgSlice(argSlice.ReadOnlySpan);

            clusterKeyParseState.Count = count + 1;
            clusterKeyParseState.SetArgument(count, stableKey);
        }
    }
}