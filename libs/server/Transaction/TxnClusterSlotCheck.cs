// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;

namespace Garnet.server
{
    sealed unsafe partial class TransactionManager
    {
        readonly bool clusterEnabled;
        internal byte* saveKeyRecvBufferPtr;

        /// <summary>
        /// Copy all existing keys into <see cref="txnScratchBuffer"/> so they are independent of the old receive buffer.
        /// Called when the receive buffer has been reallocated since keys were last stored.
        /// </summary>
        public void CopyExistingKeysToScratchBuffer()
        {
            for (var i = 0; i < clusterKeyParseState.Count; i++)
            {
                ref var key = ref clusterKeyParseState.GetArgSliceByRef(i);
                key = txnScratchBuffer.CreateArgSlice(key.ReadOnlySpan);
            }
        }

        /// <summary>
        /// Keep track of actual key accessed by command
        /// </summary>
        /// <param name="keySlice"></param>
        public void SaveKeyArgSlice(PinnedSpanByte keySlice)
        {
            // Execute method only if clusterEnabled
            if (!clusterEnabled) return;

            var count = clusterKeyParseState.Count;

            // Double the parse state buffer capacity if needed, and copy existing parameters to the extended buffer
            if (count >= clusterKeyParseState.Capacity)
            {
                var oldParams = clusterKeyParseState.Parameters;
                clusterKeyParseState.Initialize(count * 2);
                clusterKeyParseState.SetArguments(0, oldParams);
            }

            clusterKeyParseState.Count = count + 1;
            clusterKeyParseState.SetArgument(count, keySlice);
        }
    }
}