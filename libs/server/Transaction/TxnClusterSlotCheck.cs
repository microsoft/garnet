// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    sealed unsafe partial class TransactionManager
    {
        readonly bool clusterEnabled;
        internal byte* saveKeyRecvBufferPtr;
        int firstKeyInCurrentRecvBuffer;

        public void BeginKeyTrackingForCurrentBuffer(byte* recvBufferPtr)
        {
            if (!clusterEnabled) return;

            saveKeyRecvBufferPtr = recvBufferPtr;
            firstKeyInCurrentRecvBuffer = txnKeysParseState.Count;
        }

        public void OnRecvBufferChanged(byte* recvBufferPtr)
        {
            if (!clusterEnabled || recvBufferPtr == saveKeyRecvBufferPtr)
                return;

            Debug.Assert(firstKeyInCurrentRecvBuffer <= txnKeysParseState.Count);

            CopyKeysToScratchBuffer(firstKeyInCurrentRecvBuffer);
            firstKeyInCurrentRecvBuffer = txnKeysParseState.Count;
            saveKeyRecvBufferPtr = recvBufferPtr;
        }

        /// <summary>
        /// Keep track of actual key accessed by command
        /// </summary>
        /// <param name="keySlice"></param>
        public void SaveKeyArgSlice(PinnedSpanByte keySlice)
        {
            // Execute method only if clusterEnabled
            if (!clusterEnabled) return;

            var count = txnKeysParseState.Count;

            // Grow the buffer if needed (EnsureCapacity handles safe resize with proper GC rooting)
            txnKeysParseState.EnsureCapacity(count + 1);

            txnKeysParseState.Count = count + 1;
            txnKeysParseState.SetArgument(count, keySlice);
        }

        /// <summary>
        /// Copy all existing keys into <see cref="txnScratchBufferAllocator"/> so they are independent of the old receive buffer.
        /// Called when the receive buffer has been reallocated since keys were last stored.
        /// </summary>
        public void CopyExistingKeysToScratchBuffer()
        {
            Debug.Assert(clusterEnabled);

            CopyKeysToScratchBuffer(0);
            firstKeyInCurrentRecvBuffer = txnKeysParseState.Count;
        }

        void CopyKeysToScratchBuffer(int startIndex)
        {
            for (var i = startIndex; i < txnKeysParseState.Count; i++)
            {
                ref var key = ref txnKeysParseState.GetArgSliceByRef(i);
                key = txnScratchBufferAllocator.CreateArgSlice(key.ReadOnlySpan);
            }
        }
    }
}