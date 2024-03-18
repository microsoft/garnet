// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    sealed unsafe partial class TransactionManager
    {
        //Keys involved in the current transaction
        ArgSlice[] keys;
        int keyCount;

        internal byte* saveKeyRecvBufferPtr;
        readonly bool clusterEnabled;

        /// <summary>
        /// Keep track of actual key accessed by command
        /// </summary>
        /// <param name="argSlice"></param>
        public void SaveKeyArgSlice(ArgSlice argSlice)
        {
            //Execute method only if clusterEnabled
            if (!clusterEnabled) return;
            // Grow the buffer if needed
            if (keyCount >= keys.Length)
            {
                var oldKeys = keys;
                keys = new ArgSlice[keys.Length * 2];
                Array.Copy(oldKeys, keys, oldKeys.Length);
            }

            keys[keyCount++] = argSlice;
        }

        /// <summary>
        /// Update argslice ptr if input buffer has been resized
        /// </summary>
        /// <param name="recvBufferPtr"></param>
        public unsafe void UpdateRecvBufferPtr(byte* recvBufferPtr)
        {
            //Execute method only if clusterEnabled
            if (!clusterEnabled) return;
            if (recvBufferPtr != saveKeyRecvBufferPtr)
            {
                for (int i = 0; i < keyCount; i++)
                    keys[i].ptr = recvBufferPtr + (keys[i].ptr - saveKeyRecvBufferPtr);
            }
        }
    }
}