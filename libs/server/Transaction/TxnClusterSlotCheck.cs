// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
    sealed unsafe partial class TransactionManager
    {
        //Keys involved in the current transaction
        ArgSlice[] clusterKeys;
        int clusterKeyCount;

        internal byte* saveKeyRecvBufferPtr;
        readonly bool clusterEnabled;

        /// <summary>
        /// Keep track of actual key accessed by command
        /// </summary>
        public void SaveKeyArgSlice(ArgSlice argSlice)
        {
            //Execute method only if clusterEnabled
            if (!clusterEnabled)
                return;

            // Grow the buffer if needed
            if (clusterKeyCount >= clusterKeys.Length)
            {
                var oldKeys = clusterKeys;
                clusterKeys = new ArgSlice[clusterKeys.Length * 2];
                Array.Copy(oldKeys, clusterKeys, oldKeys.Length);
            }
            clusterKeys[clusterKeyCount++] = argSlice;
        }

        /// <summary>
        /// Update argslice ptr if input buffer has been resized
        /// </summary>
        public unsafe void UpdateRecvBufferPtr(byte* recvBufferPtr)
        {
            //Execute method only if clusterEnabled
            if (!clusterEnabled)
                return;

            if (recvBufferPtr != saveKeyRecvBufferPtr)
            {
                for (var i = 0; i < clusterKeyCount; i++)
                    clusterKeys[i].ptr = recvBufferPtr + (clusterKeys[i].ptr - saveKeyRecvBufferPtr);
            }
        }
    }
}