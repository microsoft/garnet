// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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

            // Double the parse state buffer capacity if needed, and copy existing parameters to the extended buffer
            if (count >= clusterKeyParseState.Capacity)
            {
                var oldParams = clusterKeyParseState.Parameters;
                clusterKeyParseState.Initialize(count * 2);
                clusterKeyParseState.SetArguments(0, oldParams);
            }

            // Copy key bytes into dedicated txn scratch buffer (independent of receive buffer lifetime)
            var keySlice = txnScratchBuffer.CreateArgSlice(argSlice.ReadOnlySpan);

            clusterKeyParseState.Count = count + 1;
            clusterKeyParseState.SetArgument(count, keySlice);
        }
    }
}