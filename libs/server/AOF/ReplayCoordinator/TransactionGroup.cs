// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Transaction group contains logAccessMap and list of operations associated with this Txn
    /// </summary>
    /// <param name="sublogIdx"></param>
    /// <param name="logAccessMap"></param>
    public class TransactionGroup(int sublogIdx, byte logAccessMap)
    {
        /// <summary>
        /// Virtual sublog index associated with this transaction group.
        /// </summary>
        public readonly int VirtualSublogIdx = sublogIdx;

        /// <summary>
        /// Virtual sublog access count associated with this transaction group.
        /// </summary>
        public readonly byte LogAccessCount = logAccessMap;

        /// <summary>
        /// Operations associated with this transaction group.
        /// </summary>
        public List<byte[]> Operations = [];

        /// <summary>
        /// Clear the underlying buffer that holds the individual transaction operations.
        /// </summary>
        public void Clear() => Operations.Clear();
    }
}