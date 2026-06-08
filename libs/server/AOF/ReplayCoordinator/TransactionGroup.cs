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
    /// <param name="startSequenceNumber">Sequence number or entry address of the TxnStart entry</param>
    public class TransactionGroup(int sublogIdx, byte logAccessMap, long startSequenceNumber = 0)
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
        /// Sequence number or entry address of the TxnStart entry, used to key the acquire-barrier
        /// so it is distinct from the release-barrier keyed on the TxnCommit entry.
        /// </summary>
        public readonly long StartSequenceNumber = startSequenceNumber;

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