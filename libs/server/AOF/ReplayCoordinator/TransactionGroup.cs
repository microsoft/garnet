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
        internal List<ReplayOperation> Operations = [];

        /// <summary>
        /// Drop the buffered operations, returning the pooled chunk buffers held by any chunked operation among them.
        /// Idempotent with respect to those buffers: a chunked operation releases its own as it is dispatched, and
        /// returning an already-returned chunk list is a no-op, so this is safe on a group that has just been replayed.
        /// </summary>
        public void Discard()
        {
            foreach (var op in Operations)
                op.Chunk?.ReturnValueChunks();
            Operations.Clear();
        }
    }
}