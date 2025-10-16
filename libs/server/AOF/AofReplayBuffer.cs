// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    public sealed unsafe partial class AofProcessor
    {
        readonly AofReplayBuffer aofReplayBuffer;

        public class AofReplayBuffer(AofProcessor aofProcessor, ILogger logger = null)
        {
            readonly AofProcessor aofProcessor = aofProcessor;
            readonly List<byte[]> fuzzyRegionOps = [];
            readonly Queue<List<byte[]>> txnBatchBuffer = [];
            readonly Dictionary<int, List<byte[]>> activeTxns = [];
            readonly ILogger logger = logger;

            internal int FuzzyRegionBufferCount => fuzzyRegionOps.Count;

            internal void ClearFuzzyRegionBuffer() => fuzzyRegionOps.Clear();

            internal unsafe void TryAddOperation(byte* ptr, int length) => fuzzyRegionOps.Add(new ReadOnlySpan<byte>(ptr, length).ToArray());

            internal unsafe bool TryAddTransactionOperation(AofHeader header, byte* ptr, int length, bool asReplica)
            {
                // First try to process this as an existing transaction
                if (activeTxns.TryGetValue(header.sessionID, out var batch))
                {
                    switch (header.opType)
                    {
                        case AofEntryType.TxnAbort:
                            ClearSessionTxn();
                            break;
                        case AofEntryType.TxnCommit:
                            if (aofProcessor.inFuzzyRegion)
                            {
                                // Buffer commit marker and operations batch when in fuzzy region
                                fuzzyRegionOps.Add(new ReadOnlySpan<byte>(ptr, length).ToArray());
                                txnBatchBuffer.Enqueue(batch);
                            }
                            else
                                aofProcessor.ProcessTxn(batch, asReplica);

                            // We want to clear and remove in both cases to make space for next txn from session if any
                            // Example (assume generated from same session): TxnStart1 CheckpointStart TxnCommit1 TxnStart2 CheckpointEnd TxnCommit2
                            ClearSessionTxn();
                            break;
                        case AofEntryType.StoredProcedure:
                            throw new GarnetException($"Unexpected AOF header operation type {header.opType} within transaction");
                        default:
                            batch.Add(new ReadOnlySpan<byte>(ptr, length).ToArray());
                            break;
                    }

                    void ClearSessionTxn()
                    {
                        activeTxns[header.sessionID].Clear();
                        activeTxns.Remove(header.sessionID);
                    }

                    return true;
                }

                // See if you have detected a txn
                switch (header.opType)
                {
                    case AofEntryType.TxnStart:
                        activeTxns[header.sessionID] = [];
                        break;
                    case AofEntryType.TxnAbort:
                    case AofEntryType.TxnCommit:
                        // We encountered a transaction end without start - this could happen because we truncated the AOF
                        // after a checkpoint, and the transaction belonged to the previous version. It can safely
                        // be ignored.
                        break;
                    default:
                        // Continue processing
                        return false;
                }

                // Processed this record succesfully
                return true;
            }

            /// <summary>
            /// Process fuzzy region operations if any
            /// </summary>
            /// <param name="storeVersion"></param>
            /// <param name="asReplica"></param>
            internal void ProcessFuzzyRegionOperations(long storeVersion, bool asReplica)
            {
                if (fuzzyRegionOps.Count > 0)
                    logger?.LogInformation("Replaying {fuzzyRegionBufferCount} records from fuzzy region for checkpoint {newVersion}", fuzzyRegionOps.Count, storeVersion);
                foreach (var entry in fuzzyRegionOps)
                {
                    fixed (byte* entryPtr = entry)
                        aofProcessor.ReplayOp(entryPtr, entry.Length, asReplica);
                }
            }

            /// <summary>
            /// Process fuzzy region transactions in FIFO order
            /// </summary>
            /// <param name="asReplica"></param>
            internal void ProcessFuzzyRegionTransactions(bool asReplica)
            {
                var batch = txnBatchBuffer.Dequeue();
                aofProcessor.ProcessTxn(batch, asReplica);
            }
        }
    }
}