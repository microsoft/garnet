// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;
using Garnet.networking;

namespace Garnet.server
{
    /// <summary>
    /// Sublog replay buffer (one for each sublog)
    /// </summary>
    internal sealed class AofReplayContext
    {
        public readonly List<ReplayOperation> fuzzyRegionOps = [];
        public readonly Queue<TransactionGroup> txnGroupBuffer = [];
        public readonly Dictionary<int, TransactionGroup> activeTxns = [];

        /// <summary>Accumulates chunked-record fragments (keyed by objectId) until a logical record is complete.</summary>
        public readonly AofChunkedRecordReader chunkedReader = new();

        internal readonly RespServerSession respServerSession;

        public CustomProcedureInput customProcInput;
        public SessionParseState parseState;

        public readonly byte[] objectOutputBuffer;

        public MemoryResult<byte> output;

        public StringBasicContext StringBasicContext => respServerSession.storageSession.stringBasicContext;
        public ObjectBasicContext ObjectBasicContext => respServerSession.storageSession.objectBasicContext.Session == null ? default : respServerSession.storageSession.objectBasicContext.Session.BasicContext;
        public UnifiedBasicContext UnifiedBasicContext => respServerSession.storageSession.unifiedBasicContext;

        /// <summary>
        /// Fuzzy region of AOF is the region between the checkpoint start and end commit markers.
        /// This regions can contain entries in both (v) and (v+1) versions. The processing logic is:
        /// 1) Process (v) entries as is.
        /// 2) Store the (v+1) entries in a buffer.
        /// 3) At the end of the fuzzy region, take a checkpoint
        /// 4) Finally, replay the buffered (v+1) entries.
        /// </summary>
        public bool inFuzzyRegion = false;

        /// <summary>
        /// Replayed records remaining before the next scratch-allocator trim. Replay has no network batch
        /// boundary, so the interval is counted here, mirroring the session's own countdown.
        /// </summary>
        internal int trimCountdown = RespServerSession.SessionTrimInterval;

        /// <summary>
        /// AOF replay context constructor
        /// </summary>
        public AofReplayContext(RespServerSession respServerSession)
        {
            this.respServerSession = respServerSession;
            parseState.Initialize();
            customProcInput.parseState = parseState;
            objectOutputBuffer = GC.AllocateArray<byte>(BufferSizeUtils.ServerBufferSize(new MaxSizeSettings()), pinned: true);
        }

        public void Dispose()
        {
            // Release the pooled chunk buffers held by anything replay did not consume: partially-accumulated records (a
            // truncated AOF tail is the normal outcome of a crash) and any operation still buffered. These rentals are
            // otherwise never returned, and a rental that is never returned permanently consumes the pool's cacheable
            // budget rather than merely going unreused. Nothing here is replayed after this point, so all of it is
            // discarded. A group is removed from activeTxns when it is enqueued to txnGroupBuffer, so the two hold
            // disjoint sets and no group is discarded twice.
            chunkedReader.DiscardInProgressAccumulations();
            DiscardFuzzyRegionBuffer();
            foreach (var txn in activeTxns.Values)
                txn.Discard();
            activeTxns.Clear();

            var databaseSessionsSnapshot = respServerSession.GetDatabaseSessionsSnapshot();
            foreach (var dbSession in databaseSessionsSnapshot)
            {
                dbSession.StorageSession.stringBasicContext.Session?.Dispose();
                dbSession.StorageSession.objectBasicContext.Session?.Dispose();
            }
            respServerSession?.Dispose();
            output.MemoryOwner?.Dispose();
        }

        /// <summary>
        /// Discard the fuzzy-region buffer, returning the pooled chunk buffers of any chunked operation it holds. Only for
        /// a buffer that will not be replayed: either it has just been replayed (returning a chunked operation's buffers a
        /// second time is a no-op, since the operation released them as it was dispatched) or replay is tearing down or
        /// abandoning the region.
        /// </summary>
        /// <remarks>
        /// The buffered transaction groups go with the operations, because a group is only reachable through the commit
        /// marker recorded alongside it: dropping the markers while leaving the groups queued would put the two out of
        /// step, and the next region's markers would dequeue the wrong groups.
        /// </remarks>
        public void DiscardFuzzyRegionBuffer()
        {
            foreach (var op in fuzzyRegionOps)
                op.Chunk?.ReturnValueChunks();
            fuzzyRegionOps.Clear();

            while (txnGroupBuffer.Count > 0)
                txnGroupBuffer.Dequeue().Discard();
        }

        /// <summary>
        /// Add transaction group to this replay buffer
        /// </summary>
        /// <param name="sessionID"></param>
        /// <param name="sublogIdx"></param>
        /// <param name="logAccessBitmap"></param>
        /// <param name="startSequenceNumber">Sequence number or entry address of the TxnStart entry</param>
        public void AddTransactionGroup(int sessionID, int sublogIdx, byte logAccessBitmap, long startSequenceNumber = 0)
            => activeTxns[sessionID] = new(sublogIdx, logAccessBitmap, startSequenceNumber);

        /// <summary>
        /// Add transaction group to fuzzy region buffer
        /// </summary>
        /// <param name="group">The transaction group, whose ownership passes to this buffer.</param>
        /// <param name="commitMarker">The TxnCommit record bytes, which mark where in the buffered stream the group
        /// is replayed.</param>
        /// <param name="commitSequenceNumber">Log address sequence number of the commit record, needed for the
        /// multi-log commit barrier when the group is eventually replayed.</param>
        public void AddToFuzzyRegionBuffer(TransactionGroup group, ReadOnlySpan<byte> commitMarker, long commitSequenceNumber = 0)
        {
            // Add commit marker operation
            fuzzyRegionOps.Add(new ReplayOperation(commitMarker.ToArray(), commitSequenceNumber));
            // Enqueue transaction group
            txnGroupBuffer.Enqueue(group);
        }
    }
}