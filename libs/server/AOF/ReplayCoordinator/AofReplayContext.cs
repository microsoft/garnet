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
    public class AofReplayContext
    {
        public readonly List<byte[]> fuzzyRegionOps = [];
        public readonly Queue<TransactionGroup> txnGroupBuffer = [];
        public readonly Dictionary<int, TransactionGroup> activeTxns = [];

        public readonly RawStringInput storeInput;
        public readonly ObjectInput objectStoreInput;
        public CustomProcedureInput customProcInput;
        public readonly SessionParseState parseState;

        public readonly byte[] objectOutputBuffer;

        public MemoryResult<byte> output;

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
        /// AofReplayContext constructor
        /// </summary>
        public AofReplayContext()
        {
            parseState.Initialize();
            storeInput.parseState = parseState;
            objectStoreInput.parseState = parseState;
            customProcInput.parseState = parseState;
            objectOutputBuffer = GC.AllocateArray<byte>(BufferSizeUtils.ServerBufferSize(new MaxSizeSettings()), pinned: true);
        }

        /// <summary>
        /// Add transaction group to this replay buffer
        /// </summary>
        /// <param name="sessionID"></param>
        /// <param name="sublogIdx"></param>
        /// <param name="logAccessBitmap"></param>
        public void AddTransactionGroup(int sessionID, int sublogIdx, byte logAccessBitmap)
            => activeTxns[sessionID] = new(sublogIdx, logAccessBitmap);

        /// <summary>
        /// Add transaction group to fuzzy region buffer
        /// </summary>
        /// <param name="group"></param>
        /// <param name="commitMarker"></param>
        public void AddToFuzzyRegionBuffer(TransactionGroup group, ReadOnlySpan<byte> commitMarker)
        {
            // Add commit marker operation
            fuzzyRegionOps.Add(commitMarker.ToArray());
            // Enqueue transaction group
            txnGroupBuffer.Enqueue(group);
        }
    }
}