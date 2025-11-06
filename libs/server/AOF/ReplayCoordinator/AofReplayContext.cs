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
