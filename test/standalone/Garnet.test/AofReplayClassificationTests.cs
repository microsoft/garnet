// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Tests of <see cref="AofProcessor.ReplayCanBlock(AofEntryType, bool)"/>, which tells a replica that replays on its
    /// network receive thread which records have to be replayed on a thread that may block. A record wrongly reported as
    /// unable to block stalls that receive thread, and with it every connection that shares the socket engine thread.
    /// </summary>
    [TestFixture]
    public class AofReplayClassificationTests : TestBase
    {
        // Whether replaying a record can block, on a single log and on a sharded one
        static readonly Dictionary<AofEntryType, (bool SingleLog, bool ShardedLog)> Expected = new()
        {
            [AofEntryType.StoreUpsert] = (false, false),
            [AofEntryType.StoreRMW] = (false, false),
            [AofEntryType.StoreDelete] = (false, false),
            [AofEntryType.ObjectStoreUpsert] = (false, false),
            [AofEntryType.ObjectStoreRMW] = (false, false),
            [AofEntryType.ObjectStoreDelete] = (false, false),
            [AofEntryType.UnifiedStoreStringUpsert] = (false, false),
            [AofEntryType.UnifiedStoreObjectUpsert] = (false, false),
            [AofEntryType.UnifiedStoreRMW] = (false, false),
            [AofEntryType.UnifiedStoreDelete] = (false, false),
            [AofEntryType.RangeIndexStreamChunk] = (false, false),
            [AofEntryType.TxnStart] = (false, false),
            [AofEntryType.TxnAbort] = (false, false),
            [AofEntryType.CheckpointStartCommit] = (false, false),
            [AofEntryType.MainStoreStreamingCheckpointEndCommit] = (false, false),
            [AofEntryType.ObjectStoreStreamingCheckpointEndCommit] = (false, false),

            // Rendezvous with the other replay participants only when replay is sharded
            [AofEntryType.TxnCommit] = (false, true),
            [AofEntryType.StoredProcedure] = (false, true),
            [AofEntryType.MainStoreStreamingCheckpointStartCommit] = (false, true),
            [AofEntryType.ObjectStoreStreamingCheckpointStartCommit] = (false, true),

            // Take a checkpoint or reset the store
            [AofEntryType.CheckpointEndCommit] = (true, true),
            [AofEntryType.FlushAll] = (true, true),
            [AofEntryType.FlushDb] = (true, true),
        };

        [Test]
        public void EveryRecordTypeIsClassifiedExplicitly()
        {
            // A type missing here is reported as able to block, which is safe but costs a thread hop per record, so a new
            // type has to be classified deliberately
            var unclassified = Enum.GetValues<AofEntryType>().Where(type => !Expected.ContainsKey(type)).ToArray();
            ClassicAssert.IsEmpty(unclassified, $"Classify these record types in {nameof(AofProcessor)}.{nameof(AofProcessor.ReplayCanBlock)} and in this table: {string.Join(", ", unclassified)}");

            var mismatches = new List<string>();
            foreach (var (type, expected) in Expected)
            {
                if (AofProcessor.ReplayCanBlock(type, shardedLog: false) != expected.SingleLog)
                    mismatches.Add($"{type} on a single log: expected {expected.SingleLog}");
                if (AofProcessor.ReplayCanBlock(type, shardedLog: true) != expected.ShardedLog)
                    mismatches.Add($"{type} on a sharded log: expected {expected.ShardedLog}");
            }

            ClassicAssert.IsEmpty(mismatches, string.Join("; ", mismatches));
        }

        [Test]
        public void AnUnknownRecordTypeIsReportedAsAbleToBlock()
        {
            ClassicAssert.IsTrue(AofProcessor.ReplayCanBlock((AofEntryType)0xFF, shardedLog: false));
            ClassicAssert.IsTrue(AofProcessor.ReplayCanBlock((AofEntryType)0xFF, shardedLog: true));
        }
    }
}
