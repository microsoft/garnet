// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Represents a pending entry in a consumer group's Pending Entries List (PEL).
    /// Tracks delivery metadata for an entry that has been read but not yet acknowledged.
    /// </summary>
    public class PendingEntry
    {
        /// <summary>Stream entry ID.</summary>
        public StreamID Id;

        /// <summary>Name of the consumer that owns this pending entry.</summary>
        public string ConsumerName;

        /// <summary>UTC timestamp (ms) of the most recent delivery to the consumer.</summary>
        public long DeliveryTime;

        /// <summary>Number of times this entry has been delivered (initial delivery = 1).</summary>
        public int DeliveryCount;

        public PendingEntry(StreamID id, string consumerName, long deliveryTimeMs)
        {
            Id = id;
            ConsumerName = consumerName;
            DeliveryTime = deliveryTimeMs;
            DeliveryCount = 1;
        }
    }

    /// <summary>
    /// Represents a consumer within a consumer group.
    /// </summary>
    public class StreamConsumer
    {
        /// <summary>Consumer name (unique within the group).</summary>
        public readonly string Name;

        /// <summary>UTC timestamp (ms) of the last time this consumer was active
        /// (XREADGROUP, XCLAIM, XACK, XAUTOCLAIM).</summary>
        public long SeenTime;

        /// <summary>
        /// IDs of entries pending for this consumer (subset of the group PEL).
        /// Maintained as a sorted set for efficient XPENDING range queries.
        /// </summary>
        public readonly SortedSet<StreamID> PendingIds;

        public StreamConsumer(string name, long nowMs)
        {
            Name = name;
            SeenTime = nowMs;
            PendingIds = new SortedSet<StreamID>(Comparer<StreamID>.Create((a, b) => a.CompareTo(b)));
        }
    }

    /// <summary>
    /// Represents a consumer group attached to a stream. Holds the group-level PEL,
    /// per-consumer state, and the last-delivered-id cursor.
    /// 
    /// Thread safety: callers must hold the owning <see cref="StreamObject"/>'s write lock
    /// for mutations, and the read lock for read-only queries. The consumer group does not
    /// carry its own lock.
    /// </summary>
    public class ConsumerGroup
    {
        /// <summary>Group name.</summary>
        public readonly string Name;

        /// <summary>
        /// The ID of the last entry delivered to any consumer via XREADGROUP with "&gt;".
        /// New reads deliver entries after this cursor.
        /// </summary>
        public StreamID LastDeliveredId;

        /// <summary>
        /// Running count of entries read (delivered) by this group. Used to compute
        /// lag = stream.Length - EntriesRead.
        /// </summary>
        public long EntriesRead;

        /// <summary>
        /// Group-level Pending Entries List, keyed by entry ID.
        /// An entry appears here from XREADGROUP delivery until XACK.
        /// </summary>
        public readonly SortedList<StreamID, PendingEntry> PEL;

        /// <summary>
        /// Consumers in this group, keyed by consumer name.
        /// </summary>
        public readonly Dictionary<string, StreamConsumer> Consumers;

        /// <summary>
        /// Creates a new consumer group.
        /// </summary>
        /// <param name="name">Group name.</param>
        /// <param name="lastDeliveredId">Initial last-delivered-id. Use <c>0-0</c> to deliver
        /// all existing entries, or <c>$</c> (resolved to the stream's last ID) to deliver
        /// only new entries.</param>
        /// <param name="entriesRead">Initial entries-read counter. Pass -1 when unknown
        /// (e.g. when using <c>$</c>), or 0 when starting from <c>0-0</c>.</param>
        public ConsumerGroup(string name, StreamID lastDeliveredId, long entriesRead)
        {
            Name = name;
            LastDeliveredId = lastDeliveredId;
            EntriesRead = entriesRead;
            PEL = new SortedList<StreamID, PendingEntry>(Comparer<StreamID>.Create((a, b) => a.CompareTo(b)));
            Consumers = new Dictionary<string, StreamConsumer>(StringComparer.Ordinal);
        }

        /// <summary>
        /// Get or auto-create a consumer. Updates SeenTime.
        /// </summary>
        public StreamConsumer GetOrCreateConsumer(string consumerName, long nowMs)
        {
            if (!Consumers.TryGetValue(consumerName, out var consumer))
            {
                consumer = new StreamConsumer(consumerName, nowMs);
                Consumers[consumerName] = consumer;
            }
            else
            {
                consumer.SeenTime = nowMs;
            }
            return consumer;
        }

        /// <summary>
        /// Explicitly create a consumer. Returns false if the consumer already exists.
        /// </summary>
        public bool CreateConsumer(string consumerName, long nowMs)
        {
            if (Consumers.ContainsKey(consumerName))
                return false;
            Consumers[consumerName] = new StreamConsumer(consumerName, nowMs);
            return true;
        }

        /// <summary>
        /// Delete a consumer. Returns the number of pending entries that were removed.
        /// </summary>
        public int DeleteConsumer(string consumerName)
        {
            if (!Consumers.TryGetValue(consumerName, out var consumer))
                return 0;

            int removed = consumer.PendingIds.Count;

            // Remove all of this consumer's entries from the group PEL
            foreach (var id in consumer.PendingIds)
            {
                PEL.Remove(id);
            }

            Consumers.Remove(consumerName);
            return removed;
        }

        /// <summary>
        /// Acknowledge one or more entry IDs: remove from group PEL and consumer PEL.
        /// Returns the number of entries actually acknowledged.
        /// </summary>
        public int Acknowledge(ReadOnlySpan<StreamID> ids, long nowMs)
        {
            int acked = 0;
            foreach (var id in ids)
            {
                if (!PEL.TryGetValue(id, out var pending))
                    continue;

                PEL.Remove(id);

                if (Consumers.TryGetValue(pending.ConsumerName, out var consumer))
                {
                    consumer.PendingIds.Remove(id);
                    consumer.SeenTime = nowMs;
                }

                acked++;
            }
            return acked;
        }

        /// <summary>
        /// Add an entry to the PEL (called during XREADGROUP delivery).
        /// </summary>
        public void AddPendingEntry(StreamID id, StreamConsumer consumer, long nowMs)
        {
            var entry = new PendingEntry(id, consumer.Name, nowMs);
            PEL[id] = entry;
            consumer.PendingIds.Add(id);
        }
    }
}