// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Garnet.server.BTreeIndex;
using Tsavorite.core;

namespace Garnet.server
{
    public partial class StreamObject : IGarnetObject
    {
        /// <summary>
        /// Adds an entry or item to the stream
        /// </summary>
        public unsafe void AddEntry(PinnedSpanByte idSlice, int numPairs, ReadOnlySpan<byte> rawFieldValuePairs, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            StreamID id = default;
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output);

            var parsedIDStatus = parseIDString(idSlice, ref id);
            if (parsedIDStatus == ParsedStreamEntryID.INVALID)
            {
                writer.WriteError(CmdStrings.RESP_ERR_XADD_INVALID_STREAM_ID);
                return;
            }
            else if (parsedIDStatus == ParsedStreamEntryID.NOT_GREATER)
            {
                writer.WriteError(CmdStrings.RESP_ERR_XADD_ID_NOT_GREATER);
                return;
            }

            // add the entry to the log
            StreamLogEntryHeader header = new StreamLogEntryHeader
            {
                id = id,
                numPairs = numPairs,
            };

            log.Enqueue<StreamLogEntryHeader>(header, item: rawFieldValuePairs, out long returnedLogicalAddr);

            // BTree append-only insert. parseIDString already enforces strict monotonic IDs,
            // so this never collides with an existing key.
            index.InsertIntoTail((byte*)Unsafe.AsPointer(ref id.idBytes[0]), new Value(returnedLogicalAddr));

            // copy encoded ms and seq
            lastId.ms = id.ms;
            lastId.seq = id.seq;

            totalEntriesAdded++;

            ulong idMS = id.getMS();
            ulong idSeq = id.getSeq();
            Span<byte> outputBuffer = stackalloc byte[(NumUtils.MaximumFormatInt64Length * 2) + 1];
            int len = NumUtils.WriteInt64((long)idMS, outputBuffer);
            outputBuffer[len++] = (byte)'-';
            len += NumUtils.WriteInt64((long)idSeq, outputBuffer.Slice(len));

            writer.WriteBulkString(outputBuffer.Slice(0, len));

        }

        /// <summary>
        /// Get current length of the stream (number of non-tombstoned entries in the stream)
        /// </summary>
        /// <returns>length of stream</returns>
        public ulong Length()
        {
            ulong len;
            len = index.ValidCount;
            return len;
        }

        /// <summary>
        /// Deletes an entry from the stream
        /// </summary>
        /// <param name="idSlice">id of the stream entry to delete</param>
        /// <returns>true if entry was deleted successfully</returns>
        public unsafe bool DeleteEntry(PinnedSpanByte idSlice)
        {
            if (!parseCompleteID(idSlice, out StreamID entryID))
            {
                return false;
            }
            bool deleted;
            deleted = index.Delete((byte*)Unsafe.AsPointer(ref entryID.idBytes[0]));
            if (deleted)
            {
                // Persist a tombstone marker so recovery doesn't resurrect this entry.
                var marker = new StreamLogEntryHeader { id = entryID, numPairs = ControlRecordKind.Tombstone };
                log.Enqueue<StreamLogEntryHeader>(marker, item: [], out _);
            }
            return deleted;
        }


        /// <summary>
        /// Read entries from the stream from given range
        /// </summary>
        /// <param name="min">start of range</param>
        /// <param name="max">end of range</param>
        /// <param name="limit">threshold to scanning</param>
        /// <param name="output"></param>
        public unsafe int ReadRange(string min, string max, int limit, ref SpanByteAndMemory output, byte respProtocolVersion, bool isReverse = false)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                if (index.ValidCount == 0)
                {
                    return 0;
                }

                // Sentinels for "-" and "+". StreamID is stored big-endian, so byte-wise
                // comparison (used by BTree) gives the same ordering as numeric comparison.
                StreamID minStreamID = new StreamID(0UL, 0UL);
                StreamID maxStreamID = new StreamID(ulong.MaxValue, ulong.MaxValue);

                StreamID startID, endID;
                if (min == "-")
                {
                    startID = minStreamID;
                }
                else if (min == "+")
                {
                    startID = maxStreamID;
                }
                else if (!ParseStreamIDFromString(min, out startID))
                {
                    return 0;
                }

                if (max == "+")
                {
                    endID = maxStreamID;
                }
                else if (max == "-")
                {
                    endID = minStreamID;
                }
                else if (!ParseStreamIDFromString(max, out endID))
                {
                    return 0;
                }

                // BTree.Get asserts ordering. For reverse, start (larger) >= end (smaller).
                int cmp = startID.CompareTo(endID);
                if (isReverse ? cmp < 0 : cmp > 0)
                {
                    writer.WriteArrayLength(0);
                    return 0;
                }

                byte* startPtr = (byte*)Unsafe.AsPointer(ref startID.idBytes[0]);
                byte* endPtr = (byte*)Unsafe.AsPointer(ref endID.idBytes[0]);
                int actualLimit = limit > 0 ? limit : -1;

                int validCount = index.Get(startPtr, endPtr, out Value startVal, out Value endVal,
                    out List<Value> tombstones, actualLimit, isReverse);

                if (validCount == 0)
                {
                    writer.WriteArrayLength(0);
                    return 0;
                }

                HashSet<long> tombstoneAddrs = null;
                if (tombstones != null && tombstones.Count > 0)
                {
                    tombstoneAddrs = new HashSet<long>(tombstones.Count);
                    foreach (var t in tombstones)
                    {
                        tombstoneAddrs.Add(t.address);
                    }
                }

                // For reverse: startVal is at the higher address, endVal at the lower.
                // For forward: startVal is at the lower address, endVal at the higher.
                long scanStart = (isReverse ? endVal.address : startVal.address);
                long scanEnd = (isReverse ? startVal.address : endVal.address);

                // After XTRIM, BTree leaves still reference addresses below the new
                // log.BeginAddress. Clamp so log.Scan never tries to read truncated pages.
                long beginAddr = log.BeginAddress;
                if (scanStart < beginAddr) scanStart = beginAddr;
                if (scanEnd < beginAddr)
                {
                    // Whole requested window was truncated; nothing to emit.
                    writer.WriteArrayLength(0);
                    return 0;
                }

                writer.WriteArrayLength(validCount);

                if (isReverse)
                {
                    // Log scans are forward-only; buffer entries and emit reversed.
                    var entries = new List<byte[]>(validCount);
                    using (var iter = log.Scan(scanStart, scanEnd + 1, scanUncommitted: true))
                    {
                        while (iter.GetNext(out byte[] entry, out _, out long currentAddress))
                        {
                            if (tombstoneAddrs != null && tombstoneAddrs.Contains(currentAddress))
                                continue;
                            if (IsControlRecord(entry)) continue;
                            entries.Add(entry);
                            if (entries.Count >= validCount)
                                break;
                        }
                    }
                    for (int i = entries.Count - 1; i >= 0; i--)
                    {
                        WriteEntryToWriter(entries[i], ref writer);
                    }
                }
                else
                {
                    using (var iter = log.Scan(scanStart, scanEnd + 1, scanUncommitted: true))
                    {
                        int written = 0;
                        while (iter.GetNext(out byte[] entry, out _, out long currentAddress))
                        {
                            if (tombstoneAddrs != null && tombstoneAddrs.Contains(currentAddress))
                                continue;
                            if (IsControlRecord(entry)) continue;
                            WriteEntryToWriter(entry, ref writer);
                            written++;
                            if (written >= validCount)
                                break;
                        }
                    }
                }

                return validCount;
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Trims the stream based on the specified options.
        /// </summary>
        /// <param name="trimArg">length or ID specifying the threshold</param>
        /// <param name="optType">MAXLEN or MINID</param>
        /// <param name="entriesTrimmed">number of keys trimmed</param>
        /// <returns></returns>
        public unsafe bool Trim(PinnedSpanByte trimArg, StreamTrimOpts optType, out ulong entriesTrimmed, bool approximate = false)
        {
            Value newHead = default;
            switch (optType)
            {
                case StreamTrimOpts.MAXLEN:
                    if (!RespReadUtils.ReadUlong(out ulong maxLen, ref trimArg.ptr, trimArg.ptr + trimArg.length))
                    {
                        entriesTrimmed = 0;
                        return false;
                    }
                    index.TrimByLength(maxLen, out entriesTrimmed, out newHead, out _, out _, approximate);
                    break;
                case StreamTrimOpts.MINID:
                    if (!parseCompleteID(trimArg, out StreamID minID))
                    {
                        entriesTrimmed = 0;
                        return false;
                    }
                    index.TrimByID((byte*)Unsafe.AsPointer(ref minID.idBytes[0]),
                        out entriesTrimmed, out newHead, out _, out _);
                    break;
                default:
                    entriesTrimmed = 0;
                    break;
            }

            if (entriesTrimmed > 0)
            {
                // XTRIM always drops the oldest entries (a contiguous prefix of both the
                // BTree key order and the log address order, since IDs and log addresses
                // both grow monotonically). Truncating the log past the new head IS the
                // persistence of the trim — recovery just won't see the dropped entries.
                // If everything was trimmed, push past the current tail so nothing replays.
                long target = newHead.Valid ? newHead.address : log.TailAddress;
                log.TruncateUntil(target);
            }
            // Note: BTree leaves still reference tombstoned entries at addresses below the
            // new BeginAddress. Range reads handle this by clamping scanStart to BeginAddress
            // before calling log.Scan — see ReadRange.
            return true;
        }

        #region Consumer Group Operations

        static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        /// <summary>
        /// Create a consumer group on this stream.
        /// </summary>
        /// <param name="groupName">Name of the group.</param>
        /// <param name="startId">The ID after which new entries will be delivered.
        /// Use <c>0-0</c> to deliver all existing entries, or pass the stream's last ID
        /// for "$" semantics.</param>
        /// <param name="entriesRead">Initial entries-read counter (-1 = unknown).</param>
        /// <returns>True if created; false if a group with that name already exists.</returns>
        public bool CreateGroup(string groupName, StreamID startId, long entriesRead)
        {
            if (consumerGroups.ContainsKey(groupName))
                return false;
            consumerGroups[groupName] = new ConsumerGroup(groupName, startId, entriesRead);
            return true;
        }

        /// <summary>
        /// Destroy a consumer group. Returns true if the group existed.
        /// </summary>
        public bool DestroyGroup(string groupName)
        {
            return consumerGroups.Remove(groupName);
        }

        /// <summary>
        /// Set the last-delivered-id for a consumer group.
        /// </summary>
        public bool SetGroupId(string groupName, StreamID id, long entriesRead)
        {
            if (!consumerGroups.TryGetValue(groupName, out var group))
                return false;
            group.LastDeliveredId = id;
            if (entriesRead >= 0)
                group.EntriesRead = entriesRead;
            return true;
        }

        /// <summary>
        /// Explicitly create a consumer in a group. Returns false if it already exists.
        /// Returns null (via out bool?) if the group doesn't exist.
        /// </summary>
        public bool? CreateConsumer(string groupName, string consumerName)
        {
            if (!consumerGroups.TryGetValue(groupName, out var group))
                return null;
            return group.CreateConsumer(consumerName, NowMs());
        }

        /// <summary>
        /// Delete a consumer from a group. Returns the number of pending entries removed,
        /// or -1 if the group doesn't exist.
        /// </summary>
        public int DeleteConsumer(string groupName, string consumerName)
        {
            if (!consumerGroups.TryGetValue(groupName, out var group))
                return -1;
            return group.DeleteConsumer(consumerName);
        }

        /// <summary>
        /// Read entries from the stream for a consumer group (XREADGROUP).
        /// When <paramref name="id"/> is "&gt;", delivers new entries after the group's
        /// last-delivered-id. Otherwise, returns pending entries for the consumer starting
        /// from the given ID.
        /// </summary>
        /// <param name="groupName">Consumer group name.</param>
        /// <param name="consumerName">Consumer name (auto-created if needed).</param>
        /// <param name="id">"&gt;" for new entries, or a specific ID for pending re-read.</param>
        /// <param name="count">Max entries to return (-1 = unlimited).</param>
        /// <param name="noAck">If true, do not add to PEL on new delivery.</param>
        /// <param name="output">RESP output buffer.</param>
        /// <param name="respProtocolVersion">RESP protocol version.</param>
        /// <returns>Number of entries written, or -1 if the group was not found.</returns>
        public unsafe int ReadGroup(string groupName, string consumerName, string id,
            int count, bool noAck, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                if (!consumerGroups.TryGetValue(groupName, out var group))
                    return -1;

                long nowMs = NowMs();
                var consumer = group.GetOrCreateConsumer(consumerName, nowMs);

                if (id == ">")
                {
                    return ReadGroupNewEntries(group, consumer, count, noAck, nowMs, ref writer);
                }
                else
                {
                    return ReadGroupPendingEntries(group, consumer, id, count, ref writer);
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Deliver new entries (after group.LastDeliveredId) to the consumer.
        /// </summary>
        unsafe int ReadGroupNewEntries(ConsumerGroup group, StreamConsumer consumer,
            int count, bool noAck, long nowMs, ref RespMemoryWriter writer)
        {
            // Find entries after LastDeliveredId
            StreamID startID = group.LastDeliveredId;
            // We want entries strictly after LastDeliveredId. Increment seq to get exclusive start.
            ulong startSeq = startID.getSeq();
            if (startSeq < ulong.MaxValue)
            {
                startID.setSeq(startSeq + 1);
            }
            else
            {
                ulong startMs = startID.getMS();
                if (startMs < ulong.MaxValue)
                {
                    startID.setMS(startMs + 1);
                    startID.setSeq(0);
                }
                else
                {
                    // At the absolute maximum ID, nothing can follow
                    writer.WriteArrayLength(0);
                    return 0;
                }
            }

            StreamID endID = new StreamID(ulong.MaxValue, ulong.MaxValue);
            int actualLimit = count > 0 ? count : -1;

            byte* startPtr = (byte*)Unsafe.AsPointer(ref startID.idBytes[0]);
            byte* endPtr = (byte*)Unsafe.AsPointer(ref endID.idBytes[0]);

            int validCount = index.Get(startPtr, endPtr, out Value startVal, out Value endVal,
                out List<Value> tombstones, actualLimit, reverse: false);

            if (validCount == 0)
            {
                writer.WriteArrayLength(0);
                return 0;
            }

            HashSet<long> tombstoneAddrs = null;
            if (tombstones != null && tombstones.Count > 0)
            {
                tombstoneAddrs = new HashSet<long>(tombstones.Count);
                foreach (var t in tombstones)
                    tombstoneAddrs.Add(t.address);
            }

            long scanStart = startVal.address;
            long scanEnd = endVal.address;
            long beginAddr = log.BeginAddress;
            if (scanStart < beginAddr) scanStart = beginAddr;
            if (scanEnd < beginAddr)
            {
                writer.WriteArrayLength(0);
                return 0;
            }

            // Collect entries to deliver
            var entries = new List<(StreamID id, byte[] data)>(validCount);
            using (var iter = log.Scan(scanStart, scanEnd + 1, scanUncommitted: true))
            {
                while (iter.GetNext(out byte[] entry, out _, out long currentAddress))
                {
                    if (tombstoneAddrs != null && tombstoneAddrs.Contains(currentAddress))
                        continue;
                    if (IsControlRecord(entry)) continue;

                    var header = MemoryMarshal.Read<StreamLogEntryHeader>(
                        new ReadOnlySpan<byte>(entry, 0, sizeof(StreamLogEntryHeader)));
                    entries.Add((header.id, entry));
                    if (entries.Count >= validCount) break;
                }
            }

            writer.WriteArrayLength(entries.Count);
            foreach (var (entryId, entryData) in entries)
            {
                WriteEntryToWriter(entryData, ref writer);

                // Update group state
                group.LastDeliveredId = entryId;
                group.EntriesRead++;

                if (!noAck)
                {
                    group.AddPendingEntry(entryId, consumer, nowMs);
                }
            }

            return entries.Count;
        }

        /// <summary>
        /// Return pending entries for the consumer starting at the given ID.
        /// </summary>
        unsafe int ReadGroupPendingEntries(ConsumerGroup group, StreamConsumer consumer,
            string idStr, int count, ref RespMemoryWriter writer)
        {
            StreamID startID;
            if (idStr == "0" || idStr == "0-0")
            {
                startID = new StreamID(0, 0);
            }
            else if (!ParseCompleteStreamIDFromString(idStr, out startID))
            {
                writer.WriteArrayLength(0);
                return 0;
            }

            // Walk consumer's pending IDs from startID
            var pendingView = consumer.PendingIds.GetViewBetween(startID, new StreamID(ulong.MaxValue, ulong.MaxValue));
            var pendingIds = new List<StreamID>(pendingView);

            if (count > 0 && pendingIds.Count > count)
                pendingIds.RemoveRange(count, pendingIds.Count - count);

            if (pendingIds.Count == 0)
            {
                writer.WriteArrayLength(0);
                return 0;
            }

            // Read each pending entry from the log
            writer.WriteArrayLength(pendingIds.Count);
            foreach (var pendingId in pendingIds)
            {
                if (!group.PEL.TryGetValue(pendingId, out var pe))
                    continue;

                // Update delivery metadata
                pe.DeliveryCount++;
                pe.DeliveryTime = NowMs();

                // Look up address from BTree and read from log
                var lookupId = pendingId;
                byte* keyPtr = (byte*)Unsafe.AsPointer(ref lookupId.idBytes[0]);
                var result = index.Get(keyPtr);
                if (!result.Valid)
                {
                    // Entry was trimmed/deleted; still in PEL but data gone
                    writer.WriteNull();
                    continue;
                }

                long addr = result.address;
                using var iter = log.Scan(addr, addr + 1, scanUncommitted: true);
                if (iter.GetNext(out byte[] entry, out _, out _))
                {
                    WriteEntryToWriter(entry, ref writer);
                }
                else
                {
                    writer.WriteNull();
                }
            }

            return pendingIds.Count;
        }

        /// <summary>
        /// Acknowledge entries in a consumer group (XACK).
        /// </summary>
        /// <returns>Number of entries acknowledged, or -1 if group not found.</returns>
        public int Acknowledge(string groupName, ReadOnlySpan<StreamID> ids)
        {
            if (!consumerGroups.TryGetValue(groupName, out var group))
                return -1;
            return group.Acknowledge(ids, NowMs());
        }

        /// <summary>
        /// Get pending entries summary or detail (XPENDING).
        /// </summary>
        /// <param name="groupName">Consumer group name.</param>
        /// <param name="start">Start ID for range query (null for summary form).</param>
        /// <param name="end">End ID for range query.</param>
        /// <param name="count">Max entries to return.</param>
        /// <param name="minIdleTime">Minimum idle time filter in ms (-1 = no filter).</param>
        /// <param name="consumerFilter">Filter by consumer name (null = all consumers).</param>
        /// <param name="output">RESP output buffer.</param>
        /// <param name="respProtocolVersion">RESP protocol version.</param>
        /// <returns>True if group found, false otherwise.</returns>
        public bool GetPending(string groupName, string start, string end,
            int count, long minIdleTime, string consumerFilter,
            ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                if (!consumerGroups.TryGetValue(groupName, out var group))
                    return false;

                long nowMs = NowMs();

                if (start == null)
                {
                    // Summary form
                    WritePendingSummary(group, nowMs, ref writer);
                }
                else
                {
                    // Detail form
                    WritePendingDetail(group, start, end, count, minIdleTime, consumerFilter, nowMs, ref writer);
                }
                return true;
            }
            finally
            {
                writer.Dispose();
            }
        }

        void WritePendingSummary(ConsumerGroup group, long nowMs, ref RespMemoryWriter writer)
        {
            int total = group.PEL.Count;
            writer.WriteArrayLength(4);
            writer.WriteInt64(total);

            if (total == 0)
            {
                writer.WriteNull();
                writer.WriteNull();
                writer.WriteArrayLength(0);
                return;
            }

            // Min and max IDs in PEL
            WriteStreamIdToWriter(group.PEL.Keys[0], ref writer);
            WriteStreamIdToWriter(group.PEL.Keys[group.PEL.Count - 1], ref writer);

            // Per-consumer counts
            var consumerCounts = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var pe in group.PEL.Values)
            {
                consumerCounts.TryGetValue(pe.ConsumerName, out int c);
                consumerCounts[pe.ConsumerName] = c + 1;
            }

            writer.WriteArrayLength(consumerCounts.Count);
            foreach (var kvp in consumerCounts)
            {
                writer.WriteArrayLength(2);
                writer.WriteBulkString(System.Text.Encoding.UTF8.GetBytes(kvp.Key));
                writer.WriteBulkString(System.Text.Encoding.UTF8.GetBytes(kvp.Value.ToString()));
            }
        }

        void WritePendingDetail(ConsumerGroup group, string startStr, string endStr,
            int count, long minIdleTime, string consumerFilter, long nowMs,
            ref RespMemoryWriter writer)
        {
            StreamID startID = startStr == "-" ? new StreamID(0, 0) : default;
            StreamID endID = endStr == "+" ? new StreamID(ulong.MaxValue, ulong.MaxValue) : default;
            if (startStr != "-" && !ParseStreamIDFromString(startStr, out startID))
            {
                writer.WriteArrayLength(0);
                return;
            }
            if (endStr != "+" && !ParseStreamIDFromString(endStr, out endID))
            {
                writer.WriteArrayLength(0);
                return;
            }

            var results = new List<PendingEntry>();
            foreach (var kvp in group.PEL)
            {
                if (kvp.Key.CompareTo(startID) < 0) continue;
                if (kvp.Key.CompareTo(endID) > 0) break;

                var pe = kvp.Value;
                if (consumerFilter != null && pe.ConsumerName != consumerFilter) continue;
                if (minIdleTime >= 0 && (nowMs - pe.DeliveryTime) < minIdleTime) continue;

                results.Add(pe);
                if (count > 0 && results.Count >= count) break;
            }

            writer.WriteArrayLength(results.Count);
            foreach (var pe in results)
            {
                writer.WriteArrayLength(4);
                WriteStreamIdToWriter(pe.Id, ref writer);
                writer.WriteBulkString(Encoding.UTF8.GetBytes(pe.ConsumerName));
                writer.WriteInt64(nowMs - pe.DeliveryTime);
                writer.WriteInt64(pe.DeliveryCount);
            }
        }

        /// <summary>
        /// Claim pending entries (XCLAIM).
        /// </summary>
        /// <returns>List of claimed entry data, or null if group not found.</returns>
        public unsafe bool ClaimEntries(string groupName, string consumerName, long minIdleTime,
            StreamID[] ids, long? idleMs, long? timeMs, int? retryCount, bool force, bool justId,
            ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                if (!consumerGroups.TryGetValue(groupName, out var group))
                    return false;

                long nowMs = NowMs();
                var consumer = group.GetOrCreateConsumer(consumerName, nowMs);
                var claimed = new List<(StreamID id, byte[] data)>();

                foreach (var id in ids)
                {
                    if (!group.PEL.TryGetValue(id, out var pe))
                    {
                        if (!force) continue;
                        // FORCE: create a new PEL entry even if it doesn't exist
                        pe = new PendingEntry(id, consumerName, nowMs);
                        group.PEL[id] = pe;
                        consumer.PendingIds.Add(id);
                    }
                    else
                    {
                        long idle = nowMs - pe.DeliveryTime;
                        if (idle < minIdleTime && !force) continue;

                        // Transfer ownership: remove from old consumer
                        if (group.Consumers.TryGetValue(pe.ConsumerName, out var oldConsumer))
                        {
                            oldConsumer.PendingIds.Remove(id);
                        }

                        pe.ConsumerName = consumerName;
                        consumer.PendingIds.Add(id);
                    }

                    // Apply optional overrides
                    pe.DeliveryTime = timeMs ?? (idleMs.HasValue ? (nowMs - idleMs.Value) : nowMs);
                    if (retryCount.HasValue)
                        pe.DeliveryCount = retryCount.Value;
                    else
                        pe.DeliveryCount++;

                    if (!justId)
                    {
                        // Read entry data from log
                        var lookupId = id;
                        byte* keyPtr = (byte*)Unsafe.AsPointer(ref lookupId.idBytes[0]);
                        var result = index.Get(keyPtr);
                        if (result.Valid)
                        {
                            long addr = result.address;
                            using var iter = log.Scan(addr, addr + 1, scanUncommitted: true);
                            if (iter.GetNext(out byte[] entry, out _, out _))
                            {
                                claimed.Add((id, entry));
                                continue;
                            }
                        }
                    }
                    claimed.Add((id, null));
                }

                writer.WriteArrayLength(claimed.Count);
                foreach (var (claimedId, data) in claimed)
                {
                    if (justId)
                    {
                        WriteStreamIdToWriter(claimedId, ref writer);
                    }
                    else if (data != null)
                    {
                        WriteEntryToWriter(data, ref writer);
                    }
                    else
                    {
                        writer.WriteNull();
                    }
                }
                return true;
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Auto-claim idle pending entries (XAUTOCLAIM).
        /// </summary>
        public unsafe bool AutoClaim(string groupName, string consumerName, long minIdleTime,
            StreamID start, int count, bool justId,
            ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                if (!consumerGroups.TryGetValue(groupName, out var group))
                    return false;

                long nowMs = NowMs();
                var consumer = group.GetOrCreateConsumer(consumerName, nowMs);
                var claimed = new List<(StreamID id, byte[] data)>();
                var deletedIds = new List<StreamID>();
                StreamID nextCursor = new StreamID(0, 0);
                bool hasMore = false;

                // Scan PEL from start
                int scanned = 0;
                foreach (var kvp in group.PEL)
                {
                    if (kvp.Key.CompareTo(start) < 0) continue;

                    if (scanned >= count)
                    {
                        nextCursor = kvp.Key;
                        hasMore = true;
                        break;
                    }

                    var pe = kvp.Value;
                    long idle = nowMs - pe.DeliveryTime;
                    if (idle < minIdleTime)
                    {
                        scanned++;
                        continue;
                    }

                    // Check if entry still exists in the stream
                    var lookupId = kvp.Key;
                    byte* keyPtr = (byte*)Unsafe.AsPointer(ref lookupId.idBytes[0]);
                    var result = index.Get(keyPtr);
                    if (!result.Valid)
                    {
                        deletedIds.Add(kvp.Key);
                        scanned++;
                        continue;
                    }

                    // Transfer ownership
                    if (group.Consumers.TryGetValue(pe.ConsumerName, out var oldConsumer))
                    {
                        oldConsumer.PendingIds.Remove(kvp.Key);
                    }

                    pe.ConsumerName = consumerName;
                    pe.DeliveryTime = nowMs;
                    pe.DeliveryCount++;
                    consumer.PendingIds.Add(kvp.Key);

                    if (!justId)
                    {
                        long addr = result.address;
                        using var iter = log.Scan(addr, addr + 1, scanUncommitted: true);
                        if (iter.GetNext(out byte[] entry, out _, out _))
                        {
                            claimed.Add((kvp.Key, entry));
                        }
                        else
                        {
                            claimed.Add((kvp.Key, null));
                        }
                    }
                    else
                    {
                        claimed.Add((kvp.Key, null));
                    }
                    scanned++;
                }

                // Remove deleted entries from PEL
                foreach (var did in deletedIds)
                {
                    if (group.PEL.TryGetValue(did, out var dpe))
                    {
                        if (group.Consumers.TryGetValue(dpe.ConsumerName, out var dc))
                            dc.PendingIds.Remove(did);
                        group.PEL.Remove(did);
                    }
                }

                // Write response: [nextCursor, claimedEntries, deletedIds]
                writer.WriteArrayLength(3);

                // Next cursor (0-0 if scan complete)
                if (!hasMore)
                    nextCursor = new StreamID(0, 0);
                WriteStreamIdToWriter(nextCursor, ref writer);

                // Claimed entries
                writer.WriteArrayLength(claimed.Count);
                foreach (var (claimedId, data) in claimed)
                {
                    if (justId)
                    {
                        WriteStreamIdToWriter(claimedId, ref writer);
                    }
                    else if (data != null)
                    {
                        WriteEntryToWriter(data, ref writer);
                    }
                    else
                    {
                        writer.WriteNull();
                    }
                }

                // Deleted IDs
                writer.WriteArrayLength(deletedIds.Count);
                foreach (var did in deletedIds)
                {
                    WriteStreamIdToWriter(did, ref writer);
                }

                return true;
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Get stream info (XINFO STREAM).
        /// </summary>
        public void GetStreamInfo(ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                // Return as a flat array of field-value pairs (like Redis XINFO STREAM)
                writer.WriteArrayLength(14);

                writer.WriteBulkString("length"u8);
                writer.WriteInt64((long)index.ValidCount);

                writer.WriteBulkString("radix-tree-keys"u8);
                writer.WriteInt64(0); // N/A for BTree

                writer.WriteBulkString("radix-tree-nodes"u8);
                writer.WriteInt64(0); // N/A for BTree

                writer.WriteBulkString("last-generated-id"u8);
                WriteStreamIdToWriter(lastId, ref writer);

                writer.WriteBulkString("groups"u8);
                writer.WriteInt64(consumerGroups.Count);

                writer.WriteBulkString("entries-added"u8);
                writer.WriteInt64(totalEntriesAdded);

                writer.WriteBulkString("recorded-first-entry-id"u8);
                if (index.ValidCount > 0)
                {
                    var first = index.FirstAlive();
                    if (first.Value.Valid)
                        WriteStreamIdToWriter(MemoryMarshal.Read<StreamID>(first.Key.AsSpan()), ref writer);
                    else
                        WriteStreamIdToWriter(new StreamID(0, 0), ref writer);
                }
                else
                {
                    WriteStreamIdToWriter(new StreamID(0, 0), ref writer);
                }
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Get consumer groups info (XINFO GROUPS).
        /// </summary>
        public bool GetGroupsInfo(ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                writer.WriteArrayLength(consumerGroups.Count);
                foreach (var group in consumerGroups.Values)
                {
                    writer.WriteArrayLength(10);

                    writer.WriteBulkString("name"u8);
                    writer.WriteBulkString(System.Text.Encoding.UTF8.GetBytes(group.Name));

                    writer.WriteBulkString("consumers"u8);
                    writer.WriteInt64(group.Consumers.Count);

                    writer.WriteBulkString("pending"u8);
                    writer.WriteInt64(group.PEL.Count);

                    writer.WriteBulkString("last-delivered-id"u8);
                    WriteStreamIdToWriter(group.LastDeliveredId, ref writer);

                    writer.WriteBulkString("entries-read"u8);
                    writer.WriteInt64(group.EntriesRead);
                }
                return true;
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Get consumers info for a group (XINFO CONSUMERS).
        /// </summary>
        public bool GetConsumersInfo(string groupName, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            var writer = new RespMemoryWriter(respProtocolVersion, ref output);
            try
            {
                if (!consumerGroups.TryGetValue(groupName, out var group))
                    return false;

                long nowMs = NowMs();
                writer.WriteArrayLength(group.Consumers.Count);
                foreach (var consumer in group.Consumers.Values)
                {
                    writer.WriteArrayLength(6);

                    writer.WriteBulkString("name"u8);
                    writer.WriteBulkString(System.Text.Encoding.UTF8.GetBytes(consumer.Name));

                    writer.WriteBulkString("pending"u8);
                    writer.WriteInt64(consumer.PendingIds.Count);

                    writer.WriteBulkString("idle"u8);
                    writer.WriteInt64(nowMs - consumer.SeenTime);
                }
                return true;
            }
            finally
            {
                writer.Dispose();
            }
        }

        /// <summary>
        /// Read entries from the stream after the given ID (for XREAD, non-group).
        /// </summary>
        public unsafe int ReadAfter(StreamID afterId, int count, ref SpanByteAndMemory output, byte respProtocolVersion)
        {
            // Compute exclusive start: afterId + 1
            StreamID startID = afterId;
            ulong seq = startID.getSeq();
            if (seq < ulong.MaxValue)
            {
                startID.setSeq(seq + 1);
            }
            else
            {
                ulong ms = startID.getMS();
                if (ms < ulong.MaxValue)
                {
                    startID.setMS(ms + 1);
                    startID.setSeq(0);
                }
                else
                {
                    using var w = new RespMemoryWriter(respProtocolVersion, ref output);
                    w.WriteArrayLength(0);
                    return 0;
                }
            }

            StreamID endID = new StreamID(ulong.MaxValue, ulong.MaxValue);
            // Reuse ReadRange infrastructure
            string startStr = $"{startID.getMS()}-{startID.getSeq()}";
            string endStr = "+";
            return ReadRange(startStr, endStr, count, ref output, respProtocolVersion, isReverse: false);
        }

        #endregion Consumer Group Operations
    }
}