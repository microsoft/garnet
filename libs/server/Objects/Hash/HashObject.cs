// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Tsavorite.core;

using ByteSpan = System.ReadOnlySpan<byte>;

namespace Garnet.server
{
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

    /// <summary>
    /// Operations on Hash.
    /// Persisted in the AOF via RespInputHeader.SubId, so values are EXPLICIT and APPEND-ONLY:
    /// never change/reorder an existing value; add new operations at the end with the next value.
    /// </summary>
    public enum HashOperation : byte
    {
        HCOLLECT = 0,
        HEXPIRE = 1,
        HTTL = 2,
        HPERSIST = 3,
        HGET = 4,
        HMGET = 5,
        HSET = 6,
        HMSET = 7,
        HSETNX = 8,
        HLEN = 9,
        HDEL = 10,
        HEXISTS = 11,
        HGETALL = 12,
        HKEYS = 13,
        HVALS = 14,
        HINCRBY = 15,
        HINCRBYFLOAT = 16,
        HRANDFIELD = 17,
        HSCAN = 18,
        HSTRLEN = 19,
    }


    /// <summary>
    ///  Hash Object Class
    /// </summary>
    public partial class HashObject : GarnetObjectBase
    {
        readonly Dictionary<byte[], byte[]> hash;

        // Expiration state for fields with a TTL. Both structures are null until the first TTL is set, are allocated
        // together by InitializeExpirationStructures, and are torn back down together by
        // CleanupExpirationStructuresIfEmpty once no field has a TTL. HasExpirableItems tests expirationTimes and
        // guards every access to either of them.

        // The expiration time, in UTC ticks, of each field that has one, and the source of truth for whether a field
        // is expired. A field absent from this dictionary never expires.
        Dictionary<byte[], long> expirationTimes;

        // The same expirations ordered soonest-first, so lazy cleanup only has to inspect the head of the queue.
        // PriorityQueue has no update operation, so resetting a field's TTL enqueues a second entry and leaves the
        // stale one behind, and removing a field leaves its entry behind entirely. Entries are therefore only hints:
        // DeleteExpiredItemsWorker re-checks each one against expirationTimes and discards it if the field is gone or
        // now carries a different expiration.
        PriorityQueue<byte[], long> expirationQueue;

        private readonly Dictionary<byte[], byte[]>.AlternateLookup<ReadOnlySpan<byte>> hashSpanLookup;

        // View of expirationTimes keyed by ReadOnlySpan<byte>, so a lookup does not have to allocate a byte[]. Follows
        // the lifetime of expirationTimes and is recreated and cleared alongside it.
        Dictionary<byte[], long>.AlternateLookup<ReadOnlySpan<byte>> expirationTimeSpanLookup;

        // Byte #31 is used to denote if key has expiration (1) or not (0) 
        private const int ExpirationBitMask = 1 << 31;

        internal bool HasExpirableItems
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => expirationTimes is not null;
        }

        /// <summary>
        ///  Constructor
        /// </summary>
        public HashObject()
            : base(MemoryUtils.DictionaryOverhead)
        {
            hash = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);
            hashSpanLookup = hash.GetAlternateLookup<ReadOnlySpan<byte>>();
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public HashObject(BinaryReader reader)
            : base(reader, MemoryUtils.DictionaryOverhead)
        {
            var count = reader.ReadInt32();
            hash = new Dictionary<byte[], byte[]>(count, ByteArrayComparer.Instance);
            hashSpanLookup = hash.GetAlternateLookup<ReadOnlySpan<byte>>();
            for (var i = 0; i < count; i++)
            {
                var keyLength = reader.ReadInt32();
                var hasExpiration = (keyLength & ExpirationBitMask) != 0;
                keyLength &= ~ExpirationBitMask;
                var item = reader.ReadBytes(keyLength);
                var value = reader.ReadBytes(reader.ReadInt32());

                if (hasExpiration)
                {
                    var expiration = reader.ReadInt64();
                    var isExpired = expiration < DateTimeOffset.UtcNow.Ticks;
                    if (!isExpired)
                    {
                        hash.Add(item, value);
                        InitializeExpirationStructures();
                        expirationTimes.Add(item, expiration);
                        expirationQueue.Enqueue(item, expiration);
                        UpdateExpirationSize(add: true);
                    }
                }
                else
                {
                    hash.Add(item, value);
                }

                // Expiration has already been added via UpdateExpirationSize if hasExpiration
                UpdateSize(item, value, add: true);
            }
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        public HashObject(Dictionary<byte[], byte[]> hash, Dictionary<byte[], long> expirationTimes, PriorityQueue<byte[], long> expirationQueue, long heapMemorySize)
            : base(heapMemorySize)
        {
            this.hash = hash;
            this.expirationTimes = expirationTimes;
            this.expirationQueue = expirationQueue;
            hashSpanLookup = hash.GetAlternateLookup<ReadOnlySpan<byte>>();
            if (expirationTimes is not null)
                expirationTimeSpanLookup = expirationTimes.GetAlternateLookup<ReadOnlySpan<byte>>();
        }

        /// <inheritdoc />
        public override byte Type => (byte)GarnetObjectType.Hash;

        /// <inheritdoc />
        /// <remarks>
        /// Serialization must not mutate the object. It runs on the flush path while readers may concurrently access
        /// the same instance, and only writers are excluded from a record that is being serialized. Expired fields are
        /// therefore skipped rather than deleted; the mutating paths remove them from the live object.
        /// </remarks>
        public override void DoSerialize(BinaryWriter writer)
        {
            base.DoSerialize(writer);

            // Both passes share a single timestamp so they agree on exactly which fields are expired; otherwise a
            // field could expire between them and the declared count would not match the entries written.
            var now = DateTimeOffset.UtcNow.Ticks;
            var expirations = expirationTimes;

            var count = hash.Count;
            if (expirations is not null)
            {
                count = 0;
                foreach (var kvp in hash)
                {
                    if (!expirations.TryGetValue(kvp.Key, out var expiration) || expiration >= now)
                        count++;
                }
            }

            writer.Write(count);
            foreach (var kvp in hash)
            {
                if (expirations is not null && expirations.TryGetValue(kvp.Key, out var expiration))
                {
                    if (expiration < now)
                        continue;

                    writer.Write(kvp.Key.Length | ExpirationBitMask);
                    writer.Write(kvp.Key);
                    writer.Write(kvp.Value.Length);
                    writer.Write(kvp.Value);
                    writer.Write(expiration);
                    count--;
                    continue;
                }

                writer.Write(kvp.Key.Length);
                writer.Write(kvp.Key);
                writer.Write(kvp.Value.Length);
                writer.Write(kvp.Value);
                count--;
            }

            Debug.Assert(count == 0);
        }

        /// <inheritdoc />
        public override void Dispose() { }

        /// <inheritdoc />
        public override GarnetObjectBase Clone() => new HashObject(hash, expirationTimes, expirationQueue, HeapMemorySize);

        /// <inheritdoc />
        public override bool Operate(ref ObjectInput input, ref ObjectOutput output, byte respProtocolVersion)
        {
            if (input.header.type != GarnetObjectType.Hash)
            {
                //Indicates when there is an incorrect type 
                output.OutputFlags |= ObjectOutputFlags.WrongType;
                output.SpanByteAndMemory.Length = 0;
                return true;
            }

            switch (input.header.HashOp)
            {
                case HashOperation.HSET:
                    HashSet(ref input, ref output);
                    break;
                case HashOperation.HMSET:
                    HashSet(ref input, ref output);
                    break;
                case HashOperation.HGET:
                    HashGet(ref input, ref output, respProtocolVersion);
                    break;
                case HashOperation.HMGET:
                    HashMultipleGet(ref input, ref output, respProtocolVersion);
                    break;
                case HashOperation.HGETALL:
                    HashGetAll(ref output, respProtocolVersion);
                    break;
                case HashOperation.HDEL:
                    HashDelete(ref input, ref output);
                    break;
                case HashOperation.HLEN:
                    HashLength(ref output);
                    break;
                case HashOperation.HSTRLEN:
                    HashStrLength(ref input, ref output);
                    break;
                case HashOperation.HEXISTS:
                    HashExists(ref input, ref output);
                    break;
                case HashOperation.HEXPIRE:
                    HashExpire(ref input, ref output, respProtocolVersion);
                    break;
                case HashOperation.HTTL:
                    HashTimeToLive(ref input, ref output, respProtocolVersion);
                    break;
                case HashOperation.HPERSIST:
                    HashPersist(ref input, ref output, respProtocolVersion);
                    break;
                case HashOperation.HKEYS:
                    HashGetKeysOrValues(ref input, ref output, respProtocolVersion);
                    break;
                case HashOperation.HVALS:
                    HashGetKeysOrValues(ref input, ref output, respProtocolVersion);
                    break;
                case HashOperation.HINCRBY:
                    HashIncrement(ref input, ref output, respProtocolVersion);
                    break;
                case HashOperation.HINCRBYFLOAT:
                    HashIncrementFloat(ref input, ref output, respProtocolVersion);
                    break;
                case HashOperation.HSETNX:
                    HashSet(ref input, ref output);
                    break;
                case HashOperation.HRANDFIELD:
                    HashRandomField(ref input, ref output, respProtocolVersion);
                    break;
                case HashOperation.HCOLLECT:
                    HashCollect(ref input, ref output);
                    break;
                case HashOperation.HSCAN:
                    Scan(ref input, ref output, respProtocolVersion);
                    break;
                default:
                    throw new GarnetException($"Unsupported operation {input.header.HashOp} in HashObject.Operate");
            }

            if (hash.Count == 0)
                output.OutputFlags |= ObjectOutputFlags.RemoveKey;

            return true;
        }

        private void UpdateSize(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool add)
        {
            var memorySize = Utility.RoundUp(key.Length, IntPtr.Size) + Utility.RoundUp(value.Length, IntPtr.Size)
                + (2 * MemoryUtils.ByteArrayOverhead) + MemoryUtils.DictionaryEntryOverhead;

            if (add)
                HeapMemorySize += memorySize;
            else
            {
                HeapMemorySize -= memorySize;
                Debug.Assert(HeapMemorySize >= MemoryUtils.DictionaryOverhead);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InitializeExpirationStructures()
        {
            if (!HasExpirableItems)
            {
                expirationTimes = new Dictionary<byte[], long>(ByteArrayComparer.Instance);
                expirationQueue = new PriorityQueue<byte[], long>();
                expirationTimeSpanLookup = expirationTimes.GetAlternateLookup<ReadOnlySpan<byte>>();
                HeapMemorySize += MemoryUtils.DictionaryOverhead + MemoryUtils.PriorityQueueOverhead;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateExpirationSize(bool add, bool includePQ = true)
        {
            // Account for dictionary entry and priority queue entry
            var memorySize = IntPtr.Size + sizeof(long) + MemoryUtils.DictionaryEntryOverhead;
            if (includePQ)
                memorySize += IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueEntryOverhead;

            if (add)
                HeapMemorySize += memorySize;
            else
            {
                HeapMemorySize -= memorySize;
                Debug.Assert(this.HeapMemorySize >= MemoryUtils.DictionaryOverhead);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CleanupExpirationStructuresIfEmpty()
        {
            if (expirationTimes.Count == 0)
            {
                HeapMemorySize -= (IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueEntryOverhead) * expirationQueue.Count;
                HeapMemorySize -= MemoryUtils.DictionaryOverhead + MemoryUtils.PriorityQueueOverhead;
                expirationTimes = null;
                expirationQueue = null;
                expirationTimeSpanLookup = default;
            }
        }

        /// <inheritdoc />
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0, bool isNoValue = false)
        {
            cursor = start;
            items = [];

            if (hash.Count < start)
            {
                cursor = 0;
                return;
            }

            // Hashset has key and value, so count is multiplied by 2
            count = isNoValue ? count : count * 2;
            var index = 0;
            var expiredKeysCount = 0;
            foreach (var item in hash)
            {
                if (IsExpired(item.Key))
                {
                    expiredKeysCount++;
                    continue;
                }

                if (index < start)
                {
                    index++;
                    continue;
                }

                if (patternLength == 0)
                {
                    items.Add(item.Key);
                    if (!isNoValue)
                        items.Add(item.Value);
                }
                else
                {
                    fixed (byte* keyPtr = item.Key)
                    {
                        if (GlobUtils.Match(pattern, patternLength, keyPtr, item.Key.Length))
                        {
                            items.Add(item.Key);
                            if (!isNoValue)
                                items.Add(item.Value);
                        }
                    }
                }

                cursor++;

                if (items.Count == count)
                    break;
            }

            // Indicates end of collection has been reached.
            if (cursor + expiredKeysCount == hash.Count)
                cursor = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsExpired(ReadOnlySpan<byte> key) => HasExpirableItems && expirationTimeSpanLookup.TryGetValue(key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeleteExpiredItems()
        {
            if (!HasExpirableItems)
                return;
            DeleteExpiredItemsWorker();
        }

        private void DeleteExpiredItemsWorker()
        {
            // The PQ is ordered such that oldest items are dequeued first
            while (expirationQueue.TryPeek(out var key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks)
            {
                // expirationTimes and expirationQueue will be out of sync when user is updating the expire time of key which already has some TTL.
                // PriorityQueue Doesn't have update option, so we will just enqueue the new expiration and already treat expirationTimes as the source of truth
                if (expirationTimes.TryGetValue(key, out var actualExpiration) && actualExpiration == expiration)
                {
                    _ = expirationTimes.Remove(key);
                    _ = expirationQueue.Dequeue();
                    UpdateExpirationSize(add: false);
                    if (hash.Remove(key, out var value))
                        UpdateSize(key, value, add: false);
                }
                else
                {
                    // The key was not in expirationTimes. It may have been Remove()d.
                    _ = expirationQueue.Dequeue();

                    // Adjust memory size for the priority queue entry removal. No DiskSize change needed as it was not in expirationTimes.
                    HeapMemorySize -= MemoryUtils.PriorityQueueEntryOverhead + IntPtr.Size + sizeof(long);
                }
            }

            CleanupExpirationStructuresIfEmpty();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryGetValue(ByteSpan key, out byte[] value)
        {
            value = default;
            if (IsExpired(key))
                return false;

            return hashSpanLookup.TryGetValue(key, out value);
        }

        private bool Remove(ByteSpan key, out byte[] value)
        {
            DeleteExpiredItems();
            var result = hashSpanLookup.Remove(key, out _, out value);
            if (result)
            {
                if (HasExpirableItems)
                {
                    // We cannot remove from the PQ so just remove from expirationTimes, let the next call to DeleteExpiredItems() clean it up, and don't adjust PQ sizes.
                    _ = expirationTimeSpanLookup.Remove(key);
                    UpdateExpirationSize(add: false, includePQ: false);
                }
                UpdateSize(key, value, add: false);
            }
            return result;
        }

        private int Count()
        {
            if (!HasExpirableItems)
                return hash.Count;

            var expiredKeysCount = 0;
            foreach (var item in expirationTimes)
            {
                if (IsExpired(item.Key))
                    expiredKeysCount++;
            }
            return hash.Count - expiredKeysCount;
        }

        private bool ContainsKey(ByteSpan key)
        {
            var result = hashSpanLookup.ContainsKey(key);
            if (result && IsExpired(key))
                return false;
            return result;
        }

        private bool ContainsKey(ByteSpan key, out byte[] keyArray)
        {
            var result = hashSpanLookup.TryGetValue(key, out keyArray, out _);
            if (result && IsExpired(key))
                return false;

            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Add(ByteSpan key, byte[] value)
        {
            // Called only when we have verified the key exists
            DeleteExpiredItems();
            var success = hashSpanLookup.TryAdd(key, value);
            Debug.Assert(success);

            UpdateSize(key, value, add: true);
        }

        private ExpireResult SetExpiration(ByteSpan key, long expiration, ExpireOption expireOption)
        {
            if (!ContainsKey(key, out var keyArray))
                return ExpireResult.KeyNotFound;

            if (expiration <= DateTimeOffset.UtcNow.Ticks)
            {
                _ = Remove(key, out _);
                return ExpireResult.KeyAlreadyExpired;
            }

            InitializeExpirationStructures();

            // Avoid multiple hash calculations by acquiring ref to the dictionary value.
            // The ref is unsafe to read/write to if the expiration dictionary is mutated.
            ref var expirationTimeRef =
                ref CollectionsMarshal.GetValueRefOrAddDefault(expirationTimeSpanLookup, key, out var exists);
            if (exists)
            {
                if ((expireOption & ExpireOption.NX) == ExpireOption.NX ||
                    ((expireOption & ExpireOption.GT) == ExpireOption.GT && expiration <= expirationTimeRef) ||
                    ((expireOption & ExpireOption.LT) == ExpireOption.LT && expiration >= expirationTimeRef))
                {
                    return ExpireResult.ExpireConditionNotMet;
                }

                expirationTimeRef = expiration;
                expirationQueue.Enqueue(keyArray, expiration);

                // LogMemorySize of dictionary entry already accounted for as the key already exists.
                // SerializedSize of expiration is already accounted for as the key already exists in expirationTimes.
                HeapMemorySize += IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueEntryOverhead;
            }
            else
            {
                if ((expireOption & ExpireOption.XX) == ExpireOption.XX || (expireOption & ExpireOption.GT) == ExpireOption.GT)
                    return ExpireResult.ExpireConditionNotMet;

                expirationTimeRef = expiration;
                expirationQueue.Enqueue(keyArray, expiration);
                UpdateExpirationSize(add: true, includePQ: true);
            }

            return ExpireResult.ExpireUpdated;
        }

        private int Persist(ByteSpan key)
        {
            if (!ContainsKey(key))
            {
                return (int)ExpireResult.KeyNotFound;
            }

            if (HasExpirableItems && expirationTimeSpanLookup.Remove(key))
            {
                HeapMemorySize -= IntPtr.Size + sizeof(long) + MemoryUtils.DictionaryEntryOverhead;
                CleanupExpirationStructuresIfEmpty();
                return (int)ExpireResult.ExpireUpdated;
            }

            return -1;
        }

        private long GetExpiration(ByteSpan key)
        {
            if (!ContainsKey(key))
                return (long)ExpireResult.KeyNotFound;

            if (HasExpirableItems && expirationTimeSpanLookup.TryGetValue(key, out var expiration))
                return expiration;
            return -1;
        }

        private KeyValuePair<byte[], byte[]> ElementAt(int index)
        {
            if (HasExpirableItems)
            {
                var currIndex = 0;
                foreach (var item in hash)
                {
                    if (IsExpired(item.Key))
                        continue;

                    if (currIndex++ == index)
                        return item;
                }

                throw new ArgumentOutOfRangeException("index is outside the bounds of the source sequence.");
            }

            return hash.ElementAt(index);
        }
    }

    enum ExpireResult : int
    {
        KeyNotFound = -2,
        ExpireConditionNotMet = 0,
        ExpireUpdated = 1,
        KeyAlreadyExpired = 2,
    }
}