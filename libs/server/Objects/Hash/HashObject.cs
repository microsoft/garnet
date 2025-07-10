// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

    /// <summary>
    /// Operations on Hash
    /// </summary>
    public enum HashOperation : byte
    {
        HCOLLECT,
        HEXPIRE,
        HTTL,
        HPERSIST,
        HGET,
        HMGET,
        HSET,
        HMSET,
        HSETNX,
        HLEN,
        HDEL,
        HEXISTS,
        HGETALL,
        HKEYS,
        HVALS,
        HINCRBY,
        HINCRBYFLOAT,
        HRANDFIELD,
        HSCAN,
        HSTRLEN
    }


    /// <summary>
    ///  Hash Object Class
    /// </summary>
    public partial class HashObject : GarnetObjectBase
    {
        readonly Dictionary<byte[], byte[]> hash;
        Dictionary<byte[], long> expirationTimes;
        PriorityQueue<byte[], long> expirationQueue;

        // Byte #31 is used to denote if key has expiration (1) or not (0) 
        private const int ExpirationBitMask = 1 << 31;

        /// <summary>
        ///  Constructor
        /// </summary>
        public HashObject()
            : base(new(MemoryUtils.DictionaryOverhead, sizeof(int), serializedIsExact: true))
        {
            hash = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public HashObject(BinaryReader reader)
            : base(reader, new(MemoryUtils.DictionaryOverhead, sizeof(int), serializedIsExact: true))
        {
            hash = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);

            int count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
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
                this.UpdateSize(item, value, add: true);
            }
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        public HashObject(Dictionary<byte[], byte[]> hash, Dictionary<byte[], long> expirationTimes, PriorityQueue<byte[], long> expirationQueue, ObjectSizes sizes)
            : base(sizes)
        {
            this.hash = hash;
            this.expirationTimes = expirationTimes;
            this.expirationQueue = expirationQueue;
        }

        /// <inheritdoc />
        public override byte Type => (byte)GarnetObjectType.Hash;

        /// <inheritdoc />
        public override void DoSerialize(BinaryWriter writer)
        {
            base.DoSerialize(writer);

            DeleteExpiredItems();

            int count = hash.Count; // Since expired items are already deleted, no need to worry about expiring items
            writer.Write(count);
            foreach (var kvp in hash)
            {
                if (expirationTimes is not null && expirationTimes.TryGetValue(kvp.Key, out var expiration))
                {
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
        public override GarnetObjectBase Clone() => new HashObject(hash, expirationTimes, expirationQueue, sizes);

        /// <inheritdoc />
        public override bool Operate(ref ObjectInput input, ref GarnetObjectStoreOutput output,
                                     byte respProtocolVersion, out long memorySizeChange)
        {
            memorySizeChange = 0;

            if (input.header.type != GarnetObjectType.Hash)
            {
                //Indicates when there is an incorrect type 
                output.OutputFlags |= ObjectStoreOutputFlags.WrongType;
                output.SpanByteAndMemory.Length = 0;
                return true;
            }

            var previousMemorySize = this.HeapMemorySize;
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
                    HashIncrement(ref input, ref output, respProtocolVersion);
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

            memorySizeChange = this.HeapMemorySize - previousMemorySize;

            if (hash.Count == 0)
                output.OutputFlags |= ObjectStoreOutputFlags.RemoveKey;

            return true;
        }

        private void UpdateSize(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool add)
        {
            var memorySize = Utility.RoundUp(key.Length, IntPtr.Size) + Utility.RoundUp(value.Length, IntPtr.Size)
                + (2 * MemoryUtils.ByteArrayOverhead) + MemoryUtils.DictionaryEntryOverhead;

            // Expired items are removed before this is called or we're called after UpdateExpirationSize, so we don't adjust for expiration
            var kvSize = sizeof(int) * 2 + key.Length + value.Length;

            if (add)
            {
                this.HeapMemorySize += memorySize;
                this.SerializedSize += kvSize;
            }
            else
            {
                this.HeapMemorySize -= memorySize;
                this.SerializedSize -= kvSize;
                Debug.Assert(this.HeapMemorySize >= MemoryUtils.DictionaryOverhead);
                Debug.Assert(this.SerializedSize >= sizeof(int));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InitializeExpirationStructures()
        {
            if (expirationTimes is null)
            {
                expirationTimes = new Dictionary<byte[], long>(ByteArrayComparer.Instance);
                expirationQueue = new PriorityQueue<byte[], long>();
                this.HeapMemorySize += MemoryUtils.DictionaryOverhead + MemoryUtils.PriorityQueueOverhead;
                // No SerializedSize adjustment needed yet; wait until keys are added or removed
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
            {
                this.HeapMemorySize += memorySize;
                this.SerializedSize += sizeof(long);  // SerializedSize only needs to adjust the writing or not of the expiration value
            }
            else
            {
                this.HeapMemorySize -= memorySize;
                this.SerializedSize -= sizeof(long);  // SerializedSize only needs to adjust the writing or not of the expiration value
                Debug.Assert(this.HeapMemorySize >= MemoryUtils.DictionaryOverhead);
                Debug.Assert(this.SerializedSize >= sizeof(int));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CleanupExpirationStructuresIfEmpty()
        {
            if (expirationTimes.Count == 0)
            {
                this.HeapMemorySize -= (IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueOverhead) * expirationQueue.Count;
                this.HeapMemorySize -= MemoryUtils.DictionaryOverhead + MemoryUtils.PriorityQueueOverhead;
                this.SerializedSize -= sizeof(long) * expirationTimes.Count;
                expirationTimes = null;
                expirationQueue = null;
            }
        }

        /// <inheritdoc />
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0, bool isNoValue = false)
        {
            cursor = start;
            items = new List<byte[]>();

            if (hash.Count < start)
            {
                cursor = 0;
                return;
            }

            // Hashset has key and value, so count is multiplied by 2
            count = isNoValue ? count : count * 2;
            int index = 0;
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
        private bool IsExpired(byte[] key) => expirationTimes is not null && expirationTimes.TryGetValue(key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeleteExpiredItems()
        {
            if (expirationTimes is null)
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
                    if (hash.TryGetValue(key, out var value))
                    {
                        _ = hash.Remove(key);
                        UpdateSize(key, value, add: false);
                    }
                }
                else
                {
                    // The key was not in expirationTimes. It may have been Remove()d.
                    _ = expirationQueue.Dequeue();

                    // Adjust memory size for the priority queue entry removal. No DiskSize change needed as it was not in expirationTimes.
                    this.HeapMemorySize -= MemoryUtils.PriorityQueueEntryOverhead + IntPtr.Size + sizeof(long);
                }
            }

            CleanupExpirationStructuresIfEmpty();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryGetValue(byte[] key, out byte[] value)
        {
            value = default;
            if (IsExpired(key))
                return false;
            return hash.TryGetValue(key, out value);
        }

        private bool Remove(byte[] key, out byte[] value)
        {
            DeleteExpiredItems();
            var result = hash.Remove(key, out value);

            if (result)
            {
                if (HasExpirableItems())
                {
                    // We cannot remove from the PQ so just remove from expirationTimes, let the next call to DeleteExpiredItems() clean it up, and don't adjust PQ sizes.
                    _ = expirationTimes.Remove(key);
                    UpdateExpirationSize(add: false, includePQ: false);
                }
                UpdateSize(key, value, add: false);
            }
            return result;
        }

        private int Count()
        {
            if (!HasExpirableItems())
                return hash.Count;

            var expiredKeysCount = 0;
            foreach (var item in expirationTimes)
            {
                if (IsExpired(item.Key))
                    expiredKeysCount++;
            }
            return hash.Count - expiredKeysCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HasExpirableItems() => expirationTimes is not null;

        private bool ContainsKey(byte[] key)
        {
            var result = hash.ContainsKey(key);
            if (result && IsExpired(key))
                return false;
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Add(byte[] key, byte[] value)
        {
            DeleteExpiredItems();
            hash.Add(key, value);

            // Add() is not called in a context that includes expiration
            UpdateSize(key, value, add: true);
        }

        private void Set(byte[] key, byte[] value)
        {
            SetWithoutPersist(key, value);

            // Persist the key, if it has an expiration
            if (expirationTimes is not null && expirationTimes.Remove(key))
            {
                this.HeapMemorySize -= IntPtr.Size + sizeof(long) + MemoryUtils.DictionaryEntryOverhead;
                this.SerializedSize -= sizeof(long);  // expiration value size
                CleanupExpirationStructuresIfEmpty();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetWithoutPersist(byte[] key, byte[] value)
        {
            // Called only when we have verified the key exists
            DeleteExpiredItems();
            hash[key] = value;

            // Skip MemorySize adjustment as existing item is getting replaced.
        }

        private int SetExpiration(byte[] key, long expiration, ExpireOption expireOption)
        {
            if (!ContainsKey(key))
                return (int)ExpireResult.KeyNotFound;

            if (expiration <= DateTimeOffset.UtcNow.Ticks)
            {
                _ = Remove(key, out _);
                return (int)ExpireResult.KeyAlreadyExpired;
            }

            InitializeExpirationStructures();

            if (expirationTimes.TryGetValue(key, out var currentExpiration))
            {
                if ((expireOption & ExpireOption.NX) == ExpireOption.NX ||
                    ((expireOption & ExpireOption.GT) == ExpireOption.GT && expiration <= currentExpiration) ||
                    ((expireOption & ExpireOption.LT) == ExpireOption.LT && expiration >= currentExpiration))
                {
                    return (int)ExpireResult.ExpireConditionNotMet;
                }

                expirationTimes[key] = expiration;
                expirationQueue.Enqueue(key, expiration);

                // MemorySize of dictionary entry already accounted for as the key already exists.
                // SerializedSize of expiration already accounted for as the key already exists in expirationTimes.
                this.HeapMemorySize += IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueEntryOverhead;
            }
            else
            {
                if ((expireOption & ExpireOption.XX) == ExpireOption.XX || (expireOption & ExpireOption.GT) == ExpireOption.GT)
                    return (int)ExpireResult.ExpireConditionNotMet;

                expirationTimes[key] = expiration;
                expirationQueue.Enqueue(key, expiration);
                UpdateExpirationSize(add: true);
            }

            return (int)ExpireResult.ExpireUpdated;
        }

        private int Persist(byte[] key)
        {
            if (!ContainsKey(key))
                return -2;

            if (expirationTimes is not null && expirationTimes.TryGetValue(key, out _))
            {
                expirationTimes.Remove(key);
                this.HeapMemorySize -= IntPtr.Size + sizeof(long) + MemoryUtils.DictionaryEntryOverhead;
                this.SerializedSize -= sizeof(long);  // expiration value size
                CleanupExpirationStructuresIfEmpty();
                return 1;
            }

            return -1;
        }

        private long GetExpiration(byte[] key)
        {
            if (!ContainsKey(key))
                return -2;
            if (expirationTimes is not null && expirationTimes.TryGetValue(key, out var expiration))
                return expiration;
            return -1;
        }

        private KeyValuePair<byte[], byte[]> ElementAt(int index)
        {
            if (HasExpirableItems())
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

    enum ExpireResult
    {
        KeyNotFound = -2,
        ExpireConditionNotMet = 0,
        ExpireUpdated = 1,
        KeyAlreadyExpired = 2,
    }
}