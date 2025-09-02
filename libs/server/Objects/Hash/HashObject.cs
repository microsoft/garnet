﻿// Copyright (c) Microsoft Corporation.
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

#if NET9_0_OR_GREATER
using ByteSpan = System.ReadOnlySpan<byte>;
#else
using ByteSpan = byte[];
#endif

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

#if NET9_0_OR_GREATER
        private readonly Dictionary<byte[], byte[]>.AlternateLookup<ReadOnlySpan<byte>> hashSpanLookup;
        Dictionary<byte[], long>.AlternateLookup<ReadOnlySpan<byte>> expirationTimeSpanLookup;
#endif

        // Byte #31 is used to denote if key has expiration (1) or not (0) 
        private const int ExpirationBitMask = 1 << 31;

        private bool HasExpirableItems
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => expirationTimes is not null;
        }

        /// <summary>
        ///  Constructor
        /// </summary>
        public HashObject(long expiration = 0)
            : base(expiration, MemoryUtils.DictionaryOverhead)
        {
            hash = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);
#if NET9_0_OR_GREATER
            hashSpanLookup = hash.GetAlternateLookup<ReadOnlySpan<byte>>();
#endif
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public HashObject(BinaryReader reader)
            : base(reader, MemoryUtils.DictionaryOverhead)
        {
            var count = reader.ReadInt32();
            hash = new Dictionary<byte[], byte[]>(count, ByteArrayComparer.Instance);
#if NET9_0_OR_GREATER
            hashSpanLookup = hash.GetAlternateLookup<ReadOnlySpan<byte>>();
#endif
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
                        UpdateExpirationSize(item, true);
                    }
                }
                else
                {
                    hash.Add(item, value);
                }

                this.UpdateSize(item, value);
            }
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        public HashObject(Dictionary<byte[], byte[]> hash, Dictionary<byte[], long> expirationTimes, PriorityQueue<byte[], long> expirationQueue, long expiration, long size)
            : base(expiration, size)
        {
            this.hash = hash;
            this.expirationTimes = expirationTimes;
            this.expirationQueue = expirationQueue;
#if NET9_0_OR_GREATER
            hashSpanLookup = hash.GetAlternateLookup<ReadOnlySpan<byte>>();
#endif
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
                if (HasExpirableItems && expirationTimes.TryGetValue(kvp.Key, out var expiration))
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
        public override GarnetObjectBase Clone() => new HashObject(hash, expirationTimes, expirationQueue, Expiration, Size);

        /// <inheritdoc />
        public override bool Operate(ref ObjectInput input, ref GarnetObjectStoreOutput output,
                                     byte respProtocolVersion, out long sizeChange)
        {
            sizeChange = 0;

            if (input.header.type != GarnetObjectType.Hash)
            {
                //Indicates when there is an incorrect type 
                output.OutputFlags |= ObjectStoreOutputFlags.WrongType;
                output.SpanByteAndMemory.Length = 0;
                return true;
            }

            var previousSize = this.Size;
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

            sizeChange = this.Size - previousSize;

            if (hash.Count == 0)
                output.OutputFlags |= ObjectStoreOutputFlags.RemoveKey;

            return true;
        }

        private void UpdateSize(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool add = true)
        {
            var size = Utility.RoundUp(key.Length, IntPtr.Size) + Utility.RoundUp(value.Length, IntPtr.Size)
                + (2 * MemoryUtils.ByteArrayOverhead) + MemoryUtils.DictionaryEntryOverhead;
            this.Size += add ? size : -size;
            Debug.Assert(this.Size >= MemoryUtils.DictionaryOverhead);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InitializeExpirationStructures()
        {
            if (!HasExpirableItems)
            {
                expirationTimes = new Dictionary<byte[], long>(ByteArrayComparer.Instance);
                expirationQueue = new PriorityQueue<byte[], long>();
#if NET9_0_OR_GREATER
                expirationTimeSpanLookup = expirationTimes.GetAlternateLookup<ReadOnlySpan<byte>>();
#endif
                this.Size += MemoryUtils.DictionaryOverhead + MemoryUtils.PriorityQueueOverhead;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateExpirationSize(ReadOnlySpan<byte> key, bool add = true)
        {
            // Account for dictionary entry and priority queue entry
            var size = IntPtr.Size + sizeof(long) + MemoryUtils.DictionaryEntryOverhead
                + IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueEntryOverhead;
            this.Size += add ? size : -size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CleanupExpirationStructures()
        {
            if (expirationTimes.Count == 0)
            {
                this.Size -= (IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueOverhead) * expirationQueue.Count;
                this.Size -= MemoryUtils.DictionaryOverhead + MemoryUtils.PriorityQueueOverhead;
                expirationTimes = null;
                expirationQueue = null;
#if NET9_0_OR_GREATER
                expirationTimeSpanLookup = default;
#endif
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
                    {
                        items.Add(item.Value);
                    }
                }
                else
                {
                    fixed (byte* keyPtr = item.Key)
                    {
                        if (GlobUtils.Match(pattern, patternLength, keyPtr, item.Key.Length))
                        {
                            items.Add(item.Key);
                            if (!isNoValue)
                            {
                                items.Add(item.Value);
                            }
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

#if NET9_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsExpired(ReadOnlySpan<byte> key) => HasExpirableItems && expirationTimeSpanLookup.TryGetValue(key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks;
#else
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsExpired(byte[] key) => HasExpirableItems && expirationTimes.TryGetValue(key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks;
#endif

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeleteExpiredItems()
        {
            if (!HasExpirableItems)
                return;
            DeleteExpiredItemsWorker();
        }

        private void DeleteExpiredItemsWorker()
        {
            while (expirationQueue.TryPeek(out var key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks)
            {
                // expirationTimes and expirationQueue will be out of sync when user is updating the expire time of key which already has some TTL.
                // PriorityQueue Doesn't have update option, so we will just enqueue the new expiration and already treat expirationTimes as the source of truth
                if (expirationTimes.TryGetValue(key, out var actualExpiration) && actualExpiration == expiration)
                {
                    expirationTimes.Remove(key);
                    expirationQueue.Dequeue();
                    UpdateExpirationSize(key, false);
                    if (hash.Remove(key, out var value))
                    {
                        UpdateSize(key, value, false);
                    }
                }
                else
                {
                    expirationQueue.Dequeue();
                    this.Size -= MemoryUtils.PriorityQueueEntryOverhead + IntPtr.Size + sizeof(long);
                }
            }

            CleanupExpirationStructures();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryGetValue(ByteSpan key, out byte[] value)
        {
            value = default;
            if (IsExpired(key))
            {
                return false;
            }

#if NET9_0_OR_GREATER
            return hashSpanLookup.TryGetValue(key, out value);
#else
            return hash.TryGetValue(key, out value);
#endif
        }

        private bool Remove(ByteSpan key, out byte[] value)
        {
            DeleteExpiredItems();
#if NET9_0_OR_GREATER
            var result = hashSpanLookup.Remove(key, out _, out value);
#else
            var result = hash.Remove(key, out value);
#endif
            if (result)
            {
                UpdateSize(key, value, false);
            }
            return result;
        }

        private int Count()
        {
            if (!HasExpirableItems)
            {
                return hash.Count;
            }

            var expiredKeysCount = 0;
            foreach (var item in expirationTimes)
            {
                if (IsExpired(item.Key))
                {
                    expiredKeysCount++;
                }
            }

            return hash.Count - expiredKeysCount;
        }

        private bool ContainsKey(ByteSpan key)
        {
#if NET9_0_OR_GREATER
            var result = hashSpanLookup.ContainsKey(key);
#else
            var result = hash.ContainsKey(key);
#endif
            if (result && IsExpired(key))
            {
                return false;
            }

            return result;
        }

#if NET9_0_OR_GREATER
        private bool ContainsKey(ByteSpan key, out byte[] keyArray)
        {
            var result = hashSpanLookup.TryGetValue(key, out keyArray, out _);
            if (result && IsExpired(key))
            {
                return false;
            }

            return result;
        }
#endif

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Add(ByteSpan key, byte[] value)
        {
            DeleteExpiredItems();
#if NET9_0_OR_GREATER
            var success = hashSpanLookup.TryAdd(key, value);
#else
            var success = hash.TryAdd(key, value);
#endif
            Debug.Assert(success);

            UpdateSize(key, value);
        }

        private ExpireResult SetExpiration(ByteSpan key, long expiration, ExpireOption expireOption)
        {
#if NET9_0_OR_GREATER
            if (!ContainsKey(key, out var keyArray))
#else
            if (!ContainsKey(key))
#endif
            {
                return ExpireResult.KeyNotFound;
            }

            if (expiration <= DateTimeOffset.UtcNow.Ticks)
            {
                Remove(key, out _);
                return ExpireResult.KeyAlreadyExpired;
            }

            InitializeExpirationStructures();

            // Avoid multiple hash calculations by acquiring ref to the dictionary value.
            // The ref is unsafe to read/write to if the expiration dictionary is mutated.
            ref var expirationTimeRef =
#if NET9_0_OR_GREATER
                ref CollectionsMarshal.GetValueRefOrAddDefault(expirationTimeSpanLookup, key, out var exists);
#else
                ref CollectionsMarshal.GetValueRefOrAddDefault(expirationTimes, key, out var exists);
#endif
            if (exists)
            {
                if ((expireOption & ExpireOption.NX) == ExpireOption.NX ||
                    ((expireOption & ExpireOption.GT) == ExpireOption.GT && expiration <= expirationTimeRef) ||
                    ((expireOption & ExpireOption.LT) == ExpireOption.LT && expiration >= expirationTimeRef))
                {
                    return ExpireResult.ExpireConditionNotMet;
                }

                expirationTimeRef = expiration;
#if NET9_0_OR_GREATER
                expirationQueue.Enqueue(keyArray, expiration);
#else
                expirationQueue.Enqueue(key, expiration);
#endif
                // Size of dictionary entry already accounted for as the key already exists
                this.Size += IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueEntryOverhead;
            }
            else
            {
                if ((expireOption & ExpireOption.XX) == ExpireOption.XX ||
                    (expireOption & ExpireOption.GT) == ExpireOption.GT)
                {
                    return ExpireResult.ExpireConditionNotMet;
                }

                expirationTimeRef = expiration;
#if NET9_0_OR_GREATER
                expirationQueue.Enqueue(keyArray, expiration);
#else
                expirationQueue.Enqueue(key, expiration);
#endif
                UpdateExpirationSize(key);
            }

            return ExpireResult.ExpireUpdated;
        }

        private int Persist(ByteSpan key)
        {
            if (!ContainsKey(key))
            {
                return (int)ExpireResult.KeyNotFound;
            }

#if NET9_0_OR_GREATER
            if (HasExpirableItems && expirationTimeSpanLookup.Remove(key))
#else
            if (HasExpirableItems && expirationTimes.Remove(key, out var currentExpiration))
#endif
            {
                this.Size -= IntPtr.Size + sizeof(long) + MemoryUtils.DictionaryEntryOverhead;
                CleanupExpirationStructures();
                return (int)ExpireResult.ExpireUpdated;
            }

            return -1;
        }

        private long GetExpiration(ByteSpan key)
        {
            if (!ContainsKey(key))
            {
                return (long)ExpireResult.KeyNotFound;
            }

#if NET9_0_OR_GREATER
            if (HasExpirableItems && expirationTimeSpanLookup.TryGetValue(key, out var expiration))
#else
            if (HasExpirableItems && expirationTimes.TryGetValue(key, out var expiration))
#endif
            {
                return expiration;
            }

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
                    {
                        continue;
                    }

                    if (currIndex++ == index)
                    {
                        return item;
                    }
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