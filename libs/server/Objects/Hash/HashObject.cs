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
    public unsafe partial class HashObject : GarnetObjectBase
    {
        private readonly Dictionary<byte[], byte[]> hash;
        private Dictionary<byte[], long> expirationTimes;
        private PriorityQueue<byte[], long> expirationQueue;

#if NET9_0_OR_GREATER
        private readonly Dictionary<byte[], byte[]>.AlternateLookup<ReadOnlySpan<byte>> hashLookup;
        private Dictionary<byte[], long>.AlternateLookup<ReadOnlySpan<byte>> expirationLookup;
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
            hashLookup = hash.GetAlternateLookup<ReadOnlySpan<byte>>();
#endif
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public HashObject(BinaryReader reader)
            : base(reader, MemoryUtils.DictionaryOverhead)
        {
            int count = reader.ReadInt32();

            hash = new Dictionary<byte[], byte[]>(count, ByteArrayComparer.Instance);
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
                        UpdateExpirationSize(item, true);
                    }
                }
                else
                {
                    hash.Add(item, value);
                }

                this.UpdateSize(item, value);
            }

#if NET9_0_OR_GREATER
            hashLookup = hash.GetAlternateLookup<ReadOnlySpan<byte>>();
#endif
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
        public override GarnetObjectBase Clone() => new HashObject(hash, expirationTimes, expirationQueue, Expiration, Size);

        /// <inheritdoc />
        public override unsafe bool Operate(ref ObjectInput input, ref GarnetObjectStoreOutput output, out long sizeChange)
        {
            sizeChange = 0;

            fixed (byte* outputSpan = output.SpanByteAndMemory.SpanByte.AsSpan())
            {
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
                        HashSet(ref input, outputSpan);
                        break;
                    case HashOperation.HMSET:
                        HashSet(ref input, outputSpan);
                        break;
                    case HashOperation.HGET:
                        HashGet(ref input, ref output.SpanByteAndMemory);
                        break;
                    case HashOperation.HMGET:
                        HashMultipleGet(ref input, ref output.SpanByteAndMemory);
                        break;
                    case HashOperation.HGETALL:
                        HashGetAll(ref input, ref output.SpanByteAndMemory);
                        break;
                    case HashOperation.HDEL:
                        HashDelete(ref input, outputSpan);
                        break;
                    case HashOperation.HLEN:
                        HashLength(outputSpan);
                        break;
                    case HashOperation.HSTRLEN:
                        HashStrLength(ref input, outputSpan);
                        break;
                    case HashOperation.HEXISTS:
                        HashExists(ref input, outputSpan);
                        break;
                    case HashOperation.HEXPIRE:
                        HashExpire(ref input, ref output.SpanByteAndMemory);
                        break;
                    case HashOperation.HTTL:
                        HashTimeToLive(ref input, ref output.SpanByteAndMemory);
                        break;
                    case HashOperation.HPERSIST:
                        HashPersist(ref input, ref output.SpanByteAndMemory);
                        break;
                    case HashOperation.HKEYS:
                        HashGetKeysOrValues(ref input, ref output.SpanByteAndMemory);
                        break;
                    case HashOperation.HVALS:
                        HashGetKeysOrValues(ref input, ref output.SpanByteAndMemory);
                        break;
                    case HashOperation.HINCRBY:
                        HashIncrement(ref input, ref output.SpanByteAndMemory);
                        break;
                    case HashOperation.HINCRBYFLOAT:
                        HashIncrement(ref input, ref output.SpanByteAndMemory);
                        break;
                    case HashOperation.HSETNX:
                        HashSet(ref input, outputSpan);
                        break;
                    case HashOperation.HRANDFIELD:
                        HashRandomField(ref input, ref output.SpanByteAndMemory);
                        break;
                    case HashOperation.HCOLLECT:
                        HashCollect(ref input, outputSpan);
                        break;
                    case HashOperation.HSCAN:
                        if (ObjectUtils.ReadScanInput(ref input, ref output.SpanByteAndMemory, out var cursorInput, out var pattern,
                                out var patternLength, out var limitCount, out var isNoValue, out var error))
                        {
                            Scan(cursorInput, out var items, out var cursorOutput, count: limitCount, pattern: pattern,
                                patternLength: patternLength, isNoValue);
                            ObjectUtils.WriteScanOutput(items, cursorOutput, ref output.SpanByteAndMemory);
                        }
                        else
                        {
                            ObjectUtils.WriteScanError(error, ref output.SpanByteAndMemory);
                        }
                        break;
                    default:
                        throw new GarnetException($"Unsupported operation {input.header.HashOp} in HashObject.Operate");
                }

                sizeChange = this.Size - previousSize;
            }

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
            if (expirationTimes is null)
            {
                expirationTimes = new Dictionary<byte[], long>(ByteArrayComparer.Instance);
#if NET9_0_OR_GREATER
                expirationLookup = expirationTimes.GetAlternateLookup<ReadOnlySpan<byte>>();
#endif

                expirationQueue = new PriorityQueue<byte[], long>();
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
                expirationLookup = default;
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

        /// <summary>
        /// Checks whether the <paramref name="key"/> has expired.
        /// </summary>
        /// <param name="key">The element to remove.</param>
        /// <returns><see langword="true"/> if the <paramref name="key"/> is found and has expired; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if NET9_0_OR_GREATER
        private bool IsExpired(ReadOnlySpan<byte> key) => HasExpirableItems && expirationLookup.TryGetValue(key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks;
#else
        private bool IsExpired(byte[] key) => HasExpirableItems && expirationTimes.TryGetValue(key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks;
#endif

        private void DeleteExpiredItems()
        {
            if (expirationTimes is null)
                return;

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

        private bool TryGetValue(byte[] key, out byte[] value)
        {
            value = default;
            if (IsExpired(key))
            {
                return false;
            }
            return hash.TryGetValue(key, out value);
        }

#if NET9_0_OR_GREATER
        private bool TryGetValue(ReadOnlySpan<byte> key, out byte[] value)
        {
            value = default;
            if (IsExpired(key))
            {
                return false;
            }
            return hashLookup.TryGetValue(key, out value);
        }
#endif

        private bool Remove(byte[] key, out byte[] value)
        {
            DeleteExpiredItems();
            var result = hash.Remove(key, out value);
            if (result)
            {
                UpdateSize(key, value, false);
            }
            return result;
        }

        /// <summary>
        /// Checks whether the <paramref name="key"/> has expired.
        /// </summary>
        /// <remarks>
        /// On .NET 8, this method copies the <paramref name="key"/> to a new array in order to perform the removal.
        /// </remarks>
        /// <param name="key">The key.</param>
        /// <returns><see langword="true"/> if the <paramref name="key"/> is found and has expired; otherwise, <see langword="false"/>.</returns>
        private bool Remove(ReadOnlySpan<byte> key, out byte[] value)
        {
            DeleteExpiredItems();

#if NET9_0_OR_GREATER
            var result = hashLookup.Remove(key, out _, out value);
#else
            var result = hash.Remove(key.ToArray(), out value);
#endif
            if (result)
            {
                UpdateSize(key, value, false);
            }
            return result;
        }

        private int Count()
        {
            if (expirationTimes is null)
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

#if NET9_0_OR_GREATER
        /// <summary>
        /// Checks whether the <paramref name="key"/> exists and if it does, that it has not expired.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns><see langword="true"/> if the <paramref name="key"/> is found and has not expired; otherwise, <see langword="false"/>.</returns>
        private bool ContainsKey(ReadOnlySpan<byte> key) => hashLookup.ContainsKey(key) && !IsExpired(key);
#endif

        /// <summary>
        /// Checks whether the <paramref name="key"/> exists and if it does, that it has not expired.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns><see langword="true"/> if the <paramref name="key"/> is found and has not expired; otherwise, <see langword="false"/>.</returns>
        private bool ContainsKey(byte[] key) => hash.ContainsKey(key) && !IsExpired(key);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Add(byte[] key, byte[] value)
        {
            DeleteExpiredItems();
            hash.Add(key, value);
            UpdateSize(key, value);
        }

        private void SetWithoutPersist(byte[] key, byte[] value)
        {
            DeleteExpiredItems();
            hash[key] = value;
            // Skip overhead as existing item is getting replaced.
            this.Size += Utility.RoundUp(value.Length, IntPtr.Size) -
                         Utility.RoundUp(value.Length, IntPtr.Size);
        }

        private ExpireResult SetExpiration(byte[] key, long expiration, ExpireOption expireOption)
        {
            if (!ContainsKey(key))
            {
                return ExpireResult.KeyNotFound;
            }

            if (expiration <= DateTimeOffset.UtcNow.Ticks)
            {
                Remove(key, out _);
                return ExpireResult.KeyAlreadyExpired;
            }

            InitializeExpirationStructures();

            ref var expirationTimeRef = ref CollectionsMarshal.GetValueRefOrAddDefault(expirationTimes, key, out var exists);

            if (exists)
            {
                if ((expireOption & ExpireOption.NX) == ExpireOption.NX ||
                    ((expireOption & ExpireOption.GT) == ExpireOption.GT && expiration <= expirationTimeRef) ||
                    ((expireOption & ExpireOption.LT) == ExpireOption.LT && expiration >= expirationTimeRef))
                {
                    return ExpireResult.ExpireConditionNotMet;
                }

                expirationTimeRef = expiration;
                expirationQueue.Enqueue(key, expiration);
                // Size of dictionary entry already accounted for as the key already exists
                Size += IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueEntryOverhead;
            }
            else
            {
                if ((expireOption & ExpireOption.XX) == ExpireOption.XX ||
                    (expireOption & ExpireOption.GT) == ExpireOption.GT)
                {
                    return ExpireResult.ExpireConditionNotMet;
                }

                expirationTimeRef = expiration;
                expirationQueue.Enqueue(key, expiration);
                UpdateExpirationSize(key);
            }

            return ExpireResult.ExpireUpdated;
        }

        /// <summary>
        /// Persist the <paramref name="key"/>.
        /// </summary>
        /// <remarks>
        /// On .NET 8, this method copies the <paramref name="key"/> to a new array in order to perform the lookups.
        /// </remarks>
        /// <param name="key">The key.</param>
        /// <returns><see langword="true"/> if the <paramref name="key"/> is found and has expired; otherwise, <see langword="false"/>.</returns>
        private ExpireResult Persist(ReadOnlySpan<byte> key)
        {
#if NET9_0_OR_GREATER
            if (!ContainsKey(key))
#else
            var keyArray = key.ToArray();
            if (!ContainsKey(keyArray))
#endif
            {
                return ExpireResult.KeyNotFound;
            }

            if (HasExpirableItems &&
#if NET9_0_OR_GREATER
                expirationLookup.Remove(key))
#else
                expirationTimes.Remove(keyArray))
#endif
            {
                this.Size -= IntPtr.Size + sizeof(long) + MemoryUtils.DictionaryEntryOverhead;
                CleanupExpirationStructures();
                return ExpireResult.ExpireUpdated;
            }

            return ExpireResult.NoExpiration;
        }

        private long GetExpiration(byte[] key)
        {
            if (!ContainsKey(key))
            {
                return (long)ExpireResult.KeyNotFound;
            }

            if (expirationTimes is not null && expirationTimes.TryGetValue(key, out var expiration))
            {
                return expiration;
            }

            return (long)ExpireResult.NoExpiration;
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
        NoExpiration = -1,
        ExpireConditionNotMet = 0,
        ExpireUpdated = 1,
        KeyAlreadyExpired = 2,
    }
}