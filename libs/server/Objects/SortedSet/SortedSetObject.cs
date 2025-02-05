﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
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
    /// Operations on SortedSet
    /// </summary>
    public enum SortedSetOperation : byte
    {
        ZADD,
        ZCARD,
        ZPOPMAX,
        ZSCORE,
        ZREM,
        ZCOUNT,
        ZINCRBY,
        ZRANK,
        ZRANGE,
        ZRANGEBYSCORE,
        ZRANGESTORE,
        GEOADD,
        GEOHASH,
        GEODIST,
        GEOPOS,
        GEOSEARCH,
        GEOSEARCHSTORE,
        ZREVRANGE,
        ZREVRANGEBYLEX,
        ZREVRANGEBYSCORE,
        ZREVRANK,
        ZREMRANGEBYLEX,
        ZREMRANGEBYRANK,
        ZREMRANGEBYSCORE,
        ZLEXCOUNT,
        ZPOPMIN,
        ZRANDMEMBER,
        ZDIFF,
        ZSCAN,
        ZMSCORE,
        ZEXPIRE,
        ZTTL,
        ZPERSIST,
        ZCOLLECT
    }

    [Flags]
    public enum SortedSetAddOption
    {
        None = 0,
        /// <summary>
        /// Only update elements that already exist. Don't add new elements.
        /// </summary>
        XX = 1,
        /// <summary>
        /// Only add new elements. Don't update already existing elements.
        /// </summary>
        NX = 1 << 1,
        /// <summary>
        /// Only update existing elements if the new score is less than the current score.
        /// </summary>
        LT = 1 << 2,
        /// <summary>
        /// Only update existing elements if the new score is greater than the current score.
        /// </summary>
        GT = 1 << 3,
        /// <summary>
        /// Modify the return value from the number of new elements added, to the total number of elements changed.
        /// Changed elements are new elements added and elements already existing for which the score was updated.
        /// </summary>
        CH = 1 << 4,
        /// <summary>
        /// When this option is specified ZADD acts like ZINCRBY. Only one score-element pair can be specified in this mode.
        /// </summary>
        INCR = 1 << 5,
    }

    /// <summary>
    /// Order variations for sorted set commands
    /// </summary>
    public enum SortedSetOrderOperation
    {
        /// <summary>
        /// Rank(by index of the elements)
        /// </summary>
        ByRank,

        /// <summary>
        /// Score ordering
        /// </summary>
        ByScore,

        /// <summary>
        /// Lexicographical ordering (relies on all elements having the same score).
        /// </summary>
        ByLex,
    }

    /// <summary>
    /// Sorted Set
    /// </summary>
    public unsafe partial class SortedSetObject : GarnetObjectBase
    {
        private readonly SortedSet<(double Score, byte[] Element)> sortedSet;
        private readonly Dictionary<byte[], double> sortedSetDict;
        private Dictionary<byte[], long> expirationTimes;
        private PriorityQueue<byte[], long> expirationQueue;

        // Byte #31 is used to denote if key has expiration (1) or not (0)
        private const int ExpirationBitMask = 1 << 31;

        /// <summary>
        /// Constructor
        /// </summary>
        public SortedSetObject(long expiration = 0)
            : base(expiration, MemoryUtils.SortedSetOverhead + MemoryUtils.DictionaryOverhead)
        {
            sortedSet = new(SortedSetComparer.Instance);
            sortedSetDict = new Dictionary<byte[], double>(ByteArrayComparer.Instance);
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public SortedSetObject(BinaryReader reader)
            : base(reader, MemoryUtils.SortedSetOverhead + MemoryUtils.DictionaryOverhead)
        {
            sortedSet = new(SortedSetComparer.Instance);
            sortedSetDict = new Dictionary<byte[], double>(ByteArrayComparer.Instance);

            int count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                var keyLength = reader.ReadInt32();
                var hasExpiration = (keyLength & ExpirationBitMask) != 0;
                keyLength &= ~ExpirationBitMask;
                var item = reader.ReadBytes(keyLength);
                var score = reader.ReadDouble();

                if (hasExpiration)
                {
                    var expiration = reader.ReadInt64();
                    var isExpired = expiration < DateTimeOffset.UtcNow.Ticks;
                    if (!isExpired)
                    {
                        sortedSetDict.Add(item, score);
                        sortedSet.Add((score, item));
                        InitializeExpirationStructures();
                        expirationTimes.Add(item, expiration);
                        expirationQueue.Enqueue(item, expiration);
                        UpdateExpirationSize(item, true);
                    }
                }
                else
                {
                    sortedSetDict.Add(item, score);
                    sortedSet.Add((score, item));
                }

                this.UpdateSize(item);
            }
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        public SortedSetObject(SortedSet<(double, byte[])> sortedSet, Dictionary<byte[], double> sortedSetDict, Dictionary<byte[], long> expirationTimes, PriorityQueue<byte[], long> expirationQueue, long expiration, long size)
            : base(expiration, size)
        {
            this.sortedSet = sortedSet;
            this.sortedSetDict = sortedSetDict;
            this.expirationTimes = expirationTimes;
            this.expirationQueue = expirationQueue;
        }

        /// <inheritdoc />
        public override byte Type => (byte)GarnetObjectType.SortedSet;

        /// <summary>
        /// Get sorted set as a dictionary
        /// </summary>
        public Dictionary<byte[], double> Dictionary => sortedSetDict;

        /// <summary>
        /// Serialize
        /// </summary>
        public override void DoSerialize(BinaryWriter writer)
        {
            base.DoSerialize(writer);

            DeleteExpiredItems();

            int count = sortedSetDict.Count; // Since expired items are already deleted, no need to worry about expiring items
            writer.Write(count);
            foreach (var kvp in sortedSetDict)
            {
                if (expirationTimes is not null && expirationTimes.TryGetValue(kvp.Key, out var expiration))
                {
                    writer.Write(kvp.Key.Length | ExpirationBitMask);
                    writer.Write(kvp.Key);
                    writer.Write(kvp.Value);
                    writer.Write(expiration);
                    count--;
                    continue;
                }

                writer.Write(kvp.Key.Length);
                writer.Write(kvp.Key);
                writer.Write(kvp.Value);
                count--;
            }
            Debug.Assert(count == 0);
        }

        /// <summary>
        /// Add to SortedSet
        /// </summary>
        /// <param name="item"></param>
        /// <param name="score"></param>
        public void Add(byte[] item, double score)
        {
            DeleteExpiredItems();

            sortedSetDict.Add(item, score);
            sortedSet.Add((score, item));

            this.UpdateSize(item);
        }

        /// <summary>
        /// Check for equality
        /// </summary>
        public bool Equals(SortedSetObject other)
        {
            // TODO: Implement equals with expiration times
            if (sortedSetDict.Count != other.sortedSetDict.Count) return false;

            foreach (var key in sortedSetDict)
                if (!other.sortedSetDict.TryGetValue(key.Key, out var otherValue) || key.Value != otherValue)
                    return false;

            return true;
        }

        /// <inheritdoc />
        public override void Dispose() { }

        /// <inheritdoc />
        public override GarnetObjectBase Clone() => new SortedSetObject(sortedSet, sortedSetDict, expirationTimes, expirationQueue, Expiration, Size);

        /// <inheritdoc />
        public override unsafe bool Operate(ref ObjectInput input, ref GarnetObjectStoreOutput output, out long sizeChange)
        {
            sizeChange = 0;

            fixed (byte* outputSpan = output.SpanByteAndMemory.SpanByte.AsSpan())
            {
                var header = input.header;
                if (header.type != GarnetObjectType.SortedSet)
                {
                    // Indicates an incorrect type of key
                    output.OutputFlags |= ObjectStoreOutputFlags.WrongType;
                    output.SpanByteAndMemory.Length = 0;
                    return true;
                }

                var prevSize = this.Size;
                var op = header.SortedSetOp;
                switch (op)
                {
                    case SortedSetOperation.ZADD:
                        SortedSetAdd(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZREM:
                        SortedSetRemove(ref input, outputSpan);
                        break;
                    case SortedSetOperation.ZCARD:
                        SortedSetLength(outputSpan);
                        break;
                    case SortedSetOperation.ZPOPMAX:
                        SortedSetPopMinOrMaxCount(ref input, ref output.SpanByteAndMemory, op);
                        break;
                    case SortedSetOperation.ZSCORE:
                        SortedSetScore(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZMSCORE:
                        SortedSetScores(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZCOUNT:
                        SortedSetCount(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZINCRBY:
                        SortedSetIncrement(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZRANK:
                        SortedSetRank(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZRANGE:
                    case SortedSetOperation.ZRANGESTORE:
                        SortedSetRange(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZRANGEBYSCORE:
                        SortedSetRange(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZEXPIRE:
                        SortedSetExpire(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZTTL:
                        SortedSetTimeToLive(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZPERSIST:
                        SortedSetPersist(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZCOLLECT:
                        SortedSetCollect(ref input, outputSpan);
                        break;
                    case SortedSetOperation.GEOADD:
                        GeoAdd(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.GEOHASH:
                        GeoHash(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.GEODIST:
                        GeoDistance(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.GEOPOS:
                        GeoPosition(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.GEOSEARCH:
                    case SortedSetOperation.GEOSEARCHSTORE:
                        GeoSearch(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZREVRANGE:
                        SortedSetRange(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZREVRANGEBYLEX:
                    case SortedSetOperation.ZREVRANGEBYSCORE:
                        SortedSetRange(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZREVRANK:
                        SortedSetRank(ref input, ref output.SpanByteAndMemory, ascending: false);
                        break;
                    case SortedSetOperation.ZREMRANGEBYLEX:
                        SortedSetRemoveOrCountRangeByLex(ref input, outputSpan, op);
                        break;
                    case SortedSetOperation.ZREMRANGEBYRANK:
                        SortedSetRemoveRangeByRank(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZREMRANGEBYSCORE:
                        SortedSetRemoveRangeByScore(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZLEXCOUNT:
                        SortedSetRemoveOrCountRangeByLex(ref input, outputSpan, op);
                        break;
                    case SortedSetOperation.ZPOPMIN:
                        SortedSetPopMinOrMaxCount(ref input, ref output.SpanByteAndMemory, op);
                        break;
                    case SortedSetOperation.ZRANDMEMBER:
                        SortedSetRandomMember(ref input, ref output.SpanByteAndMemory);
                        break;
                    case SortedSetOperation.ZSCAN:
                        if (ObjectUtils.ReadScanInput(ref input, ref output.SpanByteAndMemory, out var cursorInput, out var pattern,
                                out var patternLength, out var limitCount, out var _, out var error))
                        {
                            Scan(cursorInput, out var items, out var cursorOutput, count: limitCount, pattern: pattern,
                                patternLength: patternLength);
                            ObjectUtils.WriteScanOutput(items, cursorOutput, ref output.SpanByteAndMemory);
                        }
                        else
                        {
                            ObjectUtils.WriteScanError(error, ref output.SpanByteAndMemory);
                        }
                        break;
                    default:
                        throw new GarnetException($"Unsupported operation {op} in SortedSetObject.Operate");
                }

                sizeChange = this.Size - prevSize;
            }

            if (sortedSetDict.Count == 0)
                output.OutputFlags |= ObjectStoreOutputFlags.RemoveKey;

            return true;
        }

        /// <inheritdoc />
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0, bool isNoValue = false)
        {
            cursor = start;
            items = new List<byte[]>();

            // Allocation for score to string conversion
            // Based on the reference https://en.wikipedia.org/wiki/IEEE_754-1985
            // This is the maximum number of characters in UTF8 format written to the byte stream by TryFormat
            const int DOUBLE_MAX_STRING_LENGTH = 38;
            Span<byte> doubleValueToByteSpan = stackalloc byte[DOUBLE_MAX_STRING_LENGTH];

            int index = 0;

            if (Dictionary.Count < start)
            {
                cursor = 0;
                return;
            }

            var expiredKeysCount = 0;
            foreach (var item in Dictionary)
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

                bool addToList = false;
                if (patternLength == 0)
                {
                    items.Add(item.Key);
                    addToList = true;
                    }
                else
                {
                    fixed (byte* keyPtr = item.Key)
                    {
                        if (GlobUtils.Match(pattern, patternLength, keyPtr, item.Key.Length))
                        {
                            items.Add(item.Key);
                            addToList = true;
                            }
                        }
                    }

                if (addToList)
                {
                    // Double.TryFormat was prefered to convert the value to UTF8 byte array, but is not available before .net 8
                    if (Utf8Formatter.TryFormat(item.Value, doubleValueToByteSpan, out int bytesWritten, default))
                        items.Add(doubleValueToByteSpan.Slice(0, bytesWritten).ToArray());
                    else
                        items.Add(null);
                }

                cursor++;

                // Each item is a pair in the Dictionary but two items in the result List
                if (items.Count == (count * 2))
                    break;
            }

            // Indicates end of collection has been reached.
            if (cursor + expiredKeysCount == sortedSetDict.Count)
                cursor = 0;

        }

        #region Common Methods

        /// <summary>
        /// Compute difference of two dictionaries, with new result
        /// </summary>
        public static Dictionary<byte[], double> CopyDiff(Dictionary<byte[], double> dict1, Dictionary<byte[], double> dict2)
        {
            if (dict1 == null)
                return [];

            if (dict2 == null)
                return new Dictionary<byte[], double>(dict1, dict1.Comparer);

            var result = new Dictionary<byte[], double>(dict1.Comparer);
            foreach (var item in dict1)
            {
                if (!dict2.ContainsKey(item.Key))
                    result.Add(item.Key, item.Value);
            }
            return result;
        }

        /// <summary>
        /// Remove keys existing in second dictionary, from the first dictionary, if they exist
        /// </summary>
        public static void InPlaceDiff(Dictionary<byte[], double> dict1, Dictionary<byte[], double> dict2)
        {
            Debug.Assert(dict1 != null);

            if (dict2 != null)
            {
                foreach (var item in dict1)
                {
                    if (dict2.ContainsKey(item.Key))
                        dict1.Remove(item.Key);
                }
            }
        }

        #endregion
        private void InitializeExpirationStructures()
        {
            if (expirationTimes is null)
            {
                expirationTimes = new Dictionary<byte[], long>(ByteArrayComparer.Instance);
                expirationQueue = new PriorityQueue<byte[], long>();
                this.Size += MemoryUtils.DictionaryOverhead + MemoryUtils.PriorityQueueOverhead;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UpdateExpirationSize(ReadOnlySpan<byte> key, bool add = true)
        {
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
            }
        }

        private void DeleteExpiredItems()
        {
            if (expirationTimes is null)
                return;

            while (expirationQueue.TryPeek(out var key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks)
            {
                if (expirationTimes.TryGetValue(key, out var actualExpiration) && actualExpiration == expiration)
                {
                    expirationTimes.Remove(key);
                    expirationQueue.Dequeue();
                    UpdateExpirationSize(key, false);
                    if (sortedSetDict.TryGetValue(key, out var value))
                    {
                        sortedSetDict.Remove(key);
                        sortedSet.Remove((value, key));
                        UpdateSize(key, false);
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
        private bool TryGetScore(byte[] key, out double value)
        {
            value = default;
            if (IsExpired(key))
            {
                return false;
            }

            return sortedSetDict.TryGetValue(key, out value);
        }

        private int Count()
        {
            if (expirationTimes is null)
            {
                return sortedSetDict.Count;
            }
            var expiredKeysCount = 0;
            foreach (var item in expirationTimes)
            {
                if (IsExpired(item.Key))
                {
                    expiredKeysCount++;
                }
            }
            return sortedSetDict.Count - expiredKeysCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsExpired(byte[] key) => expirationTimes is not null && expirationTimes.TryGetValue(key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks;

        private int SetExpiration(byte[] key, long expiration, ExpireOption expireOption)
        {
            if (!sortedSetDict.ContainsKey(key))
            {
                return (int)ExpireResult.KeyNotFound;
            }

            if (expiration <= DateTimeOffset.UtcNow.Ticks)
            {
                sortedSetDict.Remove(key, out var value);
                sortedSet.Remove((value, key));
                UpdateSize(key, false);
                return (int)ExpireResult.KeyAlreadyExpired;
            }

            InitializeExpirationStructures();

            if (expirationTimes.TryGetValue(key, out var currentExpiration))
            {
                if (expireOption.HasFlag(ExpireOption.NX) ||
                    (expireOption.HasFlag(ExpireOption.GT) && expiration <= currentExpiration) ||
                    (expireOption.HasFlag(ExpireOption.LT) && expiration >= currentExpiration))
                {
                    return (int)ExpireResult.ExpireConditionNotMet;
                }

                expirationTimes[key] = expiration;
                expirationQueue.Enqueue(key, expiration);
                this.Size += IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueEntryOverhead;
            }
            else
            {
                if (expireOption.HasFlag(ExpireOption.XX) || expireOption.HasFlag(ExpireOption.GT))
                {
                    return (int)ExpireResult.ExpireConditionNotMet;
                }

                expirationTimes[key] = expiration;
                expirationQueue.Enqueue(key, expiration);
                UpdateExpirationSize(key);
            }

            return (int)ExpireResult.ExpireUpdated;
        }

        private int Persist(byte[] key)
        {
            if (!sortedSetDict.ContainsKey(key))
            {
                return -2;
            }

            return TryRemoveExpiration(key) ? 1 : - 1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryRemoveExpiration(byte[] key)
        {
            if (expirationTimes is null || !expirationTimes.TryGetValue(key, out _))
            {
                return false;
            }

            expirationTimes.Remove(key);
            this.Size -= IntPtr.Size + sizeof(long) + MemoryUtils.DictionaryEntryOverhead;
            CleanupExpirationStructures();
            return true;
        }

        private long GetExpiration(byte[] key)
        {
            if (!sortedSetDict.ContainsKey(key))
            {
                return -2;
            }

            if (expirationTimes is not null && expirationTimes.TryGetValue(key, out var expiration))
            {
                return expiration;
            }

            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HasExpirableItems()
        {
            return expirationTimes is not null;
        }

        private KeyValuePair<byte[], double> ElementAt(int index)
        {
            if (HasExpirableItems())
            {
                var currIndex = 0;
                foreach (var item in sortedSetDict)
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

            return sortedSetDict.ElementAt(index);
        }

        private void UpdateSize(ReadOnlySpan<byte> item, bool add = true)
        {
            // item's length + overhead to store item + value of type double added to sorted set and dictionary + overhead for those datastructures
            var size = Utility.RoundUp(item.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead + (2 * sizeof(double))
                + MemoryUtils.SortedSetEntryOverhead + MemoryUtils.DictionaryEntryOverhead;
            this.Size += add ? size : -size;
            Debug.Assert(this.Size >= MemoryUtils.SortedSetOverhead + MemoryUtils.DictionaryOverhead);
        }

        enum ExpireResult
        {
            KeyNotFound = -2,
            ExpireConditionNotMet = 0,
            ExpireUpdated = 1,
            KeyAlreadyExpired = 2,
        }
    }
}