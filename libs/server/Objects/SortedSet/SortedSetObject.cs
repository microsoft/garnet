// Copyright (c) Microsoft Corporation.
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
        GEOADD,
        GEOHASH,
        GEODIST,
        GEOPOS,
        GEOSEARCH,
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

    /// <summary>
    /// Options for specifying the range in sorted set operations.
    /// </summary>
    [Flags]
    public enum SortedSetRangeOptions : byte
    {
        /// <summary>
        /// No options specified.
        /// </summary>
        None = 0,
        /// <summary>
        /// Range by score.
        /// </summary>
        ByScore = 1,
        /// <summary>
        /// Range by lexicographical order.
        /// </summary>
        ByLex = 1 << 1,
        /// <summary>
        /// Reverse the range order.
        /// </summary>
        Reverse = 1 << 2,
        /// <summary>
        /// Store the result.
        /// </summary>
        Store = 1 << 3,
        /// <summary>
        /// Include scores in the result.
        /// </summary>
        WithScores = 1 << 4,
        /// <summary>
        /// Obtain a sub-range from the matching elements
        /// </summary>
        Limit = 1 << 5,
    }

    [Flags]
    public enum SortedSetAddOption : ushort
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
    public enum SortedSetOrderOperation : byte
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
    public partial class SortedSetObject : GarnetObjectBase
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
        public SortedSetObject()
            : base(MemoryUtils.SortedSetOverhead + MemoryUtils.DictionaryOverhead)
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

            var count = reader.ReadInt32();
            for (var i = 0; i < count; i++)
            {
                var keyLength = reader.ReadInt32();
                var hasExpiration = (keyLength & ExpirationBitMask) != 0;
                keyLength &= ~ExpirationBitMask;
                var item = reader.ReadBytes(keyLength);
                var score = reader.ReadDouble();
                var canAddItem = true;
                long expiration = 0;

                if (hasExpiration)
                {
                    expiration = reader.ReadInt64();
                    canAddItem = expiration >= DateTimeOffset.UtcNow.Ticks;
                }

                if (canAddItem)
                {
                    sortedSetDict.Add(item, score);
                    _ = sortedSet.Add((score, item));
                    UpdateSize(item);

                    if (expiration > 0)
                    {
                        InitializeExpirationStructures();
                        expirationTimes.Add(item, expiration);
                        expirationQueue.Enqueue(item, expiration);
                        UpdateExpirationSize(add: true);
                    }
                }
            }
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        public SortedSetObject(SortedSetObject sortedSetObject)
            : base(sortedSetObject.HeapMemorySize)
        {
            sortedSet = sortedSetObject.sortedSet;
            sortedSetDict = sortedSetObject.sortedSetDict;
            expirationTimes = sortedSetObject.expirationTimes;
            expirationQueue = sortedSetObject.expirationQueue;
        }

        /// <inheritdoc />
        public override byte Type => (byte)GarnetObjectType.SortedSet;

        /// <summary>
        /// Get sorted set as a dictionary.
        /// </summary>
        public Dictionary<byte[], double> Dictionary
        {
            get
            {
                if (!HasExpirableItems() || (expirationQueue.TryPeek(out _, out var expiration) && expiration > DateTimeOffset.UtcNow.Ticks))
                {
                    return sortedSetDict;
                }

                var result = new Dictionary<byte[], double>(ByteArrayComparer.Instance);
                foreach (var kvp in sortedSetDict)
                {
                    if (!IsExpired(kvp.Key))
                    {
                        result.Add(kvp.Key, kvp.Value);
                    }
                }
                return result;
            }
        }

        /// <summary>
        /// Serialize
        /// </summary>
        public override void DoSerialize(BinaryWriter writer)
        {
            base.DoSerialize(writer);

            DeleteExpiredItems();

            var count = sortedSetDict.Count; // Since expired items are already deleted, no need to worry about expiring items
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
            _ = sortedSet.Add((score, item));

            UpdateSize(item);
        }

        /// <summary>
        /// Check for equality
        /// </summary>
        public bool Equals(SortedSetObject other)
        {
            if (sortedSetDict.Count != other.sortedSetDict.Count)
                return false;

            foreach (var key in sortedSetDict)
            {
                if (IsExpired(key.Key) && IsExpired(key.Key))
                    continue;

                if (IsExpired(key.Key) || IsExpired(key.Key))
                    return false;

                if (!other.sortedSetDict.TryGetValue(key.Key, out var otherValue) || key.Value != otherValue)
                    return false;
            }

            return true;
        }

        /// <inheritdoc />
        public override void Dispose() { }

        /// <inheritdoc />
        public override GarnetObjectBase Clone() => new SortedSetObject(this);

        /// <inheritdoc />
        public override bool Operate(ref ObjectInput input, ref ObjectOutput output,
                                     ref RespMemoryWriter writer, out long memorySizeChange)
        {
            memorySizeChange = 0;
            
            var header = input.header;
            if (header.type != GarnetObjectType.SortedSet)
            {
                // Indicates an incorrect type of key
                output.OutputFlags |= OutputFlags.WrongType;
                output.SpanByteAndMemory.Length = 0;
                return true;
            }

            var prevMemorySize = HeapMemorySize;
            var op = header.SortedSetOp;

            switch (op)
            {
                case SortedSetOperation.ZADD:
                    SortedSetAdd(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZREM:
                    SortedSetRemove(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZCARD:
                    SortedSetLength(ref output, ref writer);
                    break;
                case SortedSetOperation.ZPOPMIN:
                case SortedSetOperation.ZPOPMAX:
                    SortedSetPopMinOrMaxCount(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZSCORE:
                    SortedSetScore(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZMSCORE:
                    SortedSetScores(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZCOUNT:
                    SortedSetCount(ref input, ref writer);
                    break;
                case SortedSetOperation.ZINCRBY:
                    SortedSetIncrement(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZRANK:
                case SortedSetOperation.ZREVRANK:
                    SortedSetRank(ref input, ref writer);
                    break;
                case SortedSetOperation.ZEXPIRE:
                    SortedSetExpire(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZTTL:
                    SortedSetTimeToLive(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZPERSIST:
                    SortedSetPersist(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZCOLLECT:
                    SortedSetCollect(ref output);
                    break;
                case SortedSetOperation.GEOADD:
                    GeoAdd(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.GEOHASH:
                    GeoHash(ref input, ref writer);
                    break;
                case SortedSetOperation.GEODIST:
                    GeoDistance(ref input, ref writer);
                    break;
                case SortedSetOperation.GEOPOS:
                    GeoPosition(ref input, ref writer);
                    break;
                case SortedSetOperation.ZRANGE:
                    SortedSetRange(ref input, ref writer);
                    break;
                case SortedSetOperation.ZLEXCOUNT:
                case SortedSetOperation.ZREMRANGEBYLEX:
                    SortedSetRemoveOrCountRangeByLex(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZREMRANGEBYRANK:
                    SortedSetRemoveRangeByRank(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZREMRANGEBYSCORE:
                    SortedSetRemoveRangeByScore(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZRANDMEMBER:
                    SortedSetRandomMember(ref input, ref output, ref writer);
                    break;
                case SortedSetOperation.ZSCAN:
                    Scan(ref input, ref output, ref writer);
                    break;
                default:
                    throw new GarnetException($"Unsupported operation {op} in {nameof(SortedSetObject)}.{nameof(Operate)}");
            }

            memorySizeChange = HeapMemorySize - prevMemorySize;

            if (sortedSetDict.Count == 0)
                output.OutputFlags |= OutputFlags.RemoveKey;

            return true;
        }

        /// <inheritdoc />
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0, bool isNoValue = false)
        {
            cursor = start;
            items = [];

            // Allocation for score to string conversion
            // Based on the reference https://en.wikipedia.org/wiki/IEEE_754-1985
            // This is the maximum number of characters in UTF8 format written to the byte stream by TryFormat
            const int DOUBLE_MAX_STRING_LENGTH = 38;
            Span<byte> doubleValueToByteSpan = stackalloc byte[DOUBLE_MAX_STRING_LENGTH];

            var index = 0;
            if (sortedSetDict.Count < start)
            {
                cursor = 0;
                return;
            }

            var expiredKeysCount = 0;
            foreach (var item in sortedSetDict)
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

                var addToList = false;
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
                    if (Utf8Formatter.TryFormat(item.Value, doubleValueToByteSpan, out var bytesWritten, default))
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
        public static Dictionary<byte[], double> CopyDiff(SortedSetObject sortedSetObject1, SortedSetObject sortedSetObject2)
        {
            if (sortedSetObject1 == null)
                return new Dictionary<byte[], double>(ByteArrayComparer.Instance);

            if (sortedSetObject2 == null)
            {
                if (sortedSetObject1.expirationTimes is null)
                {
                    return new Dictionary<byte[], double>(sortedSetObject1.sortedSetDict, ByteArrayComparer.Instance);
                }
                else
                {
                    var directResult = new Dictionary<byte[], double>(ByteArrayComparer.Instance);
                    foreach (var item in sortedSetObject1.sortedSetDict)
                    {
                        if (!sortedSetObject1.IsExpired(item.Key))
                            directResult.Add(item.Key, item.Value);
                    }
                    return directResult;
                }
            }

            var result = new Dictionary<byte[], double>(ByteArrayComparer.Instance);
            foreach (var item in sortedSetObject1.sortedSetDict)
            {
                if (!sortedSetObject1.IsExpired(item.Key) && !sortedSetObject2.IsExpired(item.Key) && !sortedSetObject2.sortedSetDict.ContainsKey(item.Key))
                    result.Add(item.Key, item.Value);
            }
            return result;
        }

        /// <summary>
        /// Remove keys existing in second dictionary, from the first dictionary, if they exist
        /// </summary>
        public static void InPlaceDiff(Dictionary<byte[], double> dict1, SortedSetObject sortedSetObject2)
        {
            Debug.Assert(dict1 != null);

            if (sortedSetObject2 != null)
            {
                foreach (var item in dict1)
                {
                    if (!sortedSetObject2.IsExpired(item.Key) && sortedSetObject2.sortedSetDict.ContainsKey(item.Key))
                        _ = dict1.Remove(item.Key);
                }
            }
        }

        /// <summary>
        /// Tries to get the score of the specified key.
        /// </summary>
        /// <param name="key">The key to get the score for.</param>
        /// <param name="value">The score of the key if found.</param>
        /// <returns>True if the key is found and not expired; otherwise, false.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetScore(byte[] key, out double value)
        {
            value = default;
            if (IsExpired(key))
                return false;

            return sortedSetDict.TryGetValue(key, out value);
        }

        /// <summary>
        /// Gets the count of elements in the sorted set.
        /// </summary>
        /// <returns>The count of elements in the sorted set.</returns>
        public int Count()
        {
            if (!HasExpirableItems())
                return sortedSetDict.Count;

            var expiredKeysCount = 0;
            foreach (var item in expirationTimes)
            {
                if (IsExpired(item.Key))
                    expiredKeysCount++;
            }
            return sortedSetDict.Count - expiredKeysCount;
        }

        /// <summary>
        /// Determines whether the specified key is expired.
        /// </summary>
        /// <param name="key">The key to check for expiration.</param>
        /// <returns>True if the key is expired; otherwise, false.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsExpired(byte[] key) => expirationTimes is not null && expirationTimes.TryGetValue(key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks;

        /// <summary>
        /// Determines whether the sorted set has expirable items.
        /// </summary>
        /// <returns>True if the sorted set has expirable items; otherwise, false.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool HasExpirableItems() => expirationTimes is not null;

        #endregion
        private void InitializeExpirationStructures()
        {
            if (expirationTimes is null)
            {
                expirationTimes = new Dictionary<byte[], long>(ByteArrayComparer.Instance);
                expirationQueue = new PriorityQueue<byte[], long>();
                HeapMemorySize += MemoryUtils.DictionaryOverhead + MemoryUtils.PriorityQueueOverhead;
                // No DiskSize adjustment needed yet; wait until keys are added or removed
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
                Debug.Assert(HeapMemorySize >= MemoryUtils.DictionaryOverhead);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CleanupExpirationStructuresIfEmpty()
        {
            if (expirationTimes.Count != 0)
                return;

            HeapMemorySize -= (IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueOverhead) * expirationQueue.Count;
            HeapMemorySize -= MemoryUtils.DictionaryOverhead + MemoryUtils.PriorityQueueOverhead;
            expirationTimes = null;
            expirationQueue = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeleteExpiredItems()
        {
            if (expirationTimes is null)
                return;
            DeleteExpiredItemsWorker();
        }

        private void DeleteExpiredItemsWorker()
        {
            while (expirationQueue.TryPeek(out var key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks)
            {
                if (expirationTimes.TryGetValue(key, out var actualExpiration) && actualExpiration == expiration)
                {
                    _ = expirationTimes.Remove(key);
                    _ = expirationQueue.Dequeue();
                    UpdateExpirationSize(add: false);
                    if (sortedSetDict.TryGetValue(key, out var value))
                    {
                        _ = sortedSetDict.Remove(key);
                        _ = sortedSet.Remove((value, key));
                        UpdateSize(key, add: false);
                    }
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

        private int SetExpiration(byte[] key, long expiration, ExpireOption expireOption)
        {
            if (!sortedSetDict.ContainsKey(key))
                return (int)SortedSetExpireResult.KeyNotFound;

            if (expiration <= DateTimeOffset.UtcNow.Ticks)
            {
                _ = sortedSetDict.Remove(key, out var value);
                _ = sortedSet.Remove((value, key));
                UpdateSize(key, add: false);
                return (int)SortedSetExpireResult.KeyAlreadyExpired;
            }

            InitializeExpirationStructures();

            if (expirationTimes.TryGetValue(key, out var currentExpiration))
            {
                if (expireOption.HasFlag(ExpireOption.NX) ||
                    (expireOption.HasFlag(ExpireOption.GT) && expiration <= currentExpiration) ||
                    (expireOption.HasFlag(ExpireOption.LT) && expiration >= currentExpiration))
                {
                    return (int)SortedSetExpireResult.ExpireConditionNotMet;
                }

                expirationTimes[key] = expiration;
                expirationQueue.Enqueue(key, expiration);

                // MemorySize of dictionary entry already accounted for as the key already exists.
                // DiskSize of expiration already accounted for as the key already exists in expirationTimes.
                HeapMemorySize += IntPtr.Size + sizeof(long) + MemoryUtils.PriorityQueueEntryOverhead;
            }
            else
            {
                if ((expireOption & ExpireOption.XX) == ExpireOption.XX || (expireOption & ExpireOption.GT) == ExpireOption.GT)
                    return (int)SortedSetExpireResult.ExpireConditionNotMet;

                expirationTimes[key] = expiration;
                expirationQueue.Enqueue(key, expiration);
                UpdateExpirationSize(add: true);
            }

            return (int)SortedSetExpireResult.ExpireUpdated;
        }

        private int Persist(byte[] key)
        {
            if (!sortedSetDict.ContainsKey(key))
                return -2;
            return TryRemoveExpiration(key) ? 1 : -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryRemoveExpiration(byte[] key)
        {
            if (expirationTimes is null)
                return false;
            return TryRemoveExpirationWorker(key);
        }

        private bool TryRemoveExpirationWorker(byte[] key)
        {
            if (!expirationTimes.TryGetValue(key, out _))
                return false;

            _ = expirationTimes.Remove(key);

            UpdateExpirationSize(add: false, includePQ: false);
            CleanupExpirationStructuresIfEmpty();
            return true;
        }

        private long GetExpiration(byte[] key)
        {
            if (!sortedSetDict.ContainsKey(key))
                return -2;
            if (expirationTimes is not null && expirationTimes.TryGetValue(key, out var expiration))
                return expiration;
            return -1;
        }

        private KeyValuePair<byte[], double> ElementAt(int index)
        {
            if (HasExpirableItems())
            {
                var currIndex = 0;
                foreach (var item in sortedSetDict)
                {
                    if (IsExpired(item.Key))
                        continue;
                    if (currIndex++ == index)
                        return item;
                }

                throw new ArgumentOutOfRangeException(nameof(index), "index is outside the bounds of the source sequence.");
            }

            return sortedSetDict.ElementAt(index);
        }

        private void UpdateSize(ReadOnlySpan<byte> item, bool add = true)
        {
            // item's length + overhead to store item + value of type double added to sorted set and dictionary + overhead for those datastructures
            var memorySize = Utility.RoundUp(item.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead + (2 * sizeof(double))
                + MemoryUtils.SortedSetEntryOverhead + MemoryUtils.DictionaryEntryOverhead;

            if (add)
                HeapMemorySize += memorySize;
            else
            {
                HeapMemorySize -= memorySize;
                Debug.Assert(HeapMemorySize >= MemoryUtils.SortedSetOverhead + MemoryUtils.DictionaryOverhead);
            }
        }

        /// <summary>
        /// Result of an expiration operation.
        /// </summary>
        enum SortedSetExpireResult
        {
            /// <summary>
            /// The key was not found.
            /// </summary>
            KeyNotFound = -2,

            /// <summary>
            /// The expiration condition was not met.
            /// </summary>
            ExpireConditionNotMet = 0,

            /// <summary>
            /// The expiration was updated.
            /// </summary>
            ExpireUpdated = 1,

            /// <summary>
            /// The key was already expired.
            /// </summary>
            KeyAlreadyExpired = 2,
        }
    }
}