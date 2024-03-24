// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
        GEOADD,
        GEOHASH,
        GEODIST,
        GEOPOS,
        GEOSEARCH,
        ZREVRANGE,
        ZREVRANK,
        ZREMRANGEBYLEX,
        ZREMRANGEBYRANK,
        ZREMRANGEBYSCORE,
        ZLEXCOUNT,
        ZPOPMIN,
        ZRANDMEMBER,
        ZDIFF,
        ZSCAN,
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
    public partial class SortedSetObject : GarnetObjectBase
    {
        readonly SortedSet<(double, byte[])> sortedSet;
        readonly Dictionary<byte[], double> sortedSetDict;

        static readonly SortedSetComparer sortedSetComparer = new();
        static readonly ByteArrayComparer byteArrayComparer = new();

        /// <summary>
        /// Constructor
        /// </summary>
        public SortedSetObject(long expiration = 0)
            : base(expiration, MemoryUtils.SortedSetOverhead + MemoryUtils.DictionaryOverhead)
        {
            sortedSet = new(sortedSetComparer);
            sortedSetDict = new Dictionary<byte[], double>(byteArrayComparer);
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public SortedSetObject(BinaryReader reader)
            : base(reader, MemoryUtils.SortedSetOverhead + MemoryUtils.DictionaryOverhead)
        {
            sortedSet = new(sortedSetComparer);
            sortedSetDict = new Dictionary<byte[], double>(byteArrayComparer);

            int count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                var item = reader.ReadBytes(reader.ReadInt32());
                var score = reader.ReadDouble();
                sortedSet.Add((score, item));
                sortedSetDict.Add(item, score);

                this.UpdateSize(item);
            }
        }

        /// <summary>
        /// Copy constructor
        /// </summary>
        public SortedSetObject(SortedSet<(double, byte[])> sortedSet, Dictionary<byte[], double> sortedSetDict, long expiration, long size)
            : base(expiration, size)
        {
            this.sortedSet = sortedSet;
            this.sortedSetDict = sortedSetDict;
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

            int count = sortedSetDict.Count;
            writer.Write(count);
            foreach (var kvp in sortedSetDict)
            {
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
            sortedSetDict.Add(item, score);
            sortedSet.Add((score, item));

            this.UpdateSize(item);
        }

        /// <summary>
        /// Check for equality
        /// </summary>
        public bool Equals(SortedSetObject other)
        {
            if (sortedSetDict.Count != other.sortedSetDict.Count) return false;

            foreach (var key in sortedSetDict)
                if (!other.sortedSetDict.TryGetValue(key.Key, out var otherValue) || key.Value != otherValue)
                    return false;

            return true;
        }

        /// <inheritdoc />
        public override void Dispose() { }

        /// <inheritdoc />
        public override GarnetObjectBase Clone() => new SortedSetObject(sortedSet, sortedSetDict, Expiration, Size);

        /// <inheritdoc />
        public override unsafe bool Operate(ref SpanByte input, ref SpanByteAndMemory output, out long sizeChange)
        {
            fixed (byte* _input = input.AsSpan())
            fixed (byte* _output = output.SpanByte.AsSpan())
            {
                var header = (RespInputHeader*)_input;
                Debug.Assert(header->type == GarnetObjectType.SortedSet);
                long previouseSize = this.Size;
                switch (header->SortedSetOp)
                {
                    case SortedSetOperation.ZADD:
                        SortedSetAdd(_input, input.Length, _output);
                        break;
                    case SortedSetOperation.ZREM:
                        SortedSetRemove(_input, input.Length, _output);
                        break;
                    case SortedSetOperation.ZCARD:
                        SortedSetLength(_output);
                        break;
                    case SortedSetOperation.ZPOPMAX:
                        SortedSetPop(_input, ref output);
                        break;
                    case SortedSetOperation.ZSCORE:
                        SortedSetScore(_input, ref output);
                        break;
                    case SortedSetOperation.ZCOUNT:
                        SortedSetCount(_input, input.Length, _output);
                        break;
                    case SortedSetOperation.ZINCRBY:
                        SortedSetIncrement(_input, input.Length, ref output);
                        break;
                    case SortedSetOperation.ZRANK:
                        SortedSetRank(_input, input.Length, _output);
                        break;
                    case SortedSetOperation.ZRANGE:
                        SortedSetRange(_input, input.Length, ref output);
                        break;
                    case SortedSetOperation.ZRANGEBYSCORE:
                        SortedSetRangeByScore(_input, input.Length, ref output);
                        break;
                    case SortedSetOperation.GEOADD:
                        GeoAdd(_input, input.Length, _output);
                        break;
                    case SortedSetOperation.GEOHASH:
                        GeoHash(_input, input.Length, ref output);
                        break;
                    case SortedSetOperation.GEODIST:
                        GeoDistance(_input, input.Length, ref output);
                        break;
                    case SortedSetOperation.GEOPOS:
                        GeoPosition(_input, input.Length, ref output);
                        break;
                    case SortedSetOperation.GEOSEARCH:
                        GeoSearch(_input, input.Length, ref output);
                        break;
                    case SortedSetOperation.ZREVRANGE:
                        SortedSetReverseRange(_input, input.Length, ref output);
                        break;
                    case SortedSetOperation.ZREVRANK:
                        SortedSetReverseRank(_input, input.Length, _output);
                        break;
                    case SortedSetOperation.ZREMRANGEBYLEX:
                        SortedSetRemoveRangeByLex(_input, input.Length, _output);
                        break;
                    case SortedSetOperation.ZREMRANGEBYRANK:
                        SortedSetRemoveRangeByRank(_input, input.Length, _output);
                        break;
                    case SortedSetOperation.ZREMRANGEBYSCORE:
                        SortedSetRemoveRangeByScore(_input, input.Length, _output);
                        break;
                    case SortedSetOperation.ZLEXCOUNT:
                        SortedSetCountByLex(_input, input.Length, _output);
                        break;
                    case SortedSetOperation.ZPOPMIN:
                        SortedSetPopMin(_input, ref output);
                        break;
                    case SortedSetOperation.ZRANDMEMBER:
                        SortedSetRandomMember(_input, input.Length, ref output);
                        break;
                    case SortedSetOperation.ZSCAN:
                        if (ObjectUtils.ReadScanInput(_input, input.Length, ref output, out var cursorInput, out var pattern, out var patternLength, out int limitCount, out int bytesDone))
                        {
                            Scan(cursorInput, out var items, out var cursorOutput, count: limitCount, pattern: pattern, patternLength: patternLength);
                            ObjectUtils.WriteScanOutput(items, cursorOutput, ref output, bytesDone);
                        }
                        break;
                    default:
                        throw new GarnetException($"Unsupported operation {(SortedSetOperation)_input[0]} in SortedSetObject.Operate");
                }
                sizeChange = this.Size - previouseSize;
            }
            return true;
        }

        /// <inheritdoc />
        public override unsafe void Scan(long start, out List<byte[]> items, out long cursor, int count = 10, byte* pattern = default, int patternLength = 0)
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

            foreach (var item in Dictionary)
            {
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
            if (cursor == Dictionary.Count)
                cursor = 0;

        }

        #region Common Methods

        /// <summary>
        /// Compute difference of two dictionaries, with new result
        /// </summary>
        public static Dictionary<byte[], double> CopyDiff(Dictionary<byte[], double> dict1, Dictionary<byte[], double> dict2)
        {
            Dictionary<byte[], double> result = new();
            if (dict1 != null)
            {
                foreach (var item in dict1)
                {
                    if (dict2 == null || !dict2.ContainsKey(item.Key))
                        result.Add(item.Key, item.Value);
                }
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

        private void UpdateSize(byte[] item, bool add = true)
        {
            // item's length + overhead to store item + value of type double added to sorted set and dictionary + overhead for those datastructures 
            var size = Utility.RoundUp(item.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead + (2 * sizeof(double))
                + MemoryUtils.SortedSetEntryOverhead + MemoryUtils.DictionaryEntryOverhead;
            this.Size += add ? size : -size;
            Debug.Assert(this.Size >= MemoryUtils.SortedSetOverhead + MemoryUtils.DictionaryOverhead);
        }
    }
}