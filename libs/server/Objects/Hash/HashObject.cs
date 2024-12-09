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
        readonly Dictionary<byte[], byte[]> hash;
        Dictionary<byte[], long> expirationTimes;
        PriorityQueue<byte[], long> expirationQueue;

        /// <summary>
        ///  Constructor
        /// </summary>
        public HashObject(long expiration = 0)
            : base(expiration, MemoryUtils.DictionaryOverhead)
        {
            hash = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);
        }

        /// <summary>
        /// Construct from binary serialized form
        /// </summary>
        public HashObject(BinaryReader reader)
            : base(reader, MemoryUtils.DictionaryOverhead)
        {
            // TODO: Handle deserialization of expiration times
            hash = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);

            int count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                var item = reader.ReadBytes(reader.ReadInt32());
                var value = reader.ReadBytes(reader.ReadInt32());
                hash.Add(item, value);

                this.UpdateSize(item, value);
            }

            int expireCount = reader.ReadInt32();
            // TODO: Can we delete expired items during serialization and deserialization?
            if (expireCount > 0)
            {
                expirationTimes = new Dictionary<byte[], long>(ByteArrayComparer.Instance);
                expirationQueue = new PriorityQueue<byte[], long>();
                for (int i = 0; i < count; i++)
                {
                    var item = reader.ReadBytes(reader.ReadInt32());
                    var value = reader.ReadInt64();
                    expirationTimes.Add(item, value);
                    expirationQueue.Enqueue(item, value);

                    // TODO: Update size
                }
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
        }

        /// <inheritdoc />
        public override byte Type => (byte)GarnetObjectType.Hash;

        /// <inheritdoc />
        public override void DoSerialize(BinaryWriter writer)
        {
            base.DoSerialize(writer);

            int count = hash.Count;
            writer.Write(count);
            foreach (var kvp in hash)
            {
                writer.Write(kvp.Key.Length);
                writer.Write(kvp.Key);
                writer.Write(kvp.Value.Length);
                writer.Write(kvp.Value);
                count--;
            }

            if (expirationTimes is not null)
            {
                // TODO: Can we delete expired items during serialization and deserialization?
                writer.Write(expirationTimes.Count);
                foreach (var kvp in expirationTimes)
                {
                    writer.Write(kvp.Key.Length);
                    writer.Write(kvp.Key);
                    writer.Write(kvp.Value);
                }
            }
            else
            {
                // TODO: This will break backward compatibility, Do we need to handle this?
                writer.Write(0);
            }
            Debug.Assert(count == 0);
        }

        /// <inheritdoc />
        public override void Dispose() { }

        /// <inheritdoc />
        public override GarnetObjectBase Clone() => new HashObject(hash, expirationTimes, expirationQueue, Expiration, Size);

        /// <inheritdoc />
        public override unsafe bool Operate(ref ObjectInput input, ref SpanByteAndMemory output, out long sizeChange, out bool removeKey)
        {
            removeKey = false;

            fixed (byte* _output = output.SpanByte.AsSpan())
            {
                if (input.header.type != GarnetObjectType.Hash)
                {
                    //Indicates when there is an incorrect type 
                    output.Length = 0;
                    sizeChange = 0;
                    return true;
                }

                var previousSize = this.Size;
                switch (input.header.HashOp)
                {
                    case HashOperation.HSET:
                        HashSet(ref input, _output);
                        break;
                    case HashOperation.HMSET:
                        HashSet(ref input, _output);
                        break;
                    case HashOperation.HGET:
                        HashGet(ref input, ref output);
                        break;
                    case HashOperation.HMGET:
                        HashMultipleGet(ref input, ref output);
                        break;
                    case HashOperation.HGETALL:
                        HashGetAll(ref input, ref output);
                        break;
                    case HashOperation.HDEL:
                        HashDelete(ref input, _output);
                        break;
                    case HashOperation.HLEN:
                        HashLength(_output);
                        break;
                    case HashOperation.HSTRLEN:
                        HashStrLength(ref input, _output);
                        break;
                    case HashOperation.HEXISTS:
                        HashExists(ref input, _output);
                        break;
                    case HashOperation.HEXPIRE:
                        HashExpire(ref input, ref output);
                        break;
                    case HashOperation.HKEYS:
                        HashGetKeysOrValues(ref input, ref output);
                        break;
                    case HashOperation.HVALS:
                        HashGetKeysOrValues(ref input, ref output);
                        break;
                    case HashOperation.HINCRBY:
                        HashIncrement(ref input, ref output);
                        break;
                    case HashOperation.HINCRBYFLOAT:
                        HashIncrement(ref input, ref output);
                        break;
                    case HashOperation.HSETNX:
                        HashSet(ref input, _output);
                        break;
                    case HashOperation.HRANDFIELD:
                        HashRandomField(ref input, ref output);
                        break;
                    case HashOperation.HSCAN:
                        if (ObjectUtils.ReadScanInput(ref input, ref output, out var cursorInput, out var pattern,
                                out var patternLength, out var limitCount, out bool isNoValue, out var error))
                        {
                            Scan(cursorInput, out var items, out var cursorOutput, count: limitCount, pattern: pattern,
                                patternLength: patternLength, isNoValue);
                            ObjectUtils.WriteScanOutput(items, cursorOutput, ref output);
                        }
                        else
                        {
                            ObjectUtils.WriteScanError(error, ref output);
                        }
                        break;
                    default:
                        throw new GarnetException($"Unsupported operation {input.header.HashOp} in HashObject.Operate");
                }

                sizeChange = this.Size - previousSize;
            }

            removeKey = hash.Count == 0;
            return true;
        }

        private void UpdateSize(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool add = true)
        {
            // TODO: Should we consider the size of the key and value of the expire dictionary and queue?
            var size = Utility.RoundUp(key.Length, IntPtr.Size) + Utility.RoundUp(value.Length, IntPtr.Size)
                + (2 * MemoryUtils.ByteArrayOverhead) + MemoryUtils.DictionaryEntryOverhead;
            this.Size += add ? size : -size;
            Debug.Assert(this.Size >= MemoryUtils.DictionaryOverhead);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsExpired(byte[] key) => expirationTimes is not null && expirationTimes.TryGetValue(key, out var expiration) && expiration < DateTimeOffset.UtcNow.Ticks;

        private void DeleteExpiredItems()
        {
            if (expirationTimes is null)
                return;

            var hasValue = expirationQueue.TryPeek(out var key, out var expiration);

            if (!hasValue)
            {
                expirationTimes = null;
                expirationQueue = null;
                return;
            }

            while (expiration < DateTimeOffset.UtcNow.Ticks)
            {
                expirationQueue.TryDequeue(out key, out _);
                expirationTimes.Remove(key);
                hash.Remove(key);
                // TODO: Update size
                hasValue = expirationQueue.TryPeek(out key, out expiration);
                if (!hasValue)
                {
                    expirationTimes = null;
                    expirationQueue = null;
                    break;
                }
            }
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

        private bool Remove(byte[] key, out byte[] value)
        {
            DeleteExpiredItems();
            return hash.Remove(key, out value);
        }

        private int Count()
        {
            if (expirationTimes is not null)
            {
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

            return hash.Count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HasExpirableItems()
        {
            return expirationTimes is not null;
        }

        private bool ContainsKey(byte[] key)
        {
            var result = hash.ContainsKey(key);
            if (result && IsExpired(key))
            {
                return false;
            }

            return result;
        }

        private IEnumerable<KeyValuePair<byte[], byte[]>> AsEnumerable()
        {
            if (HasExpirableItems())
            {
                // TODO: Check the performance of this implementation
                return hash.Where(x => !IsExpired(x.Key));
            }

            return hash;
        }

        private void Add(byte[] key, byte[] value)
        {
            DeleteExpiredItems();
            hash.Add(key, value);
        }

        private void Set(byte[] key, byte[] value)
        {
            DeleteExpiredItems();
            hash[key] = value;
        }

        private int SetExpire(byte[] key, long expiration, ExpireOption expireOption)
        {
            if (!ContainsKey(key))
            {
                return -2;
            }

            if (expiration <= DateTimeOffset.UtcNow.Ticks)
            {
                Remove(key, out _);
                return 2;
            }

            if (expirationTimes is null)
            {
                expirationTimes = new Dictionary<byte[], long>(ByteArrayComparer.Instance);
                expirationQueue = new PriorityQueue<byte[], long>();
            }

            if (expirationTimes.TryGetValue(key, out var currentExpiration))
            {
                if (expireOption.HasFlag(ExpireOption.NX))
                {
                    return 0;
                }

                if (expireOption.HasFlag(ExpireOption.GT) && expiration <= currentExpiration)
                {
                    return 0;
                }

                if (expireOption.HasFlag(ExpireOption.LT) && expiration >= currentExpiration)
                {
                    return 0;
                }
            }
            else
            {
                if (expireOption.HasFlag(ExpireOption.XX))
                {
                    return 0;
                }
            }

            expirationTimes[key] = expiration;
            expirationQueue.Enqueue(key, expiration);
            return 1;
        }

        private KeyValuePair<byte[], byte[]> ElementAt(int index)
        {
            if (HasExpirableItems())
            {
                var currIndex = 0;
                foreach (var item in AsEnumerable())
                {
                    if (currIndex == index)
                    {
                        return item;
                    }
                }

                throw new ArgumentOutOfRangeException("index is outside the bounds of the source sequence.");
            }

            return hash.ElementAt(index);
        }
    }
}