// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Globalization;
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
    /// <summary>
    ///  Hash - RESP specific operations
    /// </summary>
    public partial class HashObject : IGarnetObject
    {
        private void HashGet(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var key = GetByteSpanFromInput(ref input, 0);
            if (TryGetValue(key, out var hashValue))
                writer.WriteBulkString(hashValue);
            else
                writer.WriteNull();

            output.Header.result1++;
        }

        private void HashMultipleGet(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            writer.WriteArrayLength(input.parseState.Count);

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var key = GetByteSpanFromInput(ref input, i);
                if (TryGetValue(key, out var hashValue))
                {
                    writer.WriteBulkString(hashValue);
                }
                else
                {
                    writer.WriteNull();
                }

                output.Header.result1++;
            }
        }

        private void HashGetAll(ref RespMemoryWriter writer)
        {
            writer.WriteMapLength(Count());

            var isExpirable = HasExpirableItems;

            foreach (var item in hash)
            {
                if (isExpirable && IsExpired(item.Key))
                {
                    continue;
                }

                writer.WriteBulkString(item.Key);
                writer.WriteBulkString(item.Value);
            }
        }

        private void HashDelete(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var removed = 0;

            for (var i = 0; i < input.parseState.Count; i++)
            {
                var key = GetByteSpanFromInput(ref input, i);
                if (Remove(key, out _))
                    removed++;
            }

            if (removed == 0)
                output.OutputFlags |= OutputFlags.ValueUnchanged;

            if (!input.header.CheckSkipRespOutputFlag())
                writer.WriteInt32(removed);

            output.Header.result1 = removed;
        }

        private void HashLength(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var length = Count();

            if (!input.header.CheckSkipRespOutputFlag())
                writer.WriteInt32(length);

            output.Header.result1 = length;
        }

        private void HashStrLength(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var key = GetByteSpanFromInput(ref input, 0);
            var length = TryGetValue(key, out var hashValue) ? hashValue.Length : 0;

            writer.WriteInt32(length);

            output.Header.result1 = length;
        }

        private void HashExists(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var field = GetByteSpanFromInput(ref input, 0);
            var exists = ContainsKey(field) ? 1 : 0;

            if (!input.header.CheckSkipRespOutputFlag())
                writer.WriteInt32(exists);

            output.Header.result1 = exists;
        }

        private void HashRandomField(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            // HRANDFIELD key [count [WITHVALUES]]
            var countParameter = input.arg1 >> 2;
            var withValues = (input.arg1 & 1) == 1;
            var includedCount = ((input.arg1 >> 1) & 1) == 1;
            var seed = input.arg2;

            var countDone = 0;

            if (includedCount)
            {
                var count = Count();

                if (count == 0) // This can happen because of expiration but RMW operation haven't applied yet
                {
                    writer.WriteEmptyArray();
                    output.Header.result1 = 0;
                    return;
                }

                if (countParameter > 0 && countParameter > count)
                    countParameter = count;

                var indexCount = Math.Abs(countParameter);

                var indexes = indexCount <= RandomUtils.IndexStackallocThreshold ?
                    stackalloc int[RandomUtils.IndexStackallocThreshold].Slice(0, indexCount) : new int[indexCount];

                RandomUtils.PickKRandomIndexes(count, indexes, seed, countParameter > 0);

                // Write the size of the array reply
                writer.WriteArrayLength(withValues && (!writer.resp3) ? indexCount * 2 : indexCount);

                foreach (var index in indexes)
                {
                    var pair = ElementAt(index);

                    if (writer.resp3 && withValues)
                        writer.WriteArrayLength(2);

                    writer.WriteBulkString(pair.Key);

                    if (withValues)
                    {
                        writer.WriteBulkString(pair.Value);
                    }

                    countDone++;
                }
            }
            else // No count parameter is present, we just return a random field
            {
                // Write a bulk string value of a random field from the hash value stored at key.
                var count = Count();
                if (count == 0) // This can happen because of expiration but RMW operation haven't applied yet
                {
                    writer.WriteNull();
                    output.Header.result1 = 0;
                    return;
                }

                var index = RandomUtils.PickRandomIndex(count, seed);
                var pair = ElementAt(index);
                writer.WriteBulkString(pair.Key);
                countDone = 1;
            }

            output.Header.result1 = countDone;
        }

        private void HashSet(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            var hashOp = input.header.HashOp;
            var added = 0;

            for (var i = 0; i < input.parseState.Count; i += 2)
            {
                var key = GetByteSpanFromInput(ref input, i);
                var value = input.parseState.GetArgSliceByRef(i + 1).ReadOnlySpan;

                // Avoid multiple hash calculations by acquiring ref to the dictionary value.
                // The ref is unsafe to read/write to if the hash dictionary is mutated.
                ref var hashValueRef =
#if NET9_0_OR_GREATER
                    ref CollectionsMarshal.GetValueRefOrAddDefault(hashSpanLookup, key, out var exists);
#else
                    ref CollectionsMarshal.GetValueRefOrAddDefault(hash, key, out var exists);
#endif

                if (!exists || IsExpired(key))
                {
                    hashValueRef = value.ToArray();
                    UpdateSize(key, value, add: true);

                    added++;
                }
                else if (exists && (hashOp is HashOperation.HSET or HashOperation.HMSET))
                {
                    if (hashValueRef.Length == value.Length)
                    {
                        value.CopyTo(hashValueRef);
                    }
                    else
                    {
                        // Adjust the size to account for the new value replacing the old one.
                        HeapMemorySize += Utility.RoundUp(value.Length, IntPtr.Size) - Utility.RoundUp(hashValueRef.Length, IntPtr.Size);
                        hashValueRef = value.ToArray();
                    }

                    // To persist the key, if it has an expiration
                    if (HasExpirableItems &&
#if NET9_0_OR_GREATER
                        expirationTimeSpanLookup.Remove(key))
#else
                        expirationTimes.Remove(key))
#endif
                    {
                        HeapMemorySize -= IntPtr.Size + sizeof(long) + MemoryUtils.DictionaryEntryOverhead;
                        CleanupExpirationStructuresIfEmpty();
                    }
                }
            }

            if (!input.header.CheckSkipRespOutputFlag())
                writer.WriteInt32(added);

            output.Header.result1 = added;
        }

        private void HashCollect(ref ObjectInput input, ref ObjectOutput output)
        {
            DeleteExpiredItems();
            output.Header.result1 = 1;
        }

        private void HashGetKeysOrValues(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var count = Count();
            var op = input.header.HashOp;

            writer.WriteArrayLength(count);

            var isExpirable = HasExpirableItems;

            foreach (var item in hash)
            {
                if (isExpirable && IsExpired(item.Key))
                {
                    continue;
                }

                if (HashOperation.HKEYS == op)
                {
                    writer.WriteBulkString(item.Key);
                }
                else
                {
                    writer.WriteBulkString(item.Value);
                }

                output.Header.result1++;
            }
        }

        [SkipLocalsInit] // avoid zeroing the stackalloc buffer
        private void HashIncrement(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            // This value is used to indicate partial command execution
            output.Header.result1 = int.MinValue;

            var key = GetByteSpanFromInput(ref input, 0);
            var incrSlice = input.parseState.GetArgSliceByRef(1);
            var incr = input.parseState.GetLong(1);

            DeleteExpiredItems();

            // Avoid multiple hash calculations by acquiring ref to the dictionary value.
            // The ref is unsafe to read/write to if the hash dictionary is mutated.
            ref var hashValueRef =
#if NET9_0_OR_GREATER
                ref CollectionsMarshal.GetValueRefOrAddDefault(hashSpanLookup, key, out var exists);
#else
                ref CollectionsMarshal.GetValueRefOrAddDefault(hash, key, out var exists);
#endif

            if (!exists || IsExpired(key))
            {
                hashValueRef = incrSlice.ToArray();
                UpdateSize(key, hashValueRef, add: true);
            }
            else
            {
                if (!NumUtils.TryParse(hashValueRef, out long result))
                {
                    writer.WriteError(CmdStrings.RESP_ERR_HASH_VALUE_IS_NOT_INTEGER);
                    output.OutputFlags |= OutputFlags.ValueUnchanged;
                    return;
                }

                result += incr;

                var formattedValue = (Span<byte>)stackalloc byte[NumUtils.MaximumFormatInt64Length];
                var success = result.TryFormat(formattedValue, out var bytesWritten, provider: CultureInfo.InvariantCulture);
                Debug.Assert(success);

                formattedValue = formattedValue.Slice(0, bytesWritten);

                if (formattedValue.Length == hashValueRef.Length)
                {
                    // instead of allocating a new byte array of the same size, we can reuse the existing one
                    formattedValue.CopyTo(hashValueRef);
                }
                else
                {
                    // Adjust the size to account for the new value replacing the old one.
                    HeapMemorySize += Utility.RoundUp(formattedValue.Length, IntPtr.Size) - Utility.RoundUp(hashValueRef.Length, IntPtr.Size);
                    hashValueRef = formattedValue.ToArray();
                }
            }

            writer.WriteIntegerFromBytes(hashValueRef);

            output.Header.result1 = 1;
        }

        [SkipLocalsInit] // avoid zeroing the stackalloc buffer
        private void HashIncrementFloat(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            var op = input.header.HashOp;

            // This value is used to indicate partial command execution
            output.Header.result1 = int.MinValue;

            var key = GetByteSpanFromInput(ref input, 0);
            var incrSlice = input.parseState.GetArgSliceByRef(1);
            var incr = input.parseState.GetDouble(1);

            if (double.IsInfinity(incr))
            {
                writer.WriteError(CmdStrings.RESP_ERR_GENERIC_NAN_INFINITY);
                output.OutputFlags |= OutputFlags.ValueUnchanged;
                return;
            }

            DeleteExpiredItems();

            // Avoid multiple hash calculations by acquiring ref to the dictionary value.
            // The ref is unsafe to read/write to if the hash dictionary is mutated.
            ref var hashValueRef =
#if NET9_0_OR_GREATER
                ref CollectionsMarshal.GetValueRefOrAddDefault(hashSpanLookup, key, out var exists);
#else
                ref CollectionsMarshal.GetValueRefOrAddDefault(hash, key, out var exists);
#endif

            if (!exists || IsExpired(key))
            {
                hashValueRef = incrSlice.ToArray();
                UpdateSize(key, hashValueRef, add: true);
            }
            else
            {
                if (!NumUtils.TryParseWithInfinity(hashValueRef, out var result))
                {
                    writer.WriteError(CmdStrings.RESP_ERR_HASH_VALUE_IS_NOT_FLOAT);
                    output.OutputFlags |= OutputFlags.ValueUnchanged;
                    return;
                }

                if (double.IsInfinity(result))
                {
                    writer.WriteError(CmdStrings.RESP_ERR_GENERIC_NAN_INFINITY_INCR);
                    output.OutputFlags |= OutputFlags.ValueUnchanged;
                    return;
                }

                result += incr;

                var formattedValue = (Span<byte>)stackalloc byte[NumUtils.MaximumFormatDoubleLength];
                var success = result.TryFormat(formattedValue, out var bytesWritten, provider: CultureInfo.InvariantCulture);
                Debug.Assert(success);

                formattedValue = formattedValue.Slice(0, bytesWritten);

                if (formattedValue.Length == hashValueRef.Length)
                {
                    // instead of allocating a new byte array of the same size, we can reuse the existing one
                    formattedValue.CopyTo(hashValueRef);
                }
                else
                {
                    // Adjust the size to account for the new value replacing the old one.
                    HeapMemorySize += Utility.RoundUp(formattedValue.Length, IntPtr.Size) - Utility.RoundUp(hashValueRef.Length, IntPtr.Size);
                    hashValueRef = formattedValue.ToArray();
                }
            }

            writer.WriteBulkString(hashValueRef);

            output.Header.result1 = 1;
        }

        private void HashExpire(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            var expirationWithOption = new ExpirationWithOption(input.arg1, input.arg2);

            writer.WriteArrayLength(input.parseState.Count);

            var expired = 0;
            foreach (var item in input.parseState.Parameters)
            {
#if NET9_0_OR_GREATER
                var result = SetExpiration(item.ReadOnlySpan, expirationWithOption.ExpirationTimeInTicks, expirationWithOption.ExpireOption);
#else
                var result = SetExpiration(item.ToArray(), expirationWithOption.ExpirationTimeInTicks, expirationWithOption.ExpireOption);
#endif
                writer.WriteInt32((int)result);

                output.Header.result1++;

                if (result == ExpireResult.ExpireUpdated)
                    expired++;
            }

            if (expired == 0)
                output.OutputFlags |= OutputFlags.ValueUnchanged;
        }

        private void HashTimeToLive(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            var isMilliseconds = input.arg1 == 1;
            var isTimestamp = input.arg2 == 1;
            var numFields = input.parseState.Count;

            writer.WriteArrayLength(numFields);

            foreach (var item in input.parseState.Parameters)
            {
#if NET9_0_OR_GREATER
                var result = GetExpiration(item.ReadOnlySpan);
#else
                var result = GetExpiration(item.ToArray());
#endif

                if (result >= 0)
                {
                    if (isTimestamp && isMilliseconds)
                    {
                        result = ConvertUtils.UnixTimeInMillisecondsFromTicks(result);
                    }
                    else if (isTimestamp && !isMilliseconds)
                    {
                        result = ConvertUtils.UnixTimeInSecondsFromTicks(result);
                    }
                    else if (!isTimestamp && isMilliseconds)
                    {
                        result = ConvertUtils.MillisecondsFromDiffUtcNowTicks(result);
                    }
                    else if (!isTimestamp && !isMilliseconds)
                    {
                        result = ConvertUtils.SecondsFromDiffUtcNowTicks(result);
                    }
                }

                writer.WriteInt64(result);
                output.Header.result1++;
            }
        }

        private void HashPersist(ref ObjectInput input, ref ObjectOutput output, ref RespMemoryWriter writer)
        {
            DeleteExpiredItems();

            var numFields = input.parseState.Count;

            writer.WriteArrayLength(numFields);

            var persisted = 0;
            foreach (var item in input.parseState.Parameters)
            {
#if NET9_0_OR_GREATER
                var result = Persist(item.ReadOnlySpan);
#else
                var result = Persist(item.ToArray());
#endif
                writer.WriteInt32((int)result);
                output.Header.result1++;

                if (result == ExpireResult.ExpireUpdated)
                    persisted++;
            }

            if (persisted == 0)
                output.OutputFlags |= OutputFlags.ValueUnchanged;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ByteSpan GetByteSpanFromInput(ref ObjectInput input, int index)
        {
            return input.parseState.GetArgSliceByRef(index)
#if NET9_0_OR_GREATER
                .ReadOnlySpan;
#else
                .ToArray();
#endif
        }
    }
}