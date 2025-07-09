// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Text;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Garnet.common;

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
        private void HashGet(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            var key = GetByteSpanFromInput(ref input, 0);
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

        private void HashMultipleGet(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

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

        private void HashGetAll(ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            writer.WriteMapLength(Count());

            var isExpirable = HasExpirableItems();

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

        private void HashDelete(ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            for (var i = 0; i < input.parseState.Count; i++)
            {
                var key = GetByteSpanFromInput(ref input, i);
                if (Remove(key, out var hashValue))
                {
                    output.Header.result1++;
                }
            }
        }

        private void HashLength(ref GarnetObjectStoreOutput output)
        {
            output.Header.result1 = Count();
        }

        private void HashStrLength(ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            var key = GetByteSpanFromInput(ref input, 0);
            output.Header.result1 = TryGetValue(key, out var hashValue) ? hashValue.Length : 0;
        }

        private void HashExists(ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            var field = GetByteSpanFromInput(ref input, 0);
            output.Header.result1 = ContainsKey(field) ? 1 : 0;
        }

        private void HashRandomField(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            // HRANDFIELD key [count [WITHVALUES]]
            var countParameter = input.arg1 >> 2;
            var withValues = (input.arg1 & 1) == 1;
            var includedCount = ((input.arg1 >> 1) & 1) == 1;
            var seed = input.arg2;

            var countDone = 0;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

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
                writer.WriteArrayLength(withValues && (respProtocolVersion == 2) ? indexCount * 2 : indexCount);

                foreach (var index in indexes)
                {
                    var pair = ElementAt(index);

                    if ((respProtocolVersion >= 3) && withValues)
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

        private void HashSet(ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            var hop = input.header.HashOp;
            for (var i = 0; i < input.parseState.Count; i += 2)
            {
                var key = GetByteSpanFromInput(ref input, i);
                var value = input.parseState.GetArgSliceByRef(i + 1).SpanByte.AsReadOnlySpan();

                if (!TryGetValue(key, out var hashValue))
                {
                    Add(key, value.ToArray());
                    output.Header.result1++;
                }
                else if ((hop == HashOperation.HSET || hop == HashOperation.HMSET) && hashValue != default)
                {
                    if (hashValue.Length == value.Length)
                    {
                        value.CopyTo(hashValue);
                        Set(key, hashValue);
                    }
                    else
                    {
                        Set(key, value.ToArray());
                    }
                }
            }
        }

        private void HashCollect(ref ObjectInput input, ref GarnetObjectStoreOutput output)
        {
            DeleteExpiredItems();

            output.Header.result1 = 1;
        }

        private void HashGetKeysOrValues(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            var count = Count();
            var op = input.header.HashOp;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            writer.WriteArrayLength(count);

            var isExpirable = HasExpirableItems();

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
        private void HashIncrement(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            var op = input.header.HashOp;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            // This value is used to indicate partial command execution
            output.Header.result1 = int.MinValue;

            var key = GetByteSpanFromInput(ref input, 0);
            var incrSlice = input.parseState.GetArgSliceByRef(1);

            var valueExists = TryGetValue(key, out var value);
            if (op == HashOperation.HINCRBY)
            {
                if (!NumUtils.TryParse(incrSlice.ReadOnlySpan, out long incr))
                {
                    writer.WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                    return;
                }

                ReadOnlySpan<byte> resultBytes;

                if (valueExists)
                {
                    if (!NumUtils.TryParse(value, out long result))
                    {
                        writer.WriteError(CmdStrings.RESP_ERR_HASH_VALUE_IS_NOT_INTEGER);
                        return;
                    }

                    result += incr;

                    var resultSpan = (Span<byte>)stackalloc byte[NumUtils.MaximumFormatInt64Length];
                    var success = Utf8Formatter.TryFormat(result, resultSpan, out int bytesWritten,
                        format: default);
                    Debug.Assert(success);

                    resultSpan = resultSpan.Slice(0, bytesWritten);

                    if (resultSpan.Length == value.Length)
                    {
                        // instead of allocating a new byte array of the same size, we can reuse the existing one
                        resultSpan.CopyTo(value);
                        DeleteExpiredItems();
                        resultBytes = value;
                    }
                    else
                    {
                        var resultByteArray = resultSpan.ToArray();
                        SetWithoutPersist(key, resultByteArray);
                        resultBytes = resultByteArray;
                    }
                }
                else
                {
                    var resultByteArray = incrSlice.SpanByte.ToByteArray();
                    Add(key, resultByteArray);
                    resultBytes = resultByteArray;
                }

                writer.WriteIntegerFromBytes(resultBytes);
            }
            else
            {
                if (!input.parseState.TryGetDouble(1, out var incr))
                {
                    writer.WriteError(CmdStrings.RESP_ERR_NOT_VALID_FLOAT);
                    return;
                }

                if (double.IsInfinity(incr))
                {
                    writer.WriteError(CmdStrings.RESP_ERR_GENERIC_NAN_INFINITY);
                    return;
                }

                ReadOnlySpan<byte> resultBytes;

                if (valueExists)
                {
                    if (!NumUtils.TryParseWithInfinity(value, out var result))
                    {
                        writer.WriteError(CmdStrings.RESP_ERR_HASH_VALUE_IS_NOT_FLOAT);
                        return;
                    }

                    if (double.IsInfinity(result))
                    {
                        writer.WriteError(CmdStrings.RESP_ERR_GENERIC_NAN_INFINITY_INCR);
                        return;
                    }

                    result += incr;

                    var resultSpan = (Span<byte>)stackalloc byte[NumUtils.MaximumFormatDoubleLength];
                    var success = result.TryFormat(resultSpan, out int bytesWritten,
                        provider: CultureInfo.InvariantCulture);
                    Debug.Assert(success);

                    resultSpan = resultSpan.Slice(0, bytesWritten);

                    if (resultSpan.Length == value.Length)
                    {
                        // instead of allocating a new byte array of the same size, we can reuse the existing one
                        resultSpan.CopyTo(value);
                        DeleteExpiredItems();
                        resultBytes = value;
                    }
                    else
                    {
                        var resultByteArray = resultSpan.ToArray();
                        SetWithoutPersist(key, resultByteArray);
                        resultBytes = resultByteArray;
                    }
                }
                else
                {
                    var resultByteArray = incrSlice.SpanByte.ToByteArray();
                    Add(key, resultByteArray);
                    resultBytes = resultByteArray;
                }

                writer.WriteBulkString(resultBytes);
            }

            output.Header.result1 = 1;
        }

        private void HashExpire(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            DeleteExpiredItems();

            var expireOption = (ExpireOption)input.arg1;
            var expiration = input.parseState.GetLong(0);
            var numFields = input.parseState.Count - 1;
            writer.WriteArrayLength(numFields);

            foreach (var item in input.parseState.Parameters.Slice(1))
            {
#if NET9_0_OR_GREATER
                var result = SetExpiration(item.ReadOnlySpan, expiration, expireOption);
#else
                var result = SetExpiration(item.ToArray(), expiration, expireOption);
#endif
                writer.WriteInt32(result);
                output.Header.result1++;
            }
        }

        private void HashTimeToLive(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            DeleteExpiredItems();

            var isMilliseconds = input.arg1 == 1;
            var isTimestamp = input.arg2 == 1;
            var numFields = input.parseState.Count;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

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

        private void HashPersist(ref ObjectInput input, ref GarnetObjectStoreOutput output, byte respProtocolVersion)
        {
            DeleteExpiredItems();

            var numFields = input.parseState.Count;

            using var writer = new RespMemoryWriter(respProtocolVersion, ref output.SpanByteAndMemory);

            writer.WriteArrayLength(numFields);

            foreach (var item in input.parseState.Parameters)
            {
#if NET9_0_OR_GREATER
                var result = Persist(item.ReadOnlySpan);
#else
                var result = Persist(item.ToArray());
#endif
                writer.WriteInt32(result);
                output.Header.result1++;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ByteSpan GetByteSpanFromInput(ref ObjectInput input, int index)
        {
            return input.parseState.GetArgSliceByRef(index).SpanByte
#if NET9_0_OR_GREATER
                .AsReadOnlySpan();
#else
                .ToByteArray();
#endif
        }
    }
}