// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Server API methods - HASH
    /// </summary>
    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// HashSet: Sets the specified fields to their respective values in the hash stored at key.
        /// Values of specified fields that exist in the hash are overwritten.
        /// If key doesn't exist, a new hash is created.
        /// HashSetNX: Sets only if field does not yet exist. A new hash is created if it does not exists.
        /// If field exists the operation has no effect.
        /// </summary>
        public GarnetStatus HashSet<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, ArgSlice value, out int itemsDoneCount, bool nx = false)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            itemsDoneCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, field, value);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = nx ? HashOperation.HSETNX : HashOperation.HSET,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);
            itemsDoneCount = output.result1;

            return status;
        }

        /// <summary>
        /// Sets the specified fields to their respective values in the hash stored at key.
        /// Values of specified fields that exist in the hash are overwritten.
        /// If key doesn't exist, a new hash is created.
        /// </summary>
        public GarnetStatus HashSet<TKeyLocker, TEpochGuard>(ArgSlice key, (ArgSlice field, ArgSlice value)[] elements, out int itemsDoneCount)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            itemsDoneCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.Initialize(ref parseStateBuffer, elements.Length * 2);

            for (var i = 0; i < elements.Length; i++)
            {
                parseStateBuffer[2 * i] = elements[i].field;
                parseStateBuffer[(2 * i) + 1] = elements[i].value;
            }

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HSET,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);
            itemsDoneCount = output.result1;

            return status;
        }

        /// <summary>
        /// Removes the specified field from the hash key.
        /// </summary>
        public GarnetStatus HashDelete<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, out int itemsDoneCount, bool nx = false)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
         => HashDelete<TKeyLocker, TEpochGuard>(key, [field], out itemsDoneCount);

        /// <summary>
        /// Removes the specified fields from the hash key.
        /// </summary>
        public GarnetStatus HashDelete<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] fields, out int itemsDoneCount)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            itemsDoneCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, fields);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HDEL,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);
            itemsDoneCount = output.result1;

            return status;
        }

        /// <summary>
        /// Returns the value associated with the field in the hash key.
        /// </summary>
        public GarnetStatus HashGet<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, out ArgSlice value)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            value = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, field, value);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HGET,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, ref outputFooter);
            value = status == GarnetStatus.OK ? ProcessRespSingleTokenOutput(outputFooter) : default;
            return status;
        }

        /// <summary>
        /// Returns the values associated with the fields in the hash key.
        /// </summary>
        public GarnetStatus HashGetMultiple<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            values = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, fields);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HMGET,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, ref outputFooter);
            values = status == GarnetStatus.OK ? ProcessRespArrayOutput(outputFooter, out _) : default;
            return status;
        }

        /// <summary>
        /// Returns all fields and values of the hash key.
        /// </summary>
        public GarnetStatus HashGetAll<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice[] values)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            values = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HGETALL,
                },
                arg1 = 2 // Default RESP protocol version
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, ref outputFooter);
            values = status == GarnetStatus.OK ? ProcessRespArrayOutput(outputFooter, out _) : default;
            return status;
        }

        public GarnetStatus HashLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int items, bool nx = false)   // TODO implement nx
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            items = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HLEN,
                },
            };

            var status = ReadObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);
            items = output.result1;
            return status;
        }

        /// <summary>
        /// Returns if field exists in the hash stored at key.
        /// </summary>
        public GarnetStatus HashExists<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice field, out bool exists)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            exists = false;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, field);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HEXISTS,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var status = ReadObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);
            exists = output.result1 == 1;
            return status;
        }

        /// <summary>
        /// Returns a random field from the hash value stored at key.
        /// </summary>
        public GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(ArgSlice key, out ArgSlice field)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            field = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Create a random seed
            var seed = Random.Shared.Next();

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HRANDFIELD,
                },
                arg1 = 1 << 2,
                arg2 = seed,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, ref outputFooter);

            // Process output
            if (status == GarnetStatus.OK)
                field = ProcessRespSingleTokenOutput(outputFooter);
            return status;
        }

        /// <summary>
        /// Returns an array of distinct fields, the lenght is either count or the len of the hash,
        /// whichever is lower. If count is negative, it is allowed to return ssame field multiple times,
        /// in this case the fields returned is the absolute value of count.
        /// </summary>
        public GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(ArgSlice key, int count, bool withValues, out ArgSlice[] fields)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            fields = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Create a random seed
            var seed = Random.Shared.Next();

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.Hash,
                    HashOp = HashOperation.HRANDFIELD,
                },
                arg1 = (((count << 1) | 1) << 1) | (withValues ? 1 : 0),
                arg2 = seed,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            var status = ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, ref outputFooter);

            fields = default;
            if (status == GarnetStatus.OK)
                fields = ProcessRespArrayOutput(outputFooter, out _);

            return status;
        }

        /// <summary>
        /// Sets the specified fields to their respective values in the hash stored at key.
        /// Values of specified fields that exist in the hash are overwritten.
        /// If key doesn't exist, a new hash is created.
        /// </summary>
        public GarnetStatus HashSet<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <summary>
        /// HashGet: Returns the value associated with field in the hash stored at key.
        /// HashGetAll: Returns all fields and values of the hash stored at key.
        /// HashGetMultiple: Returns the values associated with the specified fields in the hash stored at key.
        /// HashRandomField: Returns a random field from the hash value stored at key.
        /// </summary>
        public GarnetStatus HashGet<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// Returns all fields and values of the hash stored at key.
        /// </summary>
        public GarnetStatus HashGetAll<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// Returns the values associated with the specified fields in the hash stored at key.
        /// </summary>
        public GarnetStatus HashGetMultiple<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// Returns a random field from the hash value stored at key.
        /// </summary>
        public GarnetStatus HashRandomField<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// Returns the number of fields contained in the hash key.
        /// </summary>
        public GarnetStatus HashLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <summary>
        /// Returns the string length of the value associated with field in the hash stored at key. If the key or the field do not exist, 0 is returned.
        /// </summary>
        public GarnetStatus HashStrLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <summary>
        /// Removes the specified fields from the hash key.
        /// </summary>
        public GarnetStatus HashDelete<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <summary>
        /// Returns if field exists in the hash stored at key.
        /// </summary>
        public GarnetStatus HashExists<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <summary>
        /// Returns all field names in the hash key.
        /// </summary>
        public GarnetStatus HashKeys<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// Returns all values in the hash key.
        /// </summary>
        public GarnetStatus HashVals<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// Increments the number stored at field in the hash stored at key by increment.
        /// </summary>
        public GarnetStatus HashIncrement<TKeyLocker, TEpochGuard>(byte[] key, ArgSlice input, out ObjectOutputHeader output)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key, input, out output);

        /// <summary>
        /// HashIncrementByFloat: Increment the specified field of a hash stored at key,
        /// and representing a floating point number, by the specified increment.
        /// </summary>
        public GarnetStatus HashIncrement<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMWObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
    }
}