// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Server API methods - HASH
    /// </summary>
    sealed partial class StorageSession : IDisposable
    {
        private SingleWriterMultiReaderLock _hcollectTaskLock;

        /// <summary>
        /// HashSet: Sets the specified fields to their respective values in the hash stored at key.
        /// Values of specified fields that exist in the hash are overwritten.
        /// If key doesn't exist, a new hash is created.
        /// HashSetNX: Sets only if field does not yet exist. A new hash is created if it does not exists.
        /// If field exists the operation has no effect.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <param name="itemsDoneCount"></param>
        /// <param name="objectContext"></param>
        /// <param name="nx"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashSet<TObjectContext>(PinnedSpanByte key, PinnedSpanByte field, PinnedSpanByte value, out int itemsDoneCount, ref TObjectContext objectContext, bool nx = false)
           where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            itemsDoneCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(field, value);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Hash, RespMetaCommand.None, ref parseState) { HashOp = HashOperation.HSET };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);
            itemsDoneCount = output.result1;

            return status;
        }

        /// <summary>
        /// Sets the specified fields to their respective values in the hash stored at key.
        /// Values of specified fields that exist in the hash are overwritten.
        /// If key doesn't exist, a new hash is created.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <param name="itemsDoneCount"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashSet<TObjectContext>(PinnedSpanByte key, (PinnedSpanByte field, PinnedSpanByte value)[] elements, out int itemsDoneCount, ref TObjectContext objectContext)
           where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            itemsDoneCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.Initialize(elements.Length * 2);

            for (var i = 0; i < elements.Length; i++)
            {
                parseState.SetArguments(2 * i, isMetaArg: false, elements[i].field, elements[i].value);
            }

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Hash, RespMetaCommand.None, ref parseState) { HashOp = HashOperation.HSET };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);
            itemsDoneCount = output.result1;

            return status;
        }

        /// <summary>
        /// Removes the specified field from the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="itemsDoneCount"></param>
        /// <param name="objectContext"></param>
        /// <param name="nx"></param>
        /// <returns></returns>
        public GarnetStatus HashDelete<TObjectContext>(PinnedSpanByte key, PinnedSpanByte field, out int itemsDoneCount, ref TObjectContext objectContext, bool nx = false)
           where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
         => HashDelete(key, [field], out itemsDoneCount, ref objectContext);

        /// <summary>
        /// Removes the specified fields from the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="fields"></param>
        /// <param name="itemsDoneCount"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashDelete<TObjectContext>(PinnedSpanByte key, PinnedSpanByte[] fields, out int itemsDoneCount, ref TObjectContext objectContext)
           where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            itemsDoneCount = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(fields);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Hash, RespMetaCommand.None, ref parseState) { HashOp = HashOperation.HDEL };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);
            itemsDoneCount = output.result1;

            return status;
        }

        /// <summary>
        /// Returns the value associated with the field in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashGet<TObjectContext>(PinnedSpanByte key, PinnedSpanByte field, out PinnedSpanByte value, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            value = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(field, value);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Hash, RespMetaCommand.None, ref parseState) { HashOp = HashOperation.HGET };

            var output = new ObjectOutput();

            var status = ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

            value = default;
            if (status == GarnetStatus.OK)
                value = ProcessRespSingleTokenOutput(output);

            return status;
        }

        /// <summary>
        /// Returns the values associated with the fields in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="fields"></param>
        /// <param name="values"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashGetMultiple<TObjectContext>(PinnedSpanByte key, PinnedSpanByte[] fields, out PinnedSpanByte[] values, ref TObjectContext objectContext)
           where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            values = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(fields);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Hash, RespMetaCommand.None, ref parseState) { HashOp = HashOperation.HMGET };

            var output = new ObjectOutput();

            var status = ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

            values = default;
            if (status == GarnetStatus.OK)
                values = ProcessRespArrayOutput(output, out _);

            return status;
        }

        /// <summary>
        /// Returns all fields and values of the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="values"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashGetAll<TObjectContext>(PinnedSpanByte key, out PinnedSpanByte[] values, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            values = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Hash) { HashOp = HashOperation.HGETALL };

            var output = new ObjectOutput();

            var status = ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

            values = default;
            if (status == GarnetStatus.OK)
                values = ProcessRespArrayOutput(output, out _);

            return status;
        }

        /// <summary>
        /// Returns the number of fields contained in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="items"></param>
        /// <param name="objectContext"></param>
        /// <param name="nx"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashLength<TObjectContext>(PinnedSpanByte key, out int items, ref TObjectContext objectContext, bool nx = false)
          where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            items = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Hash) { HashOp = HashOperation.HLEN };

            var status = ReadObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);

            items = output.result1;

            return status;
        }

        /// <summary>
        /// Returns if field exists in the hash stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="exists"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashExists<TObjectContext>(PinnedSpanByte key, PinnedSpanByte field, out bool exists, ref TObjectContext objectContext)
         where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            exists = false;
            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArgument(field);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Hash, RespMetaCommand.None, ref parseState) { HashOp = HashOperation.HEXISTS };

            var status = ReadObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);

            exists = output.result1 == 1;

            return status;
        }

        /// <summary>
        /// Returns a random field from the hash value stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashRandomField<TObjectContext>(PinnedSpanByte key, out PinnedSpanByte field, ref TObjectContext objectContext)
           where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            field = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Create a random seed
            var seed = Random.Shared.Next();

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.Hash, arg1: 1 << 2, arg2: seed) { HashOp = HashOperation.HRANDFIELD };

            var output = new ObjectOutput();

            var status = ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

            // Process output
            if (status == GarnetStatus.OK)
                field = ProcessRespSingleTokenOutput(output);

            return status;
        }

        /// <summary>
        /// Returns an array of distinct fields, the lenght is either count or the len of the hash,
        /// whichever is lower. If count is negative, it is allowed to return ssame field multiple times,
        /// in this case the fields returned is the absolute value of count.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <param name="withValues"></param>
        /// <param name="fields"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashRandomField<TObjectContext>(PinnedSpanByte key, int count, bool withValues, out PinnedSpanByte[] fields, ref TObjectContext objectContext)
           where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            fields = default;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Create a random seed
            var seed = Random.Shared.Next();

            // Prepare the input
            var inputArg = (((count << 1) | 1) << 1) | (withValues ? 1 : 0);
            var input = new ObjectInput(GarnetObjectType.Hash, arg1: inputArg, arg2: seed) { HashOp = HashOperation.HRANDFIELD };

            var output = new ObjectOutput();
            var status = ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

            fields = default;
            if (status == GarnetStatus.OK)
                fields = ProcessRespArrayOutput(output, out _);

            return status;
        }

        /// <summary>
        /// Sets the specified fields to their respective values in the hash stored at key.
        /// Values of specified fields that exist in the hash are overwritten.
        /// If key doesn't exist, a new hash is created.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashSet<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out output, ref objectContext);

        /// <summary>
        /// HashGet: Returns the value associated with field in the hash stored at key.
        /// HashGetAll: Returns all fields and values of the hash stored at key.
        /// HashGetMultiple: Returns the values associated with the specified fields in the hash stored at key.
        /// HashRandomField: Returns a random field from the hash value stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashGet<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns all fields and values of the hash stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashGetAll<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns the values associated with the specified fields in the hash stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashGetMultiple<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns a random field from the hash value stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashRandomField<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns the number of fields contained in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashLength<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperation(key.ReadOnlySpan, ref input, out output, ref objectContext);

        /// <summary>
        /// Returns the string length of the value associated with field in the hash stored at key. If the key or the field do not exist, 0 is returned.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <returns></returns>
        public GarnetStatus HashStrLength<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperation(key.ReadOnlySpan, ref input, out output, ref objectContext);

        /// <summary>
        /// Removes the specified fields from the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashDelete<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out output, ref objectContext);

        /// <summary>
        /// Returns if field exists in the hash stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashExists<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, out OutputHeader output, ref TObjectContext objectContext)
         where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperation(key.ReadOnlySpan, ref input, out output, ref objectContext);

        /// <summary>
        /// Returns all field names in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashKeys<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns all values in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashVals<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Increments the number stored at field in the hash stored at key by increment.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashIncrement<TObjectContext>(PinnedSpanByte key, PinnedSpanByte input, out OutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperation(key.ReadOnlySpan, input, out output, ref objectContext);

        /// <summary>
        /// HashIncrementByFloat: Increment the specified field of a hash stored at key,
        /// and representing a floating point number, by the specified increment.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashIncrement<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Sets the expiration time for the specified key.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="key">The key for which to set the expiration time.</param>
        /// <param name="input">The input object containing the operation details.</param>
        /// <param name="output">The output  object to store the result.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        public GarnetStatus HashExpire<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Returns the time-to-live (TTL) of a hash key.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="key">The key of the hash.</param>
        /// <param name="input">The input object containing the operation details.</param>
        /// <param name="output">The output  object to store the result.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        public GarnetStatus HashTimeToLive<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            return ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);
        }

        /// <summary>
        /// Removes the expiration time from a hash key, making it persistent.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="key">The key of the hash.</param>
        /// <param name="input">The input object containing the operation details.</param>
        /// <param name="output">The output  object to store the result.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        public GarnetStatus HashPersist<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Collects hash keys and performs a specified operation on them.
        /// </summary>
        /// <typeparam name="TObjectContext">The type of the object context.</typeparam>
        /// <param name="keys">The keys to collect.</param>
        /// <param name="input">The input object containing the operation details.</param>
        /// <param name="objectContext">The object context for the operation.</param>
        /// <returns>The status of the operation.</returns>
        /// <remarks>
        /// If the first key is "*", all hash keys are scanned in batches and the operation is performed on each key.
        /// Otherwise, the operation is performed on the specified keys.
        /// </remarks>
        public unsafe GarnetStatus HashCollect<TObjectContext>(ReadOnlySpan<PinnedSpanByte> keys, ref ObjectInput input, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            if (keys[0].ReadOnlySpan.SequenceEqual("*"u8))
                return ObjectCollect(keys[0], CmdStrings.HASH, _hcollectTaskLock, ref input, ref objectContext);

            foreach (var key in keys)
                RMWObjectStoreOperation(key.ToArray(), ref input, out _, ref objectContext);

            return GarnetStatus.OK;
        }
    }
}