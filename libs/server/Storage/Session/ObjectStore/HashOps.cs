// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
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
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <param name="itemsDoneCount"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="nx"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashSet<TObjectContext>(ArgSlice key, ArgSlice field, ArgSlice value, out int itemsDoneCount, ref TObjectContext objectStoreContext, bool nx = false)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            itemsDoneCount = 0;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, field, value);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.Hash;
            rmwInput->header.HashOp = nx ? HashOperation.HSETNX : HashOperation.HSET;
            rmwInput->count = 1;
            rmwInput->done = 0;

            RMWObjectStoreOperation(key.Bytes, input, out var output, ref objectStoreContext);

            itemsDoneCount = output.opsDone;
            return GarnetStatus.OK;
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
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashSet<TObjectContext>(ArgSlice key, (ArgSlice field, ArgSlice value)[] elements, out int itemsDoneCount, ref TObjectContext objectStoreContext)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            itemsDoneCount = 0;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in buffer
            var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            rmwInput->header.type = GarnetObjectType.Hash;
            rmwInput->header.HashOp = HashOperation.HSET;
            rmwInput->count = elements.Length;
            rmwInput->done = 0;

            // Iterate through all inputs and add them to the scratch buffer in RESP format
            int inputLength = sizeof(ObjectInputHeader);
            foreach (var pair in elements)
            {
                var tmp = scratchBufferManager.FormatScratchAsResp(0, pair.field, pair.value);
                inputLength += tmp.length;
            }

            var input = scratchBufferManager.GetSliceFromTail(inputLength);

            var status = RMWObjectStoreOperation(key.Bytes, input, out var output, ref objectStoreContext);
            itemsDoneCount = output.opsDone;

            return status;
        }

        /// <summary>
        /// Removes the specified field from the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="itemsDoneCount"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="nx"></param>
        /// <returns></returns>
        public GarnetStatus HashDelete<TObjectContext>(ArgSlice key, ArgSlice field, out int itemsDoneCount, ref TObjectContext objectStoreContext, bool nx = false)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
         => HashDelete(key, new ArgSlice[] { field }, out itemsDoneCount, ref objectStoreContext);

        /// <summary>
        /// Removes the specified fields from the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="fields"></param>
        /// <param name="itemsDoneCount"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashDelete<TObjectContext>(ArgSlice key, ArgSlice[] fields, out int itemsDoneCount, ref TObjectContext objectStoreContext)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            itemsDoneCount = 0;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in buffer
            var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            rmwInput->header.type = GarnetObjectType.Hash;
            rmwInput->header.HashOp = HashOperation.HDEL;
            rmwInput->count = fields.Length;
            rmwInput->done = 0;

            // Iterate through all inputs and add them to the scratch buffer in RESP format
            int inputLength = sizeof(ObjectInputHeader);
            foreach (var field in fields)
            {
                var tmp = scratchBufferManager.FormatScratchAsResp(0, field);
                inputLength += tmp.length;
            }

            var input = scratchBufferManager.GetSliceFromTail(inputLength);

            var status = RMWObjectStoreOperation(key.Bytes, input, out var output, ref objectStoreContext);
            itemsDoneCount = output.opsDone;

            return status;
        }

        /// <summary>
        /// Returns all fields and values of the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="values"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus HashGetAll<TObjectContext>(ArgSlice key, out ArgSlice[] values, ref TObjectContext objectStoreContext)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => HashGet(key, default, out values, ref objectStoreContext);

        /// <summary>
        /// Returns the value associated with field in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus HashGet<TObjectContext>(ArgSlice key, ArgSlice field, out ArgSlice value, ref TObjectContext objectStoreContext)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            var status = HashGet(key, new ArgSlice[] { field }, out var values, ref objectStoreContext);
            value = values.FirstOrDefault();

            return status;
        }

        /// <summary>
        /// Returns the values associated with the fields in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="fields"></param>
        /// <param name="values"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashGet<TObjectContext>(ArgSlice key, ArgSlice[] fields, out ArgSlice[] values, ref TObjectContext objectStoreContext)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            values = default;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            rmwInput->header.type = GarnetObjectType.Hash;
            rmwInput->header.HashOp = fields == default ? HashOperation.HGETALL : HashOperation.HGET;
            rmwInput->count = fields == default ? 0 : fields.Length;
            rmwInput->done = 0;

            // Iterate through all inputs and add them to the scratch buffer in RESP format
            int inputLength = sizeof(ObjectInputHeader);

            if (rmwInput->header.HashOp != HashOperation.HGETALL)
            {
                foreach (var field in fields)
                {
                    var tmp = scratchBufferManager.FormatScratchAsResp(0, field);
                    inputLength += tmp.length;
                }
            }

            var input = scratchBufferManager.GetSliceFromTail(inputLength);
            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = ReadObjectStoreOperationWithOutput(key.Bytes, input, ref objectStoreContext, ref outputFooter);

            values = default;
            if (status == GarnetStatus.OK)
                values = ProcessRespArrayOutput(outputFooter, out _);

            return status;
        }

        /// <summary>
        /// Returns the number of fields contained in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="items"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="nx"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashLength<TObjectContext>(ArgSlice key, out int items, ref TObjectContext objectStoreContext, bool nx = false)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            items = 0;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.Hash;
            rmwInput->header.HashOp = HashOperation.HLEN;
            rmwInput->count = 1;
            rmwInput->done = 0;

            ReadObjectStoreOperation(key.Bytes, input, out var output, ref objectStoreContext);

            items = output.countDone;
            return GarnetStatus.OK;
        }

        /// <summary>
        /// Returns if field exists in the hash stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="exists"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashExists<TObjectContext>(ArgSlice key, ArgSlice field, out bool exists, ref TObjectContext objectStoreContext)
         where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            exists = false;
            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.Hash;
            rmwInput->header.HashOp = HashOperation.HEXISTS;
            rmwInput->count = 1;
            rmwInput->done = 0;

            ReadObjectStoreOperation(key.Bytes, input, out var output, ref objectStoreContext);

            exists = output.countDone == 1;

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Returns a random field from the hash value stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashRandomField<TObjectContext>(ArgSlice key, out ArgSlice field, ref TObjectContext objectStoreContext)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            field = default;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            rmwInput->header.type = GarnetObjectType.Hash;
            rmwInput->header.HashOp = HashOperation.HRANDFIELD;
            rmwInput->count = 2;
            rmwInput->done = 0;

            int inputLength = sizeof(ObjectInputHeader);

            var input = scratchBufferManager.GetSliceFromTail(inputLength);

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = ReadObjectStoreOperationWithOutput(key.Bytes, input, ref objectStoreContext, ref outputFooter);

            //process output
            if (status == GarnetStatus.OK)
                field = ProcessRespArrayOutput(outputFooter, out _).FirstOrDefault();

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
        /// <param name="withvalues"></param>
        /// <param name="fields"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus HashRandomField<TObjectContext>(ArgSlice key, int count, bool withvalues, out ArgSlice[] fields, ref TObjectContext objectStoreContext)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            fields = default;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            rmwInput->header.type = GarnetObjectType.Hash;
            rmwInput->header.HashOp = HashOperation.HRANDFIELD;
            rmwInput->count = 4;
            rmwInput->done = 0;

            // Iterate through all inputs and add them to the scratch buffer in RESP format
            int inputLength = sizeof(ObjectInputHeader);

            ArgSlice countArgSlice;
            ArgSlice withValuesArgSlice;

            // write count
            var countBytes = Encoding.ASCII.GetBytes(count.ToString());
            fixed (byte* countPtr = countBytes)
            {
                countArgSlice = new ArgSlice(countPtr, countBytes.Length);
            }
            var tmp = scratchBufferManager.FormatScratchAsResp(0, countArgSlice);
            inputLength += tmp.length;

            //write withvalues
            ReadOnlySpan<byte> withValuesBytes = "WITHVALUES"u8;
            fixed (byte* withValuesPtr = withValuesBytes)
            {
                withValuesArgSlice = new ArgSlice(withValuesPtr, withValuesBytes.Length);
            }
            tmp = scratchBufferManager.FormatScratchAsResp(0, withValuesArgSlice);
            inputLength += tmp.length;

            var input = scratchBufferManager.GetSliceFromTail(inputLength);

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            var status = ReadObjectStoreOperationWithOutput(key.Bytes, input, ref objectStoreContext, ref outputFooter);

            fields = default;
            if (status == GarnetStatus.OK)
                fields = ProcessRespArrayOutput(outputFooter, out _);

            return status;
        }


        /// <summary>
        /// Iterates fields of Hash key and their associated values using a cursor,
        /// a match pattern and count parameters
        /// </summary>
        /// <param name="key"></param>
        /// <param name="cursor"></param>
        /// <param name="match"></param>
        /// <param name="count"></param>
        /// <param name="items"></param>
        /// <param name="objectStoreContext"></param>
        public unsafe GarnetStatus HashScan<TObjectContext>(ArgSlice key, long cursor, string match, long count, out ArgSlice[] items, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            items = default;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            if (String.IsNullOrEmpty(match))
                match = "*";

            // Prepare header in input buffer
            // Header + ObjectScanCountLimit
            var inputSize = ObjectInputHeader.Size + sizeof(int);
            var rmwInput = scratchBufferManager.CreateArgSlice(inputSize).ptr;
            ((ObjectInputHeader*)rmwInput)->header.type = GarnetObjectType.Hash;
            ((ObjectInputHeader*)rmwInput)->header.HashOp = HashOperation.HSCAN;

            // Number of tokens in the input after the header (match, value, count, value)
            ((ObjectInputHeader*)rmwInput)->count = 4;
            ((ObjectInputHeader*)rmwInput)->done = (int)cursor;
            rmwInput += ObjectInputHeader.Size;

            // Object Input Limit
            (*(int*)rmwInput) = ObjectScanCountLimit;
            int inputLength = sizeof(ObjectInputHeader) + sizeof(int);

            ArgSlice tmp;

            // Write match
            var matchKeywordBytes = CmdStrings.MATCH;
            var matchPatternValue = Encoding.ASCII.GetBytes(match.Trim());
            fixed (byte* matchKeywordPtr = matchKeywordBytes, matchPatterPtr = matchPatternValue)
            {
                tmp = scratchBufferManager.FormatScratchAsResp(0, new ArgSlice(matchKeywordPtr, matchKeywordBytes.Length),
                            new ArgSlice(matchPatterPtr, matchPatternValue.Length));
            }
            inputLength += tmp.length;

            // Write count
            var countKeywordBytes = CmdStrings.COUNT;
            var countBytes = Encoding.ASCII.GetBytes(count.ToString());
            fixed (byte* countPtr = countKeywordBytes, countValuePtr = countBytes)
            {
                tmp = scratchBufferManager.FormatScratchAsResp(0, new ArgSlice(countPtr, countKeywordBytes.Length),
                          new ArgSlice(countValuePtr, countBytes.Length));
            }
            inputLength += tmp.length;

            var input = scratchBufferManager.GetSliceFromTail(inputLength);

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            var status = ReadObjectStoreOperationWithOutput(key.Bytes, input, ref objectStoreContext, ref outputFooter);

            items = default;
            if (status == GarnetStatus.OK)
                items = ProcessRespArrayOutput(outputFooter, out _, isScanOutput: true);

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
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus HashSet<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

        /// <summary>
        /// HashGet: Returns the value associated with field in the hash stored at key.
        /// HashGetAll: Returns all fields and values of the hash stored at key.
        /// HashGetMultiple: Returns the values associated with the specified fields in the hash stored at key.
        /// HashRandomField: Returns a random field from the hash value stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus HashGet<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// Returns the number of fields contained in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus HashLength<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => ReadObjectStoreOperation(key, input, out output, ref objectStoreContext);

        /// <summary>
        /// Removes the specified fields from the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus HashDelete<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

        /// <summary>
        /// Returns if field exists in the hash stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus HashExists<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
         where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => ReadObjectStoreOperation(key, input, out output, ref objectStoreContext);

        /// <summary>
        /// Returns all field names in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashKeys<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => ReadObjectStoreOperationWithOutput(key, input, ref objectContext, ref outputFooter);

        /// <summary>
        /// Returns all values in the hash key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashVals<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => ReadObjectStoreOperationWithOutput(key, input, ref objectContext, ref outputFooter);

        /// <summary>
        /// Increments the number stored at field in the hash stored at key by increment.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashIncrement<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperation(key, input, out output, ref objectContext);

        /// <summary>
        /// HashIncrementByFloat: Increment the specified field of a hash stored at key,
        /// and representing a floating point number, by the specified increment.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus HashIncrement<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperationWithOutput(key, input, ref objectContext, ref outputFooter);
    }
}