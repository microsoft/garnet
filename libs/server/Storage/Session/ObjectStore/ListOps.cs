// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using Tsavorite.core;

namespace Garnet.server
{
    sealed partial class StorageSession : IDisposable
    {
        /// <summary>
        /// Adds new elements at the head(right) or tail(left)
        /// in the list stored at key.
        /// For the case of ListPushX, the operation is only done if the key already exists
        /// and holds a list.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key">The name of the key</param>
        /// <param name="elements">The elements to be added at the left or the righ of the list</param>
        /// <param name="lop">The Right or Left modifier of the operation to perform</param>
        /// <param name="itemsDoneCount">The length of the list after the push operations.</param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListPush<TObjectContext>(ArgSlice key, ArgSlice[] elements, ListOperation lop, out int itemsDoneCount, ref TObjectContext objectStoreContext)
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            itemsDoneCount = 0;

            if (key.Bytes.Length == 0 || elements.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in buffer
            var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            rmwInput->header.type = GarnetObjectType.List;
            rmwInput->header.ListOp = lop;
            rmwInput->count = elements.Length;
            rmwInput->done = 0;

            //Iterate through all inputs and add them to the scratch buffer in RESP format
            int inputLength = sizeof(ObjectInputHeader);
            foreach (var item in elements)
            {
                var tmp = scratchBufferManager.FormatScratchAsResp(0, item);
                inputLength += tmp.length;
            }

            var input = scratchBufferManager.GetSliceFromTail(inputLength);
            RMWObjectStoreOperation(key.Bytes, input, out var output, ref objectStoreContext);

            itemsDoneCount = output.countDone;
            return GarnetStatus.OK;
        }

        /// <summary>
        /// Adds new elements at the head(right) or tail(left)
        /// in the list stored at key.
        /// For the case of ListPushX, the operation is only done if the key already exists
        /// and holds a list.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="element"></param>
        /// <param name="lop"></param>
        /// <param name="itemsDoneCount"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListPush<TObjectContext>(ArgSlice key, ArgSlice element, ListOperation lop, out int itemsDoneCount, ref TObjectContext objectStoreContext)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            itemsDoneCount = 0;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, element);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.List;
            rmwInput->header.ListOp = lop;
            rmwInput->count = 1;
            rmwInput->done = 0;

            var status = RMWObjectStoreOperation(key.Bytes, element, out var output, ref objectStoreContext);
            itemsDoneCount = output.countDone;

            return status;
        }

        /// <summary>
        /// Removes one element from the head(left) or tail(right) 
        /// of the list stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="lop"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="element"></param>
        /// <returns>The popped element</returns>
        public GarnetStatus ListPop<TObjectContext>(ArgSlice key, ListOperation lop, ref TObjectContext objectStoreContext, out ArgSlice element)
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            var status = ListPop(key, 1, lop, ref objectStoreContext, out var elements);
            element = elements.FirstOrDefault();
            return status;
        }

        /// <summary>
        /// Removes the count elements from the head(left) or tail(right) of the list stored at key.
        /// If the list contains less than count elements, removes and returns the number of elements in the list.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="count"></param>
        /// <param name="lop"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="elements"></param>
        /// <returns>The count elements popped from the list</returns>
        public unsafe GarnetStatus ListPop<TObjectContext>(ArgSlice key, int count, ListOperation lop, ref TObjectContext objectStoreContext, out ArgSlice[] elements)
                 where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            var _key = key.Bytes;
            SpanByte _keyAsSpan = key.SpanByte;

            // Construct input for operation
            var input = scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.List;
            rmwInput->header.ListOp = lop;
            rmwInput->count = count;
            rmwInput->done = 0;

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = RMWObjectStoreOperationWithOutput(key.Bytes, input, ref objectStoreContext, ref outputFooter);

            //process output
            elements = default;
            if (status == GarnetStatus.OK)
                elements = ProcessRespArrayOutput(outputFooter, out var error);

            return GarnetStatus.OK;
        }

        /// <summary>
        /// Gets the current count of elements in the List at Key
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="objectStoreContext"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListLength<TObjectContext>(ArgSlice key, ref TObjectContext objectStoreContext, out int count)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            count = 0;

            if (key.Bytes.Length == 0)
                return GarnetStatus.OK;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.List;
            rmwInput->header.ListOp = ListOperation.LLEN;
            rmwInput->count = count;
            rmwInput->done = 0;

            var status = ReadObjectStoreOperation(key.Bytes, input, out var output, ref objectStoreContext);

            count = output.countDone;
            return status;
        }

        /// <summary>
        /// Removes the first/last element of the list stored at source
        /// and pushes it to the first/last element of the list stored at destination
        /// </summary>
        /// <param name="sourceKey"></param>
        /// <param name="destinationKey"></param>
        /// <param name="sourceDirection"></param>
        /// <param name="destinationDirection"></param>
        /// <param name="element">out parameter, The element being popped and pushed</param>
        /// <returns>true when success</returns>
        public bool ListMove(ArgSlice sourceKey, ArgSlice destinationKey, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element)
        {
            element = default;
            var objectLockableContext = txnManager.ObjectStoreLockableContext;

            //If source and destination are the same, the operation is equivalent to removing the last element from the list
            //and pushing it as first element of the list, so it can be considered as a list rotation command.
            bool sameKey = sourceKey.Bytes.SequenceEqual(destinationKey.Bytes);

            bool createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                txnManager.SaveKeyEntryToLock(sourceKey, true, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(destinationKey, true, LockType.Exclusive);
                txnManager.Run(true);
            }

            var objectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                // get the source key
                var statusOp = GET(sourceKey.Bytes, out var sourceList, ref objectLockableContext);

                if (statusOp == GarnetStatus.NOTFOUND || ((ListObject)sourceList.garnetObject).LnkList.Count == 0)
                {
                    return true;
                }
                else if (statusOp == GarnetStatus.OK)
                {
                    var srcListObject = (ListObject)sourceList.garnetObject;

                    // right pop (removelast) from source
                    if (sourceDirection == OperationDirection.Right)
                    {
                        element = srcListObject.LnkList.Last.Value;
                        srcListObject.LnkList.RemoveLast();
                    }
                    else
                    {
                        // left pop (removefirst) from source
                        element = srcListObject.LnkList.First.Value;
                        srcListObject.LnkList.RemoveFirst();
                    }
                    srcListObject.UpdateSize(element, false);

                    //update sourcelist
                    SET(sourceKey.Bytes, sourceList.garnetObject, ref objectStoreLockableContext);

                    IGarnetObject newListValue = null;
                    if (!sameKey)
                    {
                        // read destination key
                        var _destinationKey = destinationKey.ReadOnlySpan.ToArray();
                        statusOp = GET(_destinationKey, out var destinationList, ref objectStoreLockableContext);

                        if (statusOp == GarnetStatus.NOTFOUND)
                        {
                            destinationList.garnetObject = new ListObject();
                        }

                        var dstListObject = (ListObject)destinationList.garnetObject;

                        //left push (addfirst) to destination
                        if (destinationDirection == OperationDirection.Left)
                            dstListObject.LnkList.AddFirst(element);
                        else
                            dstListObject.LnkList.AddLast(element);

                        dstListObject.UpdateSize(element);
                        newListValue = new ListObject(dstListObject.LnkList, dstListObject.Expiration, dstListObject.Size);
                    }
                    else
                    {
                        // when the source and the destination key is the same the operation is done only in the sourceList
                        if (sourceDirection == OperationDirection.Right && destinationDirection == OperationDirection.Left)
                            srcListObject.LnkList.AddFirst(element);
                        else if (sourceDirection == OperationDirection.Left && destinationDirection == OperationDirection.Right)
                            srcListObject.LnkList.AddLast(element);
                        newListValue = sourceList.garnetObject;
                        ((ListObject)newListValue).UpdateSize(element);
                    }

                    // upsert
                    SET(destinationKey.Bytes, newListValue, ref objectStoreLockableContext);
                }
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }

            return true;

        }

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="start"></param>
        /// <param name="stop"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns>true when successful</returns>
        public unsafe bool ListTrim<TObjectContext>(ArgSlice key, int start, int stop, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
        {
            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.List;
            rmwInput->header.ListOp = ListOperation.LTRIM;
            rmwInput->count = start;
            rmwInput->done = stop;

            var status = RMWObjectStoreOperation(key.Bytes, input, out var output, ref objectStoreContext);

            return status == GarnetStatus.OK;
        }

        /// <summary>
        /// Adds new elements at the head(right) or tail(left)
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus ListPush<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus ListTrim<TObjectContext>(byte[] key, ArgSlice input, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperation(key, input, out _, ref objectStoreContext);

        /// <summary>
        /// Gets the specified elements of the list stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus ListRange<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// Inserts a new element in the list stored at key either before or after a value pivot
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus ListInsert<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

        /// <summary>
        /// Returns the element at index.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus ListIndex<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => ReadObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// Removes the first count occurrences of elements equal to element from the list.
        /// LREM key count element
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus ListRemove<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);

        /// <summary>
        /// Removes the count elements from the head(left) or tail(right) of the list stored at key.
        /// If the list contains less than count elements, removes and returns the number of elements in the list.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListPop<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
            => RMWObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// Removes the count elements from the head(left) or tail(right) of the list stored at key.
        /// If the list contains less than count elements, removes and returns the number of elements in the list.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListLength<TObjectContext>(byte[] key, ArgSlice input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long>
             => ReadObjectStoreOperation(key, input, out output, ref objectStoreContext);

    }
}