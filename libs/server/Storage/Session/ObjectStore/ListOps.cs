﻿// Copyright (c) Microsoft Corporation.
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
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            itemsDoneCount = 0;

            if (key.Length == 0 || elements.Length == 0)
                return GarnetStatus.OK;

            // Prepare header in buffer
            var rmwInput = (ObjectInputHeader*)scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size).ptr;
            rmwInput->header.type = GarnetObjectType.List;
            rmwInput->header.flags = 0;
            rmwInput->header.ListOp = lop;
            rmwInput->arg1 = elements.Length;

            //Iterate through all inputs and add them to the scratch buffer in RESP format
            int inputLength = sizeof(ObjectInputHeader);
            foreach (var item in elements)
            {
                var tmp = scratchBufferManager.FormatScratchAsResp(0, item);
                inputLength += tmp.Length;
            }

            var input = scratchBufferManager.GetSliceFromTail(inputLength);
            var arrKey = key.ToArray();
            var status = RMWObjectStoreOperation(arrKey, input, out var output, ref objectStoreContext);

            itemsDoneCount = output.result1;
            itemBroker.HandleCollectionUpdate(arrKey);
            return status;
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
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            itemsDoneCount = 0;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, element);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.List;
            rmwInput->header.flags = 0;
            rmwInput->header.ListOp = lop;
            rmwInput->arg1 = 1;

            var status = RMWObjectStoreOperation(key.ToArray(), element, out var output, ref objectStoreContext);
            itemsDoneCount = output.result1;

            itemBroker.HandleCollectionUpdate(key.Span.ToArray());
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
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
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
                 where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            var _key = key.ToArray();
            SpanByte _keyAsSpan = key.SpanByte;

            // Construct input for operation
            var input = scratchBufferManager.CreateArgSlice(ObjectInputHeader.Size);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.List;
            rmwInput->header.flags = 0;
            rmwInput->header.ListOp = lop;
            rmwInput->arg1 = count;

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), input, ref objectStoreContext, ref outputFooter);

            //process output
            elements = default;
            if (status == GarnetStatus.OK)
                elements = ProcessRespArrayOutput(outputFooter, out var error);

            return status;
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            count = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.List;
            rmwInput->header.flags = 0;
            rmwInput->header.ListOp = ListOperation.LLEN;
            rmwInput->arg1 = count;

            var status = ReadObjectStoreOperation(key.ToArray(), input, out var output, ref objectStoreContext);

            count = output.result1;
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
        /// <returns>GarnetStatus</returns>
        public GarnetStatus ListMove(ArgSlice sourceKey, ArgSlice destinationKey, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element)
        {
            element = default;
            var objectLockableContext = txnManager.ObjectStoreLockableContext;

            // If source and destination are the same, the operation is equivalent to removing the last element from the list
            // and pushing it as first element of the list, so it can be considered as a list rotation command.
            bool sameKey = sourceKey.ReadOnlySpan.SequenceEqual(destinationKey.ReadOnlySpan);

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
                // Get the source key
                var statusOp = GET(sourceKey.ToArray(), out var sourceList, ref objectLockableContext);

                if (statusOp == GarnetStatus.NOTFOUND)
                {
                    return GarnetStatus.OK;
                }
                else if (statusOp == GarnetStatus.OK)
                {
                    if (sourceList.garnetObject is not ListObject srcListObject)
                        return GarnetStatus.WRONGTYPE;

                    if (srcListObject.LnkList.Count == 0)
                        return GarnetStatus.OK;

                    ListObject dstListObject = default;
                    if (!sameKey)
                    {
                        // Read destination key
                        var arrDestKey = destinationKey.ToArray();
                        statusOp = GET(arrDestKey, out var destinationList, ref objectStoreLockableContext);

                        if (statusOp == GarnetStatus.NOTFOUND)
                        {
                            destinationList.garnetObject = new ListObject();
                        }

                        if (destinationList.garnetObject is not ListObject listObject)
                            return GarnetStatus.WRONGTYPE;

                        dstListObject = listObject;
                    }

                    // Right pop (removelast) from source
                    if (sourceDirection == OperationDirection.Right)
                    {
                        element = srcListObject.LnkList.Last.Value;
                        srcListObject.LnkList.RemoveLast();
                    }
                    else
                    {
                        // Left pop (removefirst) from source
                        element = srcListObject.LnkList.First.Value;
                        srcListObject.LnkList.RemoveFirst();
                    }
                    srcListObject.UpdateSize(element, false);

                    IGarnetObject newListValue = null;
                    if (!sameKey)
                    {
                        if (srcListObject.LnkList.Count == 0)
                        {
                            _ = EXPIRE(sourceKey, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None,
                                ref lockableContext, ref objectLockableContext);
                        }

                        // Left push (addfirst) to destination
                        if (destinationDirection == OperationDirection.Left)
                            dstListObject.LnkList.AddFirst(element);
                        else
                            dstListObject.LnkList.AddLast(element);

                        dstListObject.UpdateSize(element);
                        newListValue = new ListObject(dstListObject.LnkList, dstListObject.Expiration, dstListObject.Size);

                        // Upsert
                        SET(destinationKey.ToArray(), newListValue, ref objectStoreLockableContext);
                    }
                    else
                    {
                        // When the source and the destination key is the same the operation is done only in the sourceList
                        if (sourceDirection == OperationDirection.Right && destinationDirection == OperationDirection.Left)
                            srcListObject.LnkList.AddFirst(element);
                        else if (sourceDirection == OperationDirection.Left && destinationDirection == OperationDirection.Right)
                            srcListObject.LnkList.AddLast(element);
                        newListValue = srcListObject;
                        ((ListObject)newListValue).UpdateSize(element);
                    }
                }
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);
            }

            itemBroker.HandleCollectionUpdate(destinationKey.Span.ToArray());
            return GarnetStatus.OK;
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            var input = scratchBufferManager.FormatScratchAsResp(ObjectInputHeader.Size, key);

            // Prepare header in input buffer
            var rmwInput = (ObjectInputHeader*)input.ptr;
            rmwInput->header.type = GarnetObjectType.List;
            rmwInput->header.flags = 0;
            rmwInput->header.ListOp = ListOperation.LTRIM;
            rmwInput->arg1 = start;
            rmwInput->arg2 = stop;

            var status = RMWObjectStoreOperation(key.ToArray(), input, out var output, ref objectStoreContext);

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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            var status = RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);
            itemBroker.HandleCollectionUpdate(key);
            return status;
        }

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus ListTrim<TObjectContext>(byte[] key, ArgSlice input, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
        {
            var status = RMWObjectStoreOperation(key, input, out output, ref objectStoreContext);
            itemBroker.HandleCollectionUpdate(key);
            return status;
        }

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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
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
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
             => ReadObjectStoreOperation(key, input, out output, ref objectStoreContext);

        /// <summary>
        /// Sets the list element at index to element.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListSet<TObjectContext>(byte[] key, ArgSlice input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>
            => RMWObjectStoreOperationWithOutput(key, input, ref objectStoreContext, ref outputFooter);
    }
}