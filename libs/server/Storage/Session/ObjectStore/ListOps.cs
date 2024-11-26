// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

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
          where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            itemsDoneCount = 0;

            if (key.Length == 0 || elements.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(elements);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.List) { ListOp = lop };
            var input = new ObjectInput(header, ref parseState);

            var arrKey = key.ToArray();
            var status = RMWObjectStoreOperation(arrKey, ref input, out var output, ref objectStoreContext);

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
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            itemsDoneCount = 0;

            // Prepare the parse state
            parseState.InitializeWithArgument(element);

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.List) { ListOp = lop };
            var input = new ObjectInput(header, ref parseState);

            var status = RMWObjectStoreOperation(key.ToArray(), ref input, out var output, ref objectStoreContext);
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
           where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
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
                 where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.List) { ListOp = lop };
            var input = new ObjectInput(header, count);

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = RMWObjectStoreOperationWithOutput(key.ToArray(), ref input, ref objectStoreContext, ref outputFooter);

            //process output
            elements = default;
            if (status == GarnetStatus.OK)
                elements = ProcessRespArrayOutput(outputFooter, out var error);

            return status;
        }

        /// <summary>
        /// Removes the count elements from the head(left) or tail(right) of the first non-empty list key from the list of provided key names.
        /// If the list contains less than count elements, removes and returns the number of elements in the list.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="keys"></param>
        /// <param name="direction"></param>
        /// <param name="count"></param>
        /// <param name="objectContext"></param>
        /// <param name="key"></param>
        /// <param name="elements"></param>
        /// <returns>The count elements popped from the list</returns>
        public unsafe GarnetStatus ListPopMultiple<TObjectContext>(ArgSlice[] keys, OperationDirection direction, int count, ref TObjectContext objectContext, out ArgSlice key, out ArgSlice[] elements)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            foreach (var k in keys)
            {
                GarnetStatus statusOp;

                if (direction == OperationDirection.Left)
                {
                    statusOp = ListPop(k, count, ListOperation.LPOP, ref objectContext, out elements);
                }
                else
                {
                    statusOp = ListPop(k, count, ListOperation.RPOP, ref objectContext, out elements);
                }

                if (statusOp == GarnetStatus.NOTFOUND) continue;

                key = k;
                return statusOp;
            }

            key = default;
            elements = default;
            return GarnetStatus.NOTFOUND;
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            count = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.List) { ListOp = ListOperation.LLEN };
            var input = new ObjectInput(header);

            var status = ReadObjectStoreOperation(key.ToArray(), ref input, out var output, ref objectStoreContext);

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
            var objectTransactionalContext = txnManager.ObjectStoreTransactionalContext;

            if (itemBroker == null)
                ThrowObjectStoreUninitializedException();

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

            var objectStoreTransactionalContext = txnManager.ObjectStoreTransactionalContext;

            try
            {
                // Get the source key
                var statusOp = GET(sourceKey.ToArray(), out var sourceList, ref objectTransactionalContext);

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
                        statusOp = GET(arrDestKey, out var destinationList, ref objectStoreTransactionalContext);

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
                                ref transactionalContext, ref objectTransactionalContext);
                        }

                        // Left push (addfirst) to destination
                        if (destinationDirection == OperationDirection.Left)
                            dstListObject.LnkList.AddFirst(element);
                        else
                            dstListObject.LnkList.AddLast(element);

                        dstListObject.UpdateSize(element);
                        newListValue = new ListObject(dstListObject.LnkList, dstListObject.Expiration, dstListObject.Size);

                        // Upsert
                        SET(destinationKey.ToArray(), newListValue, ref objectStoreTransactionalContext);
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
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            // Prepare the input
            var header = new RespInputHeader(GarnetObjectType.List) { ListOp = ListOperation.LTRIM };
            var input = new ObjectInput(header, start, stop);

            var status = RMWObjectStoreOperation(key.ToArray(), ref input, out _, ref objectStoreContext);

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
        public GarnetStatus ListPush<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var status = RMWObjectStoreOperation(key, ref input, out output, ref objectStoreContext);
            itemBroker.HandleCollectionUpdate(key);
            return status;
        }

        /// <summary>
        /// The command returns the index of matching elements inside a Redis list.
        /// By default, when no options are given, it will scan the list from head to tail, looking for the first match of "element".
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus ListPosition<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            return ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);
        }

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus ListTrim<TObjectContext>(byte[] key, ref ObjectInput input, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperation(key, ref input, out _, ref objectStoreContext);

        /// <summary>
        /// Gets the specified elements of the list stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus ListRange<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);

        /// <summary>
        /// Inserts a new element in the list stored at key either before or after a value pivot
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public GarnetStatus ListInsert<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
        {
            var status = RMWObjectStoreOperation(key, ref input, out output, ref objectStoreContext);
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
        public GarnetStatus ListIndex<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => ReadObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);

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
        public GarnetStatus ListRemove<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperation(key, ref input, out output, ref objectStoreContext);

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
        public unsafe GarnetStatus ListPop<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);

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
        public unsafe GarnetStatus ListLength<TObjectContext>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output, ref TObjectContext objectStoreContext)
             where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
             => ReadObjectStoreOperation(key, ref input, out output, ref objectStoreContext);

        /// <summary>
        /// Sets the list element at index to element.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="outputFooter"></param>
        /// <param name="objectStoreContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListSet<TObjectContext>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter, ref TObjectContext objectStoreContext)
            where TObjectContext : ITsavoriteContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator>
            => RMWObjectStoreOperationWithOutput(key, ref input, ref objectStoreContext, ref outputFooter);
    }
}