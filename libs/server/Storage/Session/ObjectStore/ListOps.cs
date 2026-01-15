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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListPush<TObjectContext>(PinnedSpanByte key, PinnedSpanByte[] elements, ListOperation lop, out int itemsDoneCount, ref TObjectContext objectContext)
          where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            itemsDoneCount = 0;

            if (key.Length == 0 || elements.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            parseState.InitializeWithArguments(elements);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.List, RespMetaCommand.None, ref parseState, flags: RespInputFlags.SkipRespOutput) { ListOp = lop };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);

            itemsDoneCount = output.result1;
            itemBroker?.HandleCollectionUpdate(key.ToArray());
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
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListPush<TObjectContext>(PinnedSpanByte key, PinnedSpanByte element, ListOperation lop, out int itemsDoneCount, ref TObjectContext objectContext)
           where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            itemsDoneCount = 0;

            // Prepare the parse state
            parseState.InitializeWithArgument(element);

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.List, RespMetaCommand.None, ref parseState, flags: RespInputFlags.SkipRespOutput) { ListOp = lop };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);
            itemsDoneCount = output.result1;

            itemBroker?.HandleCollectionUpdate(key.ToArray());
            return status;
        }

        /// <summary>
        /// Removes one element from the head(left) or tail(right)
        /// of the list stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="lop"></param>
        /// <param name="objectContext"></param>
        /// <param name="element"></param>
        /// <returns>The popped element</returns>
        public GarnetStatus ListPop<TObjectContext>(PinnedSpanByte key, ListOperation lop, ref TObjectContext objectContext, out PinnedSpanByte element)
           where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = ListPop(key, 1, lop, ref objectContext, out var elements);
            element = status == GarnetStatus.OK ? elements.FirstOrDefault() : default;
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
        /// <param name="objectContext"></param>
        /// <param name="elements"></param>
        /// <returns>The count elements popped from the list</returns>
        public unsafe GarnetStatus ListPop<TObjectContext>(PinnedSpanByte key, int count, ListOperation lop, ref TObjectContext objectContext, out PinnedSpanByte[] elements)
                 where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.List, arg1: count) { ListOp = lop };

            var output = new ObjectOutput();

            var status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

            //process output
            elements = default;
            if (status == GarnetStatus.OK)
                elements = ProcessRespArrayOutput(output, out var error);

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
        public unsafe GarnetStatus ListPopMultiple<TObjectContext>(PinnedSpanByte[] keys, OperationDirection direction, int count, ref TObjectContext objectContext, out PinnedSpanByte key, out PinnedSpanByte[] elements)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
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
        /// <param name="objectContext"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListLength<TObjectContext>(PinnedSpanByte key, ref TObjectContext objectContext, out int count)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            count = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.List, flags: RespInputFlags.SkipRespOutput) { ListOp = ListOperation.LLEN };

            var status = ReadObjectStoreOperation(key.ReadOnlySpan, ref input, out var output, ref objectContext);

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
        public GarnetStatus ListMove(PinnedSpanByte sourceKey, PinnedSpanByte destinationKey, OperationDirection sourceDirection, OperationDirection destinationDirection, out byte[] element)
        {
            element = default;
            var objectTransactionalContext = txnManager.ObjectTransactionalContext;

            if (objectTransactionalContext.Session is null)
                ThrowObjectStoreUninitializedException();

            // If source and destination are the same, the operation is equivalent to removing the last element from the list
            // and pushing it as first element of the list, so it can be considered as a list rotation command.
            var sameKey = sourceKey.ReadOnlySpan.SequenceEqual(destinationKey.ReadOnlySpan);

            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                txnManager.SaveKeyEntryToLock(sourceKey, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(destinationKey, LockType.Exclusive);
                _ = txnManager.Run(true);
            }

            var objectContext = txnManager.ObjectTransactionalContext;
            var unifiedContext = txnManager.UnifiedTransactionalContext;

            try
            {
                // Get the source key
                var statusOp = GET(sourceKey, out var sourceList, ref objectTransactionalContext);

                if (statusOp == GarnetStatus.NOTFOUND)
                {
                    return GarnetStatus.OK;
                }
                else if (statusOp == GarnetStatus.OK)
                {
                    if (sourceList.GarnetObject is not ListObject srcListObject)
                        return GarnetStatus.WRONGTYPE;

                    if (srcListObject.LnkList.Count == 0)
                        return GarnetStatus.OK;

                    ListObject dstListObject = default;
                    if (!sameKey)
                    {
                        // Read destination key
                        statusOp = GET(destinationKey, out var destinationList, ref objectContext);

                        if (statusOp == GarnetStatus.NOTFOUND)
                        {
                            destinationList.GarnetObject = new ListObject();
                        }

                        if (destinationList.GarnetObject is not ListObject listObject)
                            return GarnetStatus.WRONGTYPE;

                        dstListObject = listObject;
                    }
                    else // sameKey
                    {
                        if (sourceDirection == destinationDirection)
                        {
                            element = (sourceDirection == OperationDirection.Right) ?
                                        srcListObject.LnkList.Last.Value : srcListObject.LnkList.First.Value;
                            return GarnetStatus.OK;
                        }
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
                            _ = EXPIRE(sourceKey, TimeSpan.Zero, out _, ExpireOption.None, ref unifiedContext);
                        }

                        // Left push (addfirst) to destination
                        if (destinationDirection == OperationDirection.Left)
                            _ = dstListObject.LnkList.AddFirst(element);
                        else
                            _ = dstListObject.LnkList.AddLast(element);

                        dstListObject.UpdateSize(element);
                        newListValue = new ListObject(dstListObject.LnkList, dstListObject.HeapMemorySize);

                        // Upsert
                        _ = SET(destinationKey, newListValue, ref objectContext);
                    }
                    else
                    {
                        // When the source and the destination key is the same the operation is done only in the sourceList
                        if (destinationDirection == OperationDirection.Left)
                            _ = srcListObject.LnkList.AddFirst(element);
                        else if (destinationDirection == OperationDirection.Right)
                            _ = srcListObject.LnkList.AddLast(element);
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

            itemBroker?.HandleCollectionUpdate(destinationKey.Span.ToArray());
            return GarnetStatus.OK;
        }

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="start"></param>
        /// <param name="stop"></param>
        /// <param name="objectContext"></param>
        /// <returns>true when successful</returns>
        public unsafe bool ListTrim<TObjectContext>(PinnedSpanByte key, int start, int stop, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            // Prepare the input
            var input = new ObjectInput(GarnetObjectType.List, arg1: start, arg2: stop) { ListOp = ListOperation.LTRIM };

            var status = RMWObjectStoreOperation(key.ReadOnlySpan, ref input, out _, ref objectContext);

            return status == GarnetStatus.OK;
        }

        /// <summary>
        /// Adds new elements at the head(right) or tail(left)
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus ListPush<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);
            itemBroker?.HandleCollectionUpdate(key.ToArray());
            return status;
        }

        /// <summary>
        /// The command returns the index of matching elements inside a Redis list.
        /// By default, when no options are given, it will scan the list from head to tail, looking for the first match of "element".
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus ListPosition<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            return ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);
        }

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus ListTrim<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Gets the specified elements of the list stored at key.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus ListRange<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Inserts a new element in the list stored at key either before or after a value pivot
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus ListInsert<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var status = RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);
            itemBroker?.HandleCollectionUpdate(key.ToArray());
            return status;
        }

        /// <summary>
        /// Returns the element at index.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus ListIndex<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Removes the first count occurrences of elements equal to element from the list.
        /// LREM key count element
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public GarnetStatus ListRemove<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Removes the count elements from the head(left) or tail(right) of the list stored at key.
        /// If the list contains less than count elements, removes and returns the number of elements in the list.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListPop<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Removes the count elements from the head(left) or tail(right) of the list stored at key.
        /// If the list contains less than count elements, removes and returns the number of elements in the list.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListLength<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
             where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
             => ReadObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);

        /// <summary>
        /// Sets the list element at index to element.
        /// </summary>
        /// <typeparam name="TObjectContext"></typeparam>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="objectContext"></param>
        /// <returns></returns>
        public unsafe GarnetStatus ListSet<TObjectContext>(PinnedSpanByte key, ref ObjectInput input, ref ObjectOutput output, ref TObjectContext objectContext)
            where TObjectContext : ITsavoriteContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
            => RMWObjectStoreOperationWithOutput(key.ReadOnlySpan, ref input, ref objectContext, ref output);
    }
}