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
        /// <param name="key">The name of the key</param>
        /// <param name="elements">The elements to be added at the left or the righ of the list</param>
        /// <param name="lop">The Right or Left modifier of the operation to perform</param>
        /// <param name="itemsDoneCount">The length of the list after the push operations.</param>
        /// <returns></returns>
        public GarnetStatus ListPush<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice[] elements, ListOperation lop, out int itemsDoneCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            itemsDoneCount = 0;

            if (key.Length == 0 || elements.Length == 0)
                return GarnetStatus.OK;

            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, elements);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.List,
                    ListOp = lop,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var arrKey = key.ToArray();
            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(arrKey, ref input, out var output);

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
        public GarnetStatus ListPush<TKeyLocker, TEpochGuard>(ArgSlice key, ArgSlice element, ListOperation lop, out int itemsDoneCount)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            // Prepare the parse state
            var parseState = new SessionParseState();
            ArgSlice[] parseStateBuffer = default;
            parseState.InitializeWithArguments(ref parseStateBuffer, element);

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.List,
                    ListOp = lop,
                },
                parseState = parseState,
                parseStateStartIdx = 0,
            };

            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);
            itemsDoneCount = output.result1;

            itemBroker.HandleCollectionUpdate(key.Span.ToArray());
            return status;
        }

        /// <summary>
        /// Removes one element from the head(left) or tail(right) 
        /// of the list stored at key.
        /// </summary>
        /// <returns>The popped element</returns>
        public GarnetStatus ListPop<TKeyLocker, TEpochGuard>(ArgSlice key, ListOperation lop, out ArgSlice element)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard

        {
            var status = ListPop<TKeyLocker, TEpochGuard>(key, 1, lop, out var elements);
            element = elements.FirstOrDefault();
            return status;
        }

        /// <summary>
        /// Removes the count elements from the head(left) or tail(right) of the list stored at key.
        /// If the list contains less than count elements, removes and returns the number of elements in the list.
        /// </summary>
        /// <returns>The count elements popped from the list</returns>
        public GarnetStatus ListPop<TKeyLocker, TEpochGuard>(ArgSlice key, int count, ListOperation lop, out ArgSlice[] elements)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.List,
                    ListOp = lop,
                },
                arg1 = count,
            };

            var outputFooter = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            var status = RMWObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, ref outputFooter);
            elements = status == GarnetStatus.OK ? ProcessRespArrayOutput(outputFooter, out _) : default;
            return status;
        }

        /// <summary>
        /// Removes the count elements from the head(left) or tail(right) of the first non-empty list key from the list of provided key names.
        /// If the list contains less than count elements, removes and returns the number of elements in the list.
        /// </summary>
        /// <returns>The count elements popped from the list</returns>
        public GarnetStatus ListPopMultiple<TKeyLocker, TEpochGuard>(ArgSlice[] keys, OperationDirection direction, int count, out ArgSlice key, out ArgSlice[] elements)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            foreach (var k in keys)
            {
                var statusOp = direction == OperationDirection.Left
                    ? ListPop<TKeyLocker, TEpochGuard>(k, count, ListOperation.LPOP, out elements)
                    : ListPop<TKeyLocker, TEpochGuard>(k, count, ListOperation.RPOP, out elements);
                if (statusOp == GarnetStatus.NOTFOUND) 
                    continue;

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
        public GarnetStatus ListLength<TKeyLocker, TEpochGuard>(ArgSlice key, out int count)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            count = 0;

            if (key.Length == 0)
                return GarnetStatus.OK;

            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.List,
                    ListOp = ListOperation.LLEN,
                },
            };

            var status = ReadObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out var output);
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

            if (itemBroker == null)
                ThrowObjectStoreUninitializedException();

            // If source and destination are the same, the operation is equivalent to removing the last element from the list
            // and pushing it as first element of the list, so it can be considered as a list rotation command.
            var sameKey = sourceKey.ReadOnlySpan.SequenceEqual(destinationKey.ReadOnlySpan);

            var createTransaction = false;
            if (txnManager.state != TxnState.Running)
            {
                _ = txnManager.SaveKeyEntryToLock(sourceKey, true, LockType.Exclusive);
                _ = txnManager.SaveKeyEntryToLock(destinationKey, true, LockType.Exclusive);
                createTransaction = txnManager.Run(internal_txn: true);
            }

            var objectStoreLockableContext = txnManager.ObjectStoreLockableContext;

            try
            {
                // Perform Store operation under unsafe epoch control for pointer safety with speed, and always use TransactionalSessionLocker as we're in a transaction.
                // We have already locked via TransactionManager.Run so we only need to acquire the epoch here; operations within the transaction can use GarnetUnsafeEpochGuard.
                GarnetSafeEpochGuard.BeginUnsafe(ref dualContext.KernelSession);

                // Get the source key
                GarnetObjectStoreOutput sourceList = new();
                var statusOp = GET<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(sourceKey.ToArray(), ref sourceList);

                if (statusOp == GarnetStatus.NOTFOUND)
                    return GarnetStatus.OK;
                if (statusOp == GarnetStatus.OK)
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
                        GarnetObjectStoreOutput destinationList = new();
                        statusOp = GET<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(arrDestKey, ref destinationList);

                        if (statusOp == GarnetStatus.NOTFOUND)
                            destinationList.garnetObject = new ListObject();

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
                            _ = EXPIRE<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(sourceKey, TimeSpan.Zero, out _, StoreType.Object, ExpireOption.None);

                        // Left push (addfirst) to destination
                        if (destinationDirection == OperationDirection.Left)
                            _ = dstListObject.LnkList.AddFirst(element);
                        else
                            _ = dstListObject.LnkList.AddLast(element);

                        dstListObject.UpdateSize(element);
                        newListValue = new ListObject(dstListObject.LnkList, dstListObject.Expiration, dstListObject.Size);

                        // Upsert
                        _ = SET<TransactionalSessionLocker, GarnetUnsafeEpochGuard>(destinationKey.ToArray(), newListValue);
                    }
                    else
                    {
                        // When the source and the destination key is the same the operation is done only in the sourceList
                        if (sourceDirection == OperationDirection.Right && destinationDirection == OperationDirection.Left)
                            _ = srcListObject.LnkList.AddFirst(element);
                        else if (sourceDirection == OperationDirection.Left && destinationDirection == OperationDirection.Right)
                            _ = srcListObject.LnkList.AddLast(element);
                        newListValue = srcListObject;
                        ((ListObject)newListValue).UpdateSize(element);
                    }
                }
            }
            finally
            {
                GarnetSafeEpochGuard.EndUnsafe(ref dualContext.KernelSession);
                if (createTransaction)
                    txnManager.Commit(true);
            }

            itemBroker.HandleCollectionUpdate(destinationKey.Span.ToArray());
            return GarnetStatus.OK;
        }

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        /// <returns>true when successful</returns>
        public bool ListTrim<TKeyLocker, TEpochGuard>(ArgSlice key, int start, int stop)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            // Prepare the input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    type = GarnetObjectType.List,
                    ListOp = ListOperation.LTRIM,
                },
                arg1 = start,
                arg2 = stop,
            };

            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key.ToArray(), ref input, out _);
            return status == GarnetStatus.OK;
        }

        /// <summary>
        /// Adds new elements at the head(right) or tail(left)
        /// </summary>
        public GarnetStatus ListPush<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);
            itemBroker.HandleCollectionUpdate(key);
            return status;
        }

        /// <summary>
        /// Trim an existing list so it only contains the specified range of elements.
        /// </summary>
        public GarnetStatus ListTrim<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out _);

        /// <summary>
        /// Gets the specified elements of the list stored at key.
        /// </summary>
        public GarnetStatus ListRange<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMWObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// Inserts a new element in the list stored at key either before or after a value pivot
        /// </summary>
        public GarnetStatus ListInsert<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
        {
            var status = RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);
            itemBroker.HandleCollectionUpdate(key);
            return status;
        }

        /// <summary>
        /// Returns the element at index.
        /// </summary>
        public GarnetStatus ListIndex<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => ReadObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// Removes the first count occurrences of elements equal to element from the list.
        /// LREM key count element
        /// </summary>
        public GarnetStatus ListRemove<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMWObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <summary>
        /// Removes the count elements from the head(left) or tail(right) of the list stored at key.
        /// If the list contains less than count elements, removes and returns the number of elements in the list.
        /// </summary>
        public GarnetStatus ListPop<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMWObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);

        /// <summary>
        /// Removes the count elements from the head(left) or tail(right) of the list stored at key.
        /// If the list contains less than count elements, removes and returns the number of elements in the list.
        /// </summary>
        public GarnetStatus ListLength<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, out ObjectOutputHeader output)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
             => ReadObjectStoreOperation<TKeyLocker, TEpochGuard>(key, ref input, out output);

        /// <summary>
        /// Sets the list element at index to element.
        /// </summary>
        public GarnetStatus ListSet<TKeyLocker, TEpochGuard>(byte[] key, ref ObjectInput input, ref GarnetObjectStoreOutput outputFooter)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            => RMWObjectStoreOperationWithOutput<TKeyLocker, TEpochGuard>(key, ref input, ref outputFooter);
    }
}