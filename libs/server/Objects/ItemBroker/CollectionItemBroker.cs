// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// This class brokers collection items for blocking operations.
    /// When a supported blocking command is initiated, RespServerSession will call the GetCollectionItemAsync method
    /// with the desired object type and operation and a list of keys to the desired objects.
    /// When an item is added to a collection, the StorageSession will call the Publish method with the relevant object key
    /// to notify the broker that a new item may be available.
    /// The main loop, in the Start method, listens for published item additions as well as new observers
    /// and notifies the calling method if an item was found.
    /// </summary>
    public class CollectionItemBroker : IDisposable
    {
        // Queue of events to be handled by the main loops
        private AsyncQueue<BrokerEventBase> BrokerEventsQueue => brokerEventsQueueLazy.Value;

        // Mapping of RespServerSession ID (ObjectStoreSessionID) to observer instance
        private ConcurrentDictionary<int, CollectionItemObserver> SessionIdToObserver => sessionIdToObserverLazy.Value;

        // Mapping of observed keys to queue of observers, by order of subscription
        private Dictionary<byte[], Queue<CollectionItemObserver>> KeysToObservers => keysToObserversLazy.Value;

        private readonly Lazy<AsyncQueue<BrokerEventBase>> brokerEventsQueueLazy = new();
        private readonly Lazy<ConcurrentDictionary<int, CollectionItemObserver>> sessionIdToObserverLazy = new();
        private readonly Lazy<Dictionary<byte[], Queue<CollectionItemObserver>>> keysToObserversLazy =
            new(() => new Dictionary<byte[], Queue<CollectionItemObserver>>(ByteArrayComparer.Instance));

        // Cancellation token for the main loop
        private readonly CancellationTokenSource cts = new();
        // Synchronization event for awaiting main loop to finish
        private readonly ManualResetEventSlim done = new(true);
        private readonly ReaderWriterLockSlim isStartedLock = new();
        private readonly ReaderWriterLockSlim keysToObserversLock = new();

        private bool disposed = false;
        private bool isStarted = false;

        /// <summary>
        /// Asynchronously wait for item from collection object
        /// </summary>
        /// <param name="command">RESP command</param>
        /// <param name="keys">Keys of objects to observe</param>
        /// <param name="session">Calling session instance</param>
        /// <param name="timeoutInSeconds">Timeout of operation (in seconds, 0 for waiting indefinitely)</param>
        /// <param name="cmdArgs">Additional arguments for command</param>
        /// <returns>Result of operation</returns>
        internal async Task<CollectionItemResult> GetCollectionItemAsync(RespCommand command, byte[][] keys,
            RespServerSession session, double timeoutInSeconds, ArgSlice[] cmdArgs = null)
        {
            var observer = new CollectionItemObserver(session, command, cmdArgs);
            return await this.GetCollectionItemAsync(observer, keys, timeoutInSeconds);
        }

        /// <summary>
        /// Asynchronously wait for item from collection object at srcKey and
        /// atomically add it to collection at dstKey
        /// </summary>
        /// <param name="command">RESP command</param>
        /// <param name="srcKey">Key of the object to observe</param>
        /// <param name="session">Calling session instance</param>
        /// <param name="timeoutInSeconds">Timeout of operation (in seconds, 0 for waiting indefinitely)</param>
        /// <param name="cmdArgs">Additional arguments for command</param>
        /// <returns>Result of operation</returns>
        internal async Task<CollectionItemResult> MoveCollectionItemAsync(RespCommand command, byte[] srcKey,
            RespServerSession session, double timeoutInSeconds, ArgSlice[] cmdArgs)
        {
            var observer = new CollectionItemObserver(session, command, cmdArgs);
            return await this.GetCollectionItemAsync(observer, [srcKey], timeoutInSeconds);
        }

        private async Task<CollectionItemResult> GetCollectionItemAsync(CollectionItemObserver observer, byte[][] keys,
            double timeoutInSeconds)
        {
            // Add the session ID to observer mapping
            SessionIdToObserver.TryAdd(observer.Session.ObjectStoreSessionID, observer);

            // Add a new observer event to the event queue
            BrokerEventsQueue.Enqueue(new NewObserverEvent(observer, keys));

            // Check if main loop has started, if not, start the main loop
            if (!isStarted)
            {
                isStartedLock.EnterUpgradeableReadLock();
                try
                {
                    if (!isStarted)
                    {
                        isStartedLock.EnterWriteLock();
                        try
                        {
                            _ = Task.Run(Start);
                            isStarted = true;
                        }
                        finally
                        {
                            isStartedLock.ExitWriteLock();
                        }
                    }
                }
                finally
                {
                    isStartedLock.ExitUpgradeableReadLock();
                }
            }

            var timeout = timeoutInSeconds == 0
                ? TimeSpan.FromMilliseconds(-1)
                : TimeSpan.FromSeconds(timeoutInSeconds);

            try
            {
                // Wait for either the result found notification or the timeout to expire
                await observer.ResultFoundSemaphore.WaitAsync(timeout, observer.CancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
            }

            SessionIdToObserver.TryRemove(observer.Session.ObjectStoreSessionID, out _);

            // Check if observer is still waiting for result
            if (observer.Status == ObserverStatus.WaitingForResult)
            {
                // Try to set the observer result to an empty one
                observer.HandleSetResult(CollectionItemResult.Empty);
            }

            return observer.Result;
        }

        /// <summary>
        /// Notify broker that an item was added to a collection object in specified key
        /// </summary>
        /// <param name="key">Key of the updated collection object</param>
        internal void HandleCollectionUpdate(byte[] key)
        {
            // Check if main loop is started
            isStartedLock.EnterReadLock();
            try
            {
                if (!isStarted) return;
            }
            finally
            {
                isStartedLock.ExitReadLock();
            }


            // Check if there are any observers to specified key
            if (!KeysToObservers.ContainsKey(key) || KeysToObservers[key].Count == 0)
            {
                keysToObserversLock.EnterReadLock();
                try
                {
                    if (!KeysToObservers.ContainsKey(key) || KeysToObservers[key].Count == 0) return;
                }
                finally
                {
                    keysToObserversLock.ExitReadLock();
                }
            }

            // Add collection updated event to queue
            BrokerEventsQueue.Enqueue(new CollectionUpdatedEvent(key));
        }

        /// <summary>
        /// Notify broker that a RespServerSession object is being disposed
        /// </summary>
        /// <param name="session">The disposed session</param>
        internal void HandleSessionDisposed(RespServerSession session)
        {
            // Try to remove session ID from mapping & get the observer object for the specified session, if exists
            if (!SessionIdToObserver.TryRemove(session.ObjectStoreSessionID, out var observer))
                return;

            // Change observer status to reflect that its session has been disposed
            observer.HandleSessionDisposed();
        }

        /// <summary>
        /// Calls the appropriate method based on the broker event type
        /// </summary>
        /// <param name="brokerEvent"></param>
        private void HandleBrokerEvent(BrokerEventBase brokerEvent)
        {
            switch (brokerEvent)
            {
                case NewObserverEvent noe:
                    InitializeObserver(noe.Observer, noe.Keys);
                    return;
                case CollectionUpdatedEvent cue:
                    TryAssignItemFromKey(cue.Key);
                    return;
            }
        }

        /// <summary>
        /// Handles a new observer
        /// </summary>
        /// <param name="observer">The new observer instance</param>
        /// <param name="keys">Keys observed by the new observer</param>
        private void InitializeObserver(CollectionItemObserver observer, byte[][] keys)
        {
            // This lock is for synchronization with incoming collection updated events 
            keysToObserversLock.EnterWriteLock();
            try
            {
                // Iterate over the keys in order, set the observer's result if collection in key contains an item
                foreach (var key in keys)
                {
                    // If the key already has a non-empty observer queue, it does not have an item to retrieve
                    // Otherwise, try to retrieve next available item
                    if ((KeysToObservers.ContainsKey(key) && KeysToObservers[key].Count > 0) ||
                        !TryGetResult(key, observer.Session.storageSession, observer.Command, observer.CommandArgs,
                            out _, out var result)) continue;

                    // An item was found - set the observer result and return
                    SessionIdToObserver.TryRemove(observer.Session.ObjectStoreSessionID, out _);
                    observer.HandleSetResult(result);
                    return;
                }

                // No item was found, enqueue new observer in every observed keys queue
                foreach (var key in keys)
                {
                    if (!KeysToObservers.ContainsKey(key))
                        KeysToObservers.Add(key, new Queue<CollectionItemObserver>());

                    KeysToObservers[key].Enqueue(observer);
                }
            }
            finally
            {
                keysToObserversLock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Try to assign item available (if exists) with next ready observer in queue
        /// </summary>
        /// <param name="key">Key of collection from which to assign item</param>
        /// <returns>True if successful in assigning item</returns>
        private bool TryAssignItemFromKey(byte[] key)
        {
            // If queue doesn't exist for key or is empty, nothing to do
            if (!KeysToObservers.TryGetValue(key, out var observers) || observers.Count == 0)
                return false;

            // Peek at next observer in queue
            while (observers.TryPeek(out var observer))
            {
                // If observer is not waiting for result, dequeue it and continue to next observer in queue
                if (observer.Status != ObserverStatus.WaitingForResult)
                {
                    observers.Dequeue();
                    continue;
                }

                observer.ObserverStatusLock.EnterUpgradeableReadLock();
                try
                {
                    // If observer is not waiting for result, dequeue it and continue to next observer in queue
                    if (observer.Status != ObserverStatus.WaitingForResult)
                    {
                        observers.Dequeue();
                        continue;
                    }

                    // Try to get next available item from object stored in key
                    if (!TryGetResult(key, observer.Session.storageSession, observer.Command, observer.CommandArgs,
                            out var currCount, out var result))
                    {
                        // If unsuccessful getting next item but there is at least one item in the collection,
                        // continue to next observer in the queue, otherwise return
                        if (currCount > 0) continue;
                        return false;
                    }

                    // Dequeue the observer, and set the observer's result
                    observers.TryDequeue(out observer);

                    SessionIdToObserver.TryRemove(observer!.Session.ObjectStoreSessionID, out _);
                    observer.HandleSetResult(result);

                    return true;
                }
                finally
                {
                    observer.ObserverStatusLock.ExitUpgradeableReadLock();
                }
            }

            return false;
        }

        /// <summary>
        /// Try to get next available item from list object
        /// </summary>
        /// <param name="listObj">List object</param>
        /// <param name="command">RESP command</param>
        /// <param name="nextItem">Item retrieved</param>
        /// <returns>True if found available item</returns>
        private static bool TryGetNextListItem(ListObject listObj, RespCommand command, out byte[] nextItem)
        {
            nextItem = default;

            // If object has no items, return
            if (listObj.LnkList.Count == 0) return false;

            // Get the next object according to operation type
            switch (command)
            {
                case RespCommand.BRPOP:
                    nextItem = listObj.LnkList.Last!.Value;
                    listObj.LnkList.RemoveLast();
                    break;
                case RespCommand.BLPOP:
                    nextItem = listObj.LnkList.First!.Value;
                    listObj.LnkList.RemoveFirst();
                    break;
                default:
                    return false;
            }

            listObj.UpdateSize(nextItem, false);

            return true;
        }

        private static bool TryMoveNextListItem(ListObject srcListObj, ListObject dstListObj,
            OperationDirection srcDirection, OperationDirection dstDirection, out byte[] nextItem)
        {
            nextItem = default;

            // If object has no items, return
            if (srcListObj.LnkList.Count == 0) return false;

            // Get the next object according to source direction
            switch (srcDirection)
            {
                case OperationDirection.Right:
                    nextItem = srcListObj.LnkList.Last!.Value;
                    srcListObj.LnkList.RemoveLast();
                    break;
                case OperationDirection.Left:
                    nextItem = srcListObj.LnkList.First!.Value;
                    srcListObj.LnkList.RemoveFirst();
                    break;
                default:
                    return false;
            }

            srcListObj.UpdateSize(nextItem, false);

            // Add the object to the destination according to the destination direction
            switch (dstDirection)
            {
                case OperationDirection.Right:
                    dstListObj.LnkList.AddLast(nextItem);
                    break;
                case OperationDirection.Left:
                    dstListObj.LnkList.AddFirst(nextItem);
                    break;
                default:
                    return false;
            }

            dstListObj.UpdateSize(nextItem);

            return true;
        }

        /// <summary>
        /// Try to get next available item from sorted set object
        /// </summary>
        /// <param name="sortedSetObj">Sorted set object</param>
        /// <param name="command">RESP command</param>
        /// <param name="nextItem">Item retrieved</param>
        /// <returns>True if found available item</returns>
        private static bool TryGetNextSetObject(SortedSetObject sortedSetObj, RespCommand command, out byte[] nextItem)
        {
            nextItem = default;

            // If object has no items, return
            if (sortedSetObj.Dictionary.Count == 0) return false;

            // Get the next object according to operation type
            switch (command)
            {
                default:
                    return false;
            }
        }

        /// <summary>
        /// Try to get next available item from object
        /// </summary>
        /// <param name="key">Key of object</param>
        /// <param name="storageSession">Current storage session</param>
        /// <param name="command">RESP command</param>
        /// <param name="cmdArgs">Additional command arguments</param>
        /// <param name="currCount">Collection size</param>
        /// <param name="result">Result of command</param>
        /// <returns>True if found available item</returns>
        private unsafe bool TryGetResult(byte[] key, StorageSession storageSession, RespCommand command, ArgSlice[] cmdArgs, out int currCount, out CollectionItemResult result)
        {
            currCount = default;
            result = default;
            var createTransaction = false;

            var objectType = command switch
            {
                RespCommand.BLPOP or RespCommand.BRPOP or RespCommand.BLMOVE or RespCommand.BLMPOP => GarnetObjectType.List,
                _ => throw new NotSupportedException()
            };

            ArgSlice dstKey = default;
            if (command == RespCommand.BLMOVE)
            {
                dstKey = cmdArgs[0];
            }

            // Create a transaction if not currently in a running transaction
            if (storageSession.txnManager.state != TxnState.Running)
            {
                Debug.Assert(storageSession.txnManager.state == TxnState.None);
                createTransaction = true;
                var asKey = storageSession.scratchBufferManager.CreateArgSlice(key);
                storageSession.txnManager.SaveKeyEntryToLock(asKey, true, LockType.Exclusive);

                if (command == RespCommand.BLMOVE)
                {
                    storageSession.txnManager.SaveKeyEntryToLock(dstKey, true, LockType.Exclusive);
                }

                _ = storageSession.txnManager.Run(true);
            }

            var objectLockableContext = storageSession.txnManager.ObjectStoreLockableContext;

            try
            {
                // Get the object stored at key
                var statusOp = storageSession.GET(key, out var osObject, ref objectLockableContext);
                if (statusOp == GarnetStatus.NOTFOUND) return false;

                IGarnetObject dstObj = null;
                byte[] arrDstKey = default;
                if (command == RespCommand.BLMOVE)
                {
                    arrDstKey = dstKey.ToArray();
                    var dstStatusOp = storageSession.GET(arrDstKey, out var osDstObject, ref objectLockableContext);
                    if (dstStatusOp != GarnetStatus.NOTFOUND) dstObj = osDstObject.garnetObject;
                }

                // Check for type match between the observer and the actual object type
                // If types match, get next item based on item type
                switch (osObject.garnetObject)
                {
                    case ListObject listObj:
                        currCount = listObj.LnkList.Count;
                        if (objectType != GarnetObjectType.List) return false;
                        if (currCount == 0) return false;

                        switch (command)
                        {
                            case RespCommand.BLPOP:
                            case RespCommand.BRPOP:
                                var isSuccessful = TryGetNextListItem(listObj, command, out var nextItem);
                                result = new CollectionItemResult(key, nextItem);
                                return isSuccessful;
                            case RespCommand.BLMOVE:
                                ListObject dstList;
                                var newObj = false;
                                if (dstObj == null)
                                {
                                    dstList = new ListObject();
                                    newObj = true;
                                }
                                else if (dstObj is ListObject tmpDstList)
                                {
                                    dstList = tmpDstList;
                                }
                                else return false;

                                isSuccessful = TryMoveNextListItem(listObj, dstList, (OperationDirection)cmdArgs[1].ReadOnlySpan[0],
                                    (OperationDirection)cmdArgs[2].ReadOnlySpan[0], out nextItem);
                                result = new CollectionItemResult(key, nextItem);

                                if (isSuccessful && newObj)
                                {
                                    isSuccessful = storageSession.SET(arrDstKey, dstList, ref objectLockableContext) ==
                                                   GarnetStatus.OK;
                                }

                                return isSuccessful;
                            case RespCommand.BLMPOP:
                                var popDirection = (OperationDirection)cmdArgs[0].ReadOnlySpan[0];
                                var popCount = *(int*)(cmdArgs[1].ptr);
                                popCount = Math.Min(popCount, listObj.LnkList.Count);

                                var items = new byte[popCount][];
                                for (var i = 0; i < popCount; i++)
                                {
                                    var _ = TryGetNextListItem(listObj, popDirection == OperationDirection.Left ? RespCommand.BLPOP : RespCommand.BRPOP, out items[i]); // Return can be ignored because it is guaranteed to return true
                                }

                                result = new CollectionItemResult(key, items);
                                return true;
                            default:
                                return false;
                        }
                    case SortedSetObject setObj:
                        currCount = setObj.Dictionary.Count;
                        if (objectType != GarnetObjectType.SortedSet) return false;

                        var hasValue = TryGetNextSetObject(setObj, command, out var sortedSetNextItem);
                        result = new CollectionItemResult(key, sortedSetNextItem);
                        return hasValue;
                    default:
                        return false;
                }
            }
            finally
            {
                if (createTransaction)
                    storageSession.txnManager.Commit(true);
            }
        }

        /// <summary>
        /// Broker's main loop logic
        /// </summary>
        /// <returns>Task</returns>
        private async Task Start()
        {
            try
            {
                // Repeat while not disposed or cancelled
                while (!disposed && !cts.IsCancellationRequested)
                {
                    // Try to synchronously get the next event
                    if (!BrokerEventsQueue.TryDequeue(out var nextEvent))
                    {
                        // Asynchronously dequeue next event in broker's queue
                        // once event is dequeued successfully, call handler method
                        try
                        {
                            nextEvent = await BrokerEventsQueue.DequeueAsync(cts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            // Ignored
                        }
                    }

                    if (nextEvent == default) continue;

                    HandleBrokerEvent(nextEvent);
                }
            }
            finally
            {
                done.Set();
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            disposed = true;
            cts.Cancel();
            foreach (var observer in SessionIdToObserver.Values)
            {
                if (observer.Status == ObserverStatus.WaitingForResult &&
                    !observer.CancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        observer.CancellationTokenSource.Cancel();
                    }
                    catch (Exception)
                    {
                        // ignored
                    }
                }
            }
            done.Wait();
        }
    }
}