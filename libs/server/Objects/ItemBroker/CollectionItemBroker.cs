﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
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
        readonly AsyncQueue<BrokerEventBase> brokerEventsQueue = new();

        // Mapping of RespServerSession ID (ObjectStoreSessionID) to observer instance
        readonly ConcurrentDictionary<int, CollectionItemObserver> sessionIdToObserver = new();

        // Mapping of observed keys to queue of observers, by order of subscription
        // Instantiated only when needed
        Dictionary<byte[], ConcurrentQueue<CollectionItemObserver>> keysToObservers = null;

        // Synchronization for the keysToObservers dictionary
        SingleWriterMultiReaderLock keysToObserversLock = new();

        // Cancellation token for the main loop
        readonly CancellationTokenSource cts = new();

        // Synchronization for awaiting main loop to finish
        readonly ManualResetEventSlim done = new(false);

        // Task for the main loop, we keep field for diagnostic purposes
        Task mainLoopTask = null;

        // Flag to indicate if the main loop task has started
        int mainLoopTaskStarted = 0;

        /// <summary>
        /// Constructor for CollectionItemBroker
        /// </summary>
        public CollectionItemBroker()
        {
        }

        /// <summary>
        /// Tries to get the observer associated with the given session ID.
        /// </summary>
        /// <param name="sessionId">The ID of the session to retrieve the observer for.</param>
        /// <param name="observer">When this method returns, contains the observer associated with the specified session ID, if the session ID is found; otherwise, null. This parameter is passed uninitialized.</param>
        /// <returns>true if the observer is found; otherwise, false.</returns>
        internal bool TryGetObserver(int sessionId, out CollectionItemObserver observer)
        {
            return sessionIdToObserver.TryGetValue(sessionId, out observer);
        }

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
            return await GetCollectionItemAsync(observer, keys, timeoutInSeconds);
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
            return await GetCollectionItemAsync(observer, [srcKey], timeoutInSeconds);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void StartMainLoop()
        {
            if (mainLoopTaskStarted == 0 && Interlocked.CompareExchange(ref mainLoopTaskStarted, 1, 0) == 0)
            {
                mainLoopTask = Task.Run(Start);
            }
        }

        private async Task<CollectionItemResult> GetCollectionItemAsync(CollectionItemObserver observer, byte[][] keys,
            double timeoutInSeconds)
        {
            // Add the session ID to observer mapping
            sessionIdToObserver.TryAdd(observer.Session.ObjectStoreSessionID, observer);

            // Start the main loop task if it hasn't been started yet
            StartMainLoop();

            // Add a new observer event to the event queue
            brokerEventsQueue.Enqueue(new NewObserverEvent(observer, keys));

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
                // Session is disposed
            }

            sessionIdToObserver.TryRemove(observer.Session.ObjectStoreSessionID, out _);

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void HandleCollectionUpdate(byte[] key)
        {
            if (keysToObservers is null)
                return;
            HandleCollectionUpdateWorker(key);
        }

        void HandleCollectionUpdateWorker(byte[] key)
        {
            ConcurrentQueue<CollectionItemObserver> observers = null;
            keysToObserversLock.ReadLock();
            try
            {
                if (!keysToObservers.TryGetValue(key, out observers) || observers.IsEmpty) return;
            }
            finally
            {
                keysToObserversLock.ReadUnlock();
            }

            if (observers != null)
            {
                // Add collection updated event to queue
                brokerEventsQueue.Enqueue(new CollectionUpdatedEvent(key, observers));
            }
        }

        /// <summary>
        /// Notify broker that a RespServerSession object is being disposed
        /// </summary>
        /// <param name="session">The disposed session</param>
        internal void HandleSessionDisposed(RespServerSession session)
        {
            // Try to remove session ID from mapping & get the observer object for the specified session, if exists
            if (!sessionIdToObserver.TryRemove(session.ObjectStoreSessionID, out var observer))
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
                    TryAssignItemFromKey(cue.Key, cue.Observers);
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
            keysToObserversLock.WriteLock();
            try
            {
                keysToObservers ??= new Dictionary<byte[], ConcurrentQueue<CollectionItemObserver>>(ByteArrayComparer.Instance);

                // Iterate over the keys in order, set the observer's result if collection in key contains an item
                foreach (var key in keys)
                {
                    // If the key already has a non-empty observer queue, it does not have an item to retrieve
                    // Otherwise, try to retrieve next available item
                    if ((keysToObservers.ContainsKey(key) && !keysToObservers[key].IsEmpty) ||
                        !TryGetResult(key, observer.Session.storageSession, observer.Command, observer.CommandArgs,
                            out _, out var result)) continue;

                    // An item was found - set the observer result and return
                    sessionIdToObserver.TryRemove(observer.Session.ObjectStoreSessionID, out _);
                    observer.HandleSetResult(result);
                    return;
                }

                // No item was found, enqueue new observer in every observed key's queue
                foreach (var key in keys)
                {
                    if (!keysToObservers.ContainsKey(key))
                        keysToObservers.Add(key, new ConcurrentQueue<CollectionItemObserver>());

                    keysToObservers[key].Enqueue(observer);
                }
            }
            finally
            {
                keysToObserversLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Try to assign item available (if exists) with next ready observer in queue
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="observers">Observers of updated key of collection from which to assign item</param>
        /// <returns>True if successful in assigning item</returns>
        private bool TryAssignItemFromKey(byte[] key, ConcurrentQueue<CollectionItemObserver> observers)
        {
            // Peek at next observer in queue
            while (observers.TryPeek(out var observer))
            {
                // If observer is not waiting for result, dequeue it and continue to next observer in queue
                if (observer.Status != ObserverStatus.WaitingForResult)
                {
                    _ = observers.TryDequeue(out _);
                    continue;
                }

                observer.ObserverStatusLock.EnterUpgradeableReadLock();
                try
                {
                    // If observer is not waiting for result, dequeue it and continue to next observer in queue
                    if (observer.Status != ObserverStatus.WaitingForResult)
                    {
                        _ = observers.TryDequeue(out _);
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
                    _ = observers.TryDequeue(out observer);

                    sessionIdToObserver.TryRemove(observer!.Session.ObjectStoreSessionID, out _);
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
        /// Try to get next available item from sorted set object based on command type
        /// BZPOPMIN and BZPOPMAX share same implementation since Dictionary.First() and Last() 
        /// handle the ordering automatically based on sorted set scores
        /// </summary>
        private static unsafe bool TryGetNextSetObjects(byte[] key, SortedSetObject sortedSetObj, int count, RespCommand command, ArgSlice[] cmdArgs, out CollectionItemResult result)
        {
            result = default;

            if (count == 0) return false;

            switch (command)
            {
                case RespCommand.BZPOPMIN:
                case RespCommand.BZPOPMAX:
                    var element = sortedSetObj.PopMinOrMax(command == RespCommand.BZPOPMAX);
                    result = new CollectionItemResult(key, element.Score, element.Element);
                    return true;

                case RespCommand.BZMPOP:
                    var lowScoresFirst = *(bool*)cmdArgs[0].ptr;
                    var popCount = *(int*)cmdArgs[1].ptr;
                    popCount = Math.Min(popCount, count);

                    var scores = new double[popCount];
                    var items = new byte[popCount][];

                    for (int i = 0; i < popCount; i++)
                    {
                        var popResult = sortedSetObj.PopMinOrMax(!lowScoresFirst);
                        scores[i] = popResult.Score;
                        items[i] = popResult.Element;
                    }

                    result = new CollectionItemResult(key, scores, items);
                    return true;

                default:
                    return false;
            }
        }

        private unsafe bool TryGetResult(byte[] key, StorageSession storageSession, RespCommand command, ArgSlice[] cmdArgs, out int currCount, out CollectionItemResult result)
        {
            currCount = default;
            result = default;
            var createTransaction = false;

            var objectType = command switch
            {
                RespCommand.BLPOP or RespCommand.BRPOP or RespCommand.BLMOVE or RespCommand.BLMPOP => GarnetObjectType.List,
                RespCommand.BZPOPMIN or RespCommand.BZPOPMAX or RespCommand.BZMPOP => GarnetObjectType.SortedSet,
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
                    if (dstStatusOp != GarnetStatus.NOTFOUND) dstObj = osDstObject.GarnetObject;
                }

                // Check for type match between the observer and the actual object type
                // If types match, get next item based on item type
                switch (osObject.GarnetObject)
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
                        currCount = setObj.Count();
                        if (objectType != GarnetObjectType.SortedSet)
                            return false;
                        if (currCount == 0)
                            return false;

                        return TryGetNextSetObjects(key, setObj, currCount, command, cmdArgs, out result);

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
                while (!cts.IsCancellationRequested)
                {
                    // Try to synchronously get the next event
                    if (!brokerEventsQueue.TryDequeue(out var nextEvent))
                    {
                        // Asynchronously dequeue next event in broker's queue
                        // once event is dequeued successfully, call handler method
                        try
                        {
                            nextEvent = await brokerEventsQueue.DequeueAsync(cts.Token);
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
            cts.Cancel();
            foreach (var observer in sessionIdToObserver.Values)
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

            var mainLoopStatus = mainLoopTaskStarted;
            while (Interlocked.CompareExchange(ref mainLoopTaskStarted, 2, mainLoopStatus) != mainLoopStatus)
            {
                mainLoopStatus = mainLoopTaskStarted;
            }

            if (mainLoopStatus == 1)
            {
                done.Wait();
            }
            done.Dispose();
            cts.Dispose();
        }
    }
}