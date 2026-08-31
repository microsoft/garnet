// Copyright (c) Microsoft Corporation.
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
        // Minimum amount of seconds between cleanings of keysToObservers map
        private const int MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS = 5 * 60;

        // Queue of events to be handled by the main loops
        readonly AsyncQueue<CollectionItemBrokerEvent> brokerEventsQueue = new();

        // Mapping of RespServerSession ID (ObjectStoreSessionID) to observer instance
        readonly ConcurrentDictionary<int, CollectionItemObserver> sessionIdToObserver = new();

        // Mapping of observed keys to queue of observers, by order of subscription
        // Instantiated only when needed
        Dictionary<byte[], ConcurrentQueue<CollectionItemObserver>> keysToObservers = null;

        // Last time keysToObservers was cleaned (in ticks)
        long keysToObserversTimeLastClean = DateTime.Now.Ticks;

        // Minimum amount of time between cleanings of keysToObservers (in ticks)
        readonly long keysToObserversTimeBetweenCleans = TimeSpan.FromSeconds(MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS).Ticks;

        // Synchronization for the keysToObservers dictionary
        SingleWriterMultiReaderLock keysToObserversLock = new();

        // Cancellation token for the main loop
        readonly CancellationTokenSource cts = new();

        // Synchronization for awaiting main loop to finish
        readonly ManualResetEventSlim done = new(false);

        // Task for the main loop, we keep field for diagnostic purposes
        Task mainLoopTask = null;

        // Integer to indicate main loop status
        int mainLoopTaskStatus = MAIN_LOOP_NOT_STARTED;

        // Constants denoting status of main loop
        private const int MAIN_LOOP_NOT_STARTED = 0;
        private const int MAIN_LOOP_STARTED = 1;
        private const int MAIN_LOOP_DISPOSED = 2;

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
            RespServerSession session, double timeoutInSeconds, PinnedSpanByte[] cmdArgs = null)
        {
            var observer = new CollectionItemObserver(session, command, cmdArgs);
            return await GetCollectionItemAsync(observer, keys, timeoutInSeconds).ConfigureAwait(false);
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
            RespServerSession session, double timeoutInSeconds, PinnedSpanByte[] cmdArgs)
        {
            var observer = new CollectionItemObserver(session, command, cmdArgs);
            return await GetCollectionItemAsync(observer, [srcKey], timeoutInSeconds).ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void StartMainLoop()
        {
            if (mainLoopTaskStatus == MAIN_LOOP_NOT_STARTED &&
                Interlocked.CompareExchange(ref mainLoopTaskStatus, MAIN_LOOP_STARTED, MAIN_LOOP_NOT_STARTED) == MAIN_LOOP_NOT_STARTED)
            {
                mainLoopTask = Task.Run(StartAsync);
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
            brokerEventsQueue.Enqueue(CollectionItemBrokerEvent.CreateNewObserverEvent(observer, keys));

            var timeout = timeoutInSeconds == 0
                ? TimeSpan.FromMilliseconds(-1)
                : TimeSpan.FromSeconds(timeoutInSeconds);

            try
            {
                // Wait for either the result found notification or the timeout to expire
                await observer.ResultFoundSemaphore.WaitAsync(timeout, observer.CancellationTokenSource.Token).ConfigureAwait(false);
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
            ConcurrentQueue<CollectionItemObserver> observers;

            keysToObserversLock.ReadLock();
            try
            {
                if (!keysToObservers.TryGetValue(key, out observers))
                    return;
            }
            finally
            {
                keysToObserversLock.ReadUnlock();
            }

            // If the observer queue is empty, remove the key entry
            if (observers.IsEmpty)
            {
                keysToObserversLock.WriteLock();
                try
                {
                    if (!keysToObservers.TryGetValue(key, out observers))
                        return;

                    if (observers.IsEmpty)
                    {
                        keysToObservers.Remove(key);
                        return;
                    }
                }
                finally
                {
                    keysToObserversLock.WriteUnlock();
                }
            }

            // Add collection updated event to queue
            brokerEventsQueue.Enqueue(CollectionItemBrokerEvent.CreateCollectionUpdatedEvent(key));
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
        /// <param name="brokerEvent">Event to handle</param>
        private void HandleBrokerEvent(ref CollectionItemBrokerEvent brokerEvent)
        {
            switch (brokerEvent.EventType)
            {
                case CollectionItemBrokerEventType.NewObserver:
                    InitializeObserver(brokerEvent.Observer, brokerEvent.Keys);
                    return;
                case CollectionItemBrokerEventType.CollectionUpdated:
                    TryAssignItemFromKey(brokerEvent.Key);
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
                    if (keysToObservers.ContainsKey(key) && !keysToObservers[key].IsEmpty)
                        continue;

                    // The key has an empty observer queue, try to retrieve next available item
                    if (!TryGetResult(key, observer.Session.storageSession, observer.Command, observer.CommandArgs, failOnSrcTypeMismatch: true,
                            out _, out var result))
                        continue;

                    // An item was found - set the observer result and return
                    sessionIdToObserver.TryRemove(observer.Session.ObjectStoreSessionID, out _);
                    observer.HandleSetResult(result);

                    // The key still has an empty observer queue, and the current observer retrieved a result, so we can remove the key.
                    keysToObservers.Remove(key);

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
        /// <returns>True if successful in assigning item</returns>
        private bool TryAssignItemFromKey(byte[] key)
        {
            ConcurrentQueue<CollectionItemObserver> observers;

            keysToObserversLock.ReadLock();
            try
            {
                keysToObservers.TryGetValue(key, out observers);

                if (observers != null && !observers.IsEmpty)
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

                        observer.ObserverStatusLock.WriteLock();
                        try
                        {
                            // If observer is not waiting for result, dequeue it and continue to next observer in queue
                            if (observer.Status != ObserverStatus.WaitingForResult)
                            {
                                _ = observers.TryDequeue(out _);
                                continue;
                            }

                            // Try to get next available item from object stored in key
                            if (!TryGetResult(key, observer.Session.storageSession, observer.Command, observer.CommandArgs, failOnSrcTypeMismatch: false,
                                    out var currCount, out var result))
                            {
                                // If unsuccessful getting next item but there is at least one item in the collection,
                                // continue to next observer in the queue, otherwise return
                                if (currCount > 0)
                                    continue;
                                return false;
                            }

                            // Dequeue the observer, and set the observer's result
                            _ = observers.TryDequeue(out observer);

                            sessionIdToObserver.TryRemove(observer!.Session.ObjectStoreSessionID, out _);

                            observer.HandleSetResult(result, true);

                            return true;
                        }
                        finally
                        {
                            observer.ObserverStatusLock.WriteUnlock();
                        }
                    }
                }
            }
            finally
            {
                keysToObserversLock.ReadUnlock();
            }

            if (observers != null && observers.IsEmpty)
            {
                keysToObserversLock.WriteLock();
                try
                {
                    if (keysToObservers.TryGetValue(key, out observers) && observers.IsEmpty)
                    {
                        keysToObservers.Remove(key);
                    }
                }
                finally
                {
                    keysToObserversLock.WriteUnlock();
                }
            }

            return false;
        }

        /// <summary>
        /// Pops the next available item(s) from the list at <paramref name="asKey"/> using RMW, so that the update is
        /// applied by Tsavorite rather than by mutating the heap object in place.
        /// </summary>
        private static unsafe bool TryGetNextListResult<TObjectContext>(byte[] key, PinnedSpanByte asKey,
            PinnedSpanByte dstKey, StorageSession storageSession, RespCommand command, PinnedSpanByte[] cmdArgs,
            ListObject srcList, int currCount, ref TObjectContext objectContext, out CollectionItemResult result,
            out byte[] notifyKey)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            result = default;
            notifyKey = null;

            switch (command)
            {
                case RespCommand.BLPOP:
                case RespCommand.BRPOP:
                    {
                        var status = storageSession.ListPop(asKey,
                            command == RespCommand.BLPOP ? ListOperation.LPOP : ListOperation.RPOP,
                            ref objectContext, out var element);

                        if (status != GarnetStatus.OK || !element.IsValid)
                            return false;

                        result = new CollectionItemResult(key, element.ToArray());
                        return true;
                    }
                case RespCommand.BLMOVE:
                    {
                        var srcDirection = (OperationDirection)cmdArgs[1].ReadOnlySpan[0];
                        var dstDirection = (OperationDirection)cmdArgs[2].ReadOnlySpan[0];

                        if (srcDirection == OperationDirection.Unknown || dstDirection == OperationDirection.Unknown)
                            return false;

                        // Moving between the same ends of one list, or rotating a single-element list, leaves the
                        // list unchanged. Returning without the pop/push also avoids emptying the key, which would
                        // drop its TTL.
                        if (asKey.ReadOnlySpan.SequenceEqual(dstKey.ReadOnlySpan) &&
                            (srcDirection == dstDirection || currCount == 1))
                        {
                            var unchanged = srcDirection == OperationDirection.Right
                                ? srcList.LnkList.Last.Value
                                : srcList.LnkList.First.Value;
                            result = new CollectionItemResult(key, unchanged.AsSpan().ToArray());
                            return true;
                        }

                        var status = storageSession.ListPop(asKey,
                                srcDirection == OperationDirection.Left ? ListOperation.LPOP : ListOperation.RPOP,
                                ref objectContext, out var element);

                        if (status != GarnetStatus.OK || !element.IsValid)
                            return false;

                        // The result outlives the session's scratch buffers, so copy the element onto the heap.
                        var movedItem = element.ToArray();

                        // The destination type was already validated against the source type by the caller, so the
                        // push cannot fail on type mismatch here.
                        // The broker's observer lock is held here, so the push must not notify the broker inline.
                        // The destination key is reported back instead, and notified once the lock is released.
                        status = storageSession.ListPush(dstKey, element,
                            dstDirection == OperationDirection.Left ? ListOperation.LPUSH : ListOperation.RPUSH,
                            out _, ref objectContext, notifyItemBroker: false);

                        // The destination type was already validated against the source type by the caller, and the
                        // push creates the destination if it is absent, so the push cannot fail here. If it ever did,
                        // the element has already been popped and the transaction is committed unconditionally, so
                        // deliver it to the blocked client rather than discard it.
                        if (status != GarnetStatus.OK)
                        {
                            Debug.Assert(false, "List push to the BLMOVE destination failed after the source pop");
                            result = new CollectionItemResult(key, movedItem);
                            return true;
                        }

                        notifyKey = dstKey.ToArray();
                        result = new CollectionItemResult(key, movedItem);
                        return true;
                    }
                case RespCommand.BLMPOP:
                    {
                        var popDirection = (OperationDirection)cmdArgs[0].ReadOnlySpan[0];
                        var popCount = Math.Min(*(int*)cmdArgs[1].ToPointer(), currCount);

                        var status = storageSession.ListPop(asKey, popCount,
                            popDirection == OperationDirection.Left ? ListOperation.LPOP : ListOperation.RPOP,
                            ref objectContext, out var elements);

                        if (status != GarnetStatus.OK || elements is null || elements.Length == 0)
                            return false;

                        var items = new byte[elements.Length][];
                        for (var i = 0; i < elements.Length; i++)
                            items[i] = elements[i].ToArray();

                        result = new CollectionItemResult(key, items);
                        return true;
                    }
                default:
                    return false;
            }
        }

        /// <summary>
        /// Pops the next available item(s) from the sorted set at <paramref name="asKey"/> using RMW, so that the
        /// update is applied by Tsavorite rather than by mutating the heap object in place.
        /// BZPOPMIN and BZPOPMAX share the same implementation, differing only in pop order.
        /// </summary>
        private static unsafe bool TryGetNextSortedSetResult<TObjectContext>(byte[] key, PinnedSpanByte asKey,
            StorageSession storageSession, RespCommand command, PinnedSpanByte[] cmdArgs, int currCount,
            ref TObjectContext objectContext, out CollectionItemResult result)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            result = default;

            bool lowScoresFirst;
            int popCount;

            switch (command)
            {
                case RespCommand.BZPOPMIN:
                case RespCommand.BZPOPMAX:
                    lowScoresFirst = command == RespCommand.BZPOPMIN;
                    popCount = 1;
                    break;
                case RespCommand.BZMPOP:
                    lowScoresFirst = *(bool*)cmdArgs[0].ToPointer();
                    popCount = Math.Min(*(int*)cmdArgs[1].ToPointer(), currCount);
                    break;
                default:
                    return false;
            }

            var status = storageSession.SortedSetPop(asKey, popCount, lowScoresFirst, out var pairs, ref objectContext);

            if (status != GarnetStatus.OK || pairs is null || pairs.Length == 0)
                return false;

            if (command == RespCommand.BZMPOP)
            {
                var scores = new double[pairs.Length];
                var items = new byte[pairs.Length][];

                for (var i = 0; i < pairs.Length; i++)
                {
                    scores[i] = ParseScoreOrDefault(pairs[i].score);
                    items[i] = pairs[i].member.ToArray();
                }

                result = new CollectionItemResult(key, scores, items);
                return true;
            }

            result = new CollectionItemResult(key, ParseScoreOrDefault(pairs[0].score), pairs[0].member.ToArray());
            return true;
        }

        /// <summary>
        /// Parses a score emitted by the sorted set pop output.
        /// The members have already been popped and the transaction is committed unconditionally, so a parse failure
        /// must still deliver the member to the blocked client rather than discard it. Scores are written by
        /// <see cref="RespWriteUtils.TryWriteDoubleBulkString"/> using the shortest round-trippable form and NaN is
        /// rejected at insert time, so this fallback is not expected to be reachable.
        /// </summary>
        private static double ParseScoreOrDefault(PinnedSpanByte score)
        {
            if (ParseUtils.TryReadDouble(score, out var parsed, canBeInfinite: true))
                return parsed;

            Debug.Assert(false, "Sorted set score emitted by the object store failed to round-trip");
            return default;
        }

        private unsafe bool TryGetResult(byte[] key, StorageSession storageSession, RespCommand command,
            PinnedSpanByte[] cmdArgs, bool failOnSrcTypeMismatch, out int currCount, out CollectionItemResult result)
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

            PinnedSpanByte dstKey = default;
            if (command == RespCommand.BLMOVE)
                dstKey = cmdArgs[0];

            var asKey = storageSession.scratchBufferBuilder.CreateArgSlice(key);

            // Create a transaction if not currently in a running transaction
            if (storageSession.txnManager.state != TxnState.Running)
            {
                Debug.Assert(storageSession.txnManager.state == TxnState.None);
                createTransaction = true;
                storageSession.txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                storageSession.txnManager.SaveKeyEntryToLock(asKey, LockType.Exclusive);

                if (command == RespCommand.BLMOVE)
                    storageSession.txnManager.SaveKeyEntryToLock(dstKey, LockType.Exclusive);

                _ = storageSession.txnManager.Run(true);
            }

            var objectTransactionalContext = storageSession.txnManager.ObjectTransactionalContext;

            // Key whose observers must be notified once this operation completes (BLMOVE destination).
            byte[] notifyKey = null;

            try
            {
                // Get the object stored at key
                var statusOp = storageSession.GET(asKey, out var osObject, ref objectTransactionalContext);
                if (statusOp == GarnetStatus.NOTFOUND)
                    return false;

                // Check for type match between the observer and the source object type
                if (statusOp == GarnetStatus.WRONGTYPE || (GarnetObjectType)osObject.GarnetObject.Type != objectType)
                {
                    // Return a type mismatch result if we should fail on source object type mismatch
                    if (failOnSrcTypeMismatch)
                    {
                        result = CollectionItemResult.TypeMismatch;
                        return true;
                    }

                    return false;
                }

                IGarnetObject dstObj = null;
                if (command == RespCommand.BLMOVE)
                {
                    var dstStatusOp = storageSession.GET(dstKey, out var osDstObject, ref objectTransactionalContext);
                    if (dstStatusOp != GarnetStatus.NOTFOUND)
                    {
                        dstObj = osDstObject.GarnetObject;

                        // If there is a destination object type mismatch, we should always return a type mismatch result
                        if (dstStatusOp == GarnetStatus.WRONGTYPE || (GarnetObjectType)dstObj.Type != objectType)
                        {
                            result = CollectionItemResult.TypeMismatch;
                            return true;
                        }
                    }
                }

                // Get next item based on item type. The objects read above are only inspected for their type and
                // element count; all mutation is performed through RMW below so that Tsavorite controls whether the
                // record is updated in place or copied into the mutable region. Mutating the heap object returned by
                // GET directly would be lost for records that are no longer mutable, and may corrupt records that the
                // flush path is concurrently serializing.
                switch (osObject.GarnetObject)
                {
                    case ListObject listObj:
                        currCount = listObj.LnkList.Count;
                        if (currCount == 0)
                            return false;

                        return TryGetNextListResult(key, asKey, dstKey, storageSession, command, cmdArgs, listObj,
                            currCount, ref objectTransactionalContext, out result, out notifyKey);

                    case SortedSetObject sortedSetObj:
                        currCount = sortedSetObj.Count();
                        if (currCount == 0)
                            return false;

                        return TryGetNextSortedSetResult(key, asKey, storageSession, command, cmdArgs, currCount,
                            ref objectTransactionalContext, out result);

                    default:
                        return false;
                }
            }
            finally
            {
                storageSession.scratchBufferBuilder.RewindScratchBuffer(asKey);
                if (createTransaction)
                    storageSession.txnManager.Commit(true);

                // Wake observers blocked on the destination key. The event is enqueued directly rather than going
                // through HandleCollectionUpdate, because callers of this method hold keysToObserversLock and
                // re-acquiring it here would deadlock. The broker loop resolves the event asynchronously and
                // tolerates keys that have no observers.
                if (notifyKey is not null)
                    brokerEventsQueue.Enqueue(CollectionItemBrokerEvent.CreateCollectionUpdatedEvent(notifyKey));
            }
        }

        /// <summary>
        /// Broker's main loop logic
        /// </summary>
        /// <returns>Task</returns>
        private async Task StartAsync()
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
                            nextEvent = await brokerEventsQueue.DequeueAsync(cts.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            continue;
                        }
                    }

                    HandleBrokerEvent(ref nextEvent);

                    // Check if keysToObservers requires cleaning
                    if (keysToObserversTimeLastClean + keysToObserversTimeBetweenCleans < DateTime.Now.Ticks)
                        CleanKeysToObservers();
                }
            }
            finally
            {
                done.Set();
            }
        }

        private void CleanKeysToObservers()
        {
            keysToObserversLock.WriteLock();
            try
            {
                foreach (var kvp in keysToObservers)
                {
                    // Pop disposed observers from head of queue
                    while (kvp.Value.TryPeek(out var observer) && observer.Status != ObserverStatus.WaitingForResult)
                        kvp.Value.TryDequeue(out _);

                    // Remove key from map if queue is now empty
                    if (kvp.Value.IsEmpty)
                        keysToObservers.Remove(kvp.Key);
                }

                // Update last cleaned time
                keysToObserversTimeLastClean = DateTime.Now.Ticks;
            }
            finally
            {
                keysToObserversLock.WriteUnlock();
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

            var prevMainLoopStatus = Interlocked.Exchange(ref mainLoopTaskStatus, MAIN_LOOP_DISPOSED);

            if (prevMainLoopStatus == MAIN_LOOP_STARTED)
            {
                done.Wait();
            }

            done.Dispose();
            cts.Dispose();
        }
    }
}