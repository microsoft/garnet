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
    /// When a supported blocking command is initiated, RespServerSession will call the Subscribe method with the desired object type and operation
    /// and a list of keys to the desired objects.
    /// When an item is added to a collection, the StorageSession will call the Publish method with the relevant object key.
    /// The main loop, in the Start method, listens for published item additions and writes a response to the calling RespServerSession if an item was found.
    /// </summary>
    public class CollectionItemBroker : IDisposable
    {
        // Queue of keys whose objects need to be checked for items
        private readonly AsyncQueue<byte[]> updatedKeysQueue = new();

        // Queue of observers that have a specified timeout, prioritized by time of expiry in ticks
        private readonly PriorityQueue<CollectionItemObserver, long> expiringObservers = new();

        // Mapping of observed keys to queue of observers, by order of subscription
        private readonly ConcurrentDictionary<byte[], ConcurrentQueue<CollectionItemObserver>> keysToObservers = new(new ByteArrayComparer());

        // Mapping of RespServerSession ID (ObjectStoreSessionID) to observer instance
        private readonly ConcurrentDictionary<int, CollectionItemObserver> sessionIdToObserver = new();

        // Cancellation token for the main loop
        private readonly CancellationTokenSource cts = new();

        // Synchronization event for awaiting main loop to finish
        private readonly ManualResetEventSlim done = new(true);

        private bool disposed = false;
        private bool isStarted = false;
        private readonly object isStartedLock = new();
        private readonly ReaderWriterLockSlim expiringObserversLock = new();

        /// <summary>
        /// Subscribe blocking operation for list objects
        /// </summary>
        /// <param name="keys">Keys of objects to observe</param>
        /// <param name="operation">Type of list operation</param>
        /// <param name="session">Calling session instance</param>
        /// <param name="timeoutInSeconds">Timeout of operation (in seconds)</param>
        internal void ListSubscribe(byte[][] keys, ListOperation operation, RespServerSession session, double timeoutInSeconds)
        {
            Subscribe(keys, GarnetObjectType.List, (byte)operation, session, timeoutInSeconds);
        }

        /// <summary>
        /// Subscribe blocking operation for sorted set objects
        /// </summary>
        /// <param name="keys">Keys of objects to observe</param>
        /// <param name="operation">Type of sorted set operation</param>
        /// <param name="session">Calling session instance</param>
        /// <param name="timeoutInSeconds">Timeout of operation (in seconds)</param>
        internal void SortedSetSubscribe(byte[][] keys, SortedSetOperation operation, RespServerSession session, double timeoutInSeconds)
        {
            Subscribe(keys, GarnetObjectType.SortedSet, (byte)operation, session, timeoutInSeconds);
        }

        /// <summary>
        /// Publish an item addition to a collection object in specified key
        /// </summary>
        /// <param name="key">Key for which the value colleciton object was added to</param>
        internal void Publish(byte[] key)
        {
            // Check if main loop is started
            if (!isStarted)
            {
                lock (isStartedLock)
                {
                    if (!isStarted)
                    {
                        return;
                    }
                }
            }

            // Check if there are any observers to specified key
            if (!keysToObservers.ContainsKey(key)) return;

            // Add key to queue
            updatedKeysQueue.Enqueue(key);
        }

        /// <summary>
        /// Remove subscription for specified session
        /// </summary>
        /// <param name="session">The session for which to remove the subscription</param>
        internal void RemoveSubscription(RespServerSession session)
        {
            // Get the observer object for the specified session
            if (!sessionIdToObserver.TryGetValue(session.ObjectStoreSessionID, out var observer))
                return;

            // Change observer status to reflect that its session has been disposed
            observer.ObserverStatusLock.EnterWriteLock();
            try
            {
                observer.Status = ObserverStatus.SessionDisposed;
            }
            finally
            {
                observer.ObserverStatusLock.ExitWriteLock();
            }

            // Remove mapping of session ID to observer
            sessionIdToObserver.TryRemove(session.ObjectStoreSessionID, out _);
        }

        private void Subscribe(byte[][] keys, GarnetObjectType objectType, byte operation, RespServerSession session, double timeoutInSeconds)
        {
            // Create the new observer object
            var observer = new CollectionItemObserver(session, objectType, operation);

            // Add the session ID to observer mapping
            sessionIdToObserver.TryAdd(session.ObjectStoreSessionID, observer);

            // If timeout is specified, add observer to expiring sessions priority queue
            if (timeoutInSeconds > 0)
            {
                expiringObserversLock.EnterWriteLock();
                try
                {
                    expiringObservers.Enqueue(observer, DateTime.Now.AddSeconds(timeoutInSeconds).Ticks);
                }
                finally
                {
                    expiringObserversLock.ExitWriteLock();
                }
            }

            // Enqueue observer in each key's observer queue
            // Enqueue key in updated keys queue (in case there are already existing items to assign)
            foreach (var key in keys)
            {
                var queue = keysToObservers.GetOrAdd(key, new ConcurrentQueue<CollectionItemObserver>());
                queue.Enqueue(observer);

                updatedKeysQueue.Enqueue(key);
            }

            // Check if main loop has started, if not, start the main loop
            if (!isStarted)
            {
                lock (isStartedLock)
                {
                    if (!isStarted)
                    {
                        Task.Run(Start);
                    }
                    isStarted = true;
                }
            }
        }

        private void HandleExpiredObserver(CollectionItemObserver observer)
        {
            // If the observer session has been disposed, there is nothing to do
            if (observer.Status == ObserverStatus.SessionDisposed) return;

            observer.ObserverStatusLock.EnterReadLock();
            try
            {
                // If the observer session has not been disposed, write a null response to the client
                if (observer.Status != ObserverStatus.SessionDisposed)
                    observer.Session.WriteBlockedOperationResult(null, null);
            }
            finally
            {
                observer.ObserverStatusLock.ExitReadLock();
            }

        }

        private bool TryAssignItemFromKey(byte[] key)
        {
            if (!keysToObservers.TryGetValue(key, out var observers))
                return false;

            // Peek at next observer in queue
            while (observers.TryPeek(out var observer))
            {
                // If observer's session is disposed, dequeue it and continue to next observer in queue
                if (observer.Status == ObserverStatus.SessionDisposed)
                {
                    observers.TryDequeue(out observer);
                    continue;
                }

                observer.ObserverStatusLock.EnterReadLock();
                try
                {
                    // If observer's session is disposed, dequeue it and continue to next observer in queue
                    if (observer.Status == ObserverStatus.SessionDisposed)
                    {
                        observers.TryDequeue(out observer);
                        continue;
                    }

                    // Try to get next available item from object stored in key
                    if (!TryGetNextItem(key, observer.Session.storageSession, observer.ObjectType, observer.Operation,
                            out var currCount, out var nextItem))
                    {
                        // If unsuccessful getting next item but there is at least one item in the collection,
                        // continue to next observer in the queue, otherwise return
                        if (currCount > 0) continue;
                        return false;
                    }

                    // Dequeue the observer, and write the key and item to the client
                    observers.TryDequeue(out observer);
                    sessionIdToObserver.TryRemove(observer.Session.ObjectStoreSessionID, out _);
                    observer.Session.WriteBlockedOperationResult(key, nextItem);

                    return true;
                }
                finally
                {
                    observer.ObserverStatusLock.ExitReadLock();
                }
            }

            return false;
        }

        private bool TryGetNextListItem(ListObject listObj, byte operation, out byte[] nextItem)
        {
            nextItem = default;

            // If object has no items, return
            if (listObj.LnkList.Count == 0) return false;

            // Get the next object according to operation type
            switch ((ListOperation)operation)
            {
                case ListOperation.BRPOP:
                    nextItem = listObj.LnkList.Last.Value;
                    listObj.LnkList.RemoveLast();
                    break;
                case ListOperation.BLPOP:
                    nextItem = listObj.LnkList.First.Value;
                    listObj.LnkList.RemoveFirst();
                    break;
                default:
                    return false;
            }

            listObj.UpdateSize(nextItem, false);

            return true;
        }

        private bool TryGetNextSetObject(SortedSetObject sortedSetObj, byte operation, out byte[] nextItem)
        {
            nextItem = default;

            // If object has no items, return
            if (sortedSetObj.Dictionary.Count == 0) return false;

            // Get the next object according to operation type
            switch ((SetOperation)operation)
            {
                default:
                    return false;
            }

            return true;
        }

        private bool TryGetNextItem(byte[] key, StorageSession storageSession, GarnetObjectType objectType, byte operation, out int currCount, out byte[] nextItem)
        {
            currCount = default;
            nextItem = default;
            var createTransaction = false;

            // Create a transaction to try and get the next available item
            if (storageSession.txnManager.state != TxnState.Running)
            {
                Debug.Assert(storageSession.txnManager.state == TxnState.None);
                createTransaction = true;
                var asKey = storageSession.scratchBufferManager.CreateArgSlice(key);
                storageSession.txnManager.SaveKeyEntryToLock(asKey, true, LockType.Exclusive);
                _ = storageSession.txnManager.Run(true);
            }

            var objectLockableContext = storageSession.txnManager.ObjectStoreLockableContext;

            try
            {
                // Get the object stored at key
                var statusOp = storageSession.GET(key, out var osList, ref objectLockableContext);
                if (statusOp == GarnetStatus.NOTFOUND) return false;

                // Check for type match between the observer and the actual object type
                // If types match, get next item based on item type
                switch (osList.garnetObject)
                {
                    case ListObject listObj:
                        currCount = listObj.LnkList.Count;
                        if (objectType != GarnetObjectType.List) return false;
                        return TryGetNextListItem(listObj, operation, out nextItem);
                    case SortedSetObject setObj:
                        currCount = setObj.Dictionary.Count;
                        if (objectType != GarnetObjectType.SortedSet) return false;
                        return TryGetNextSetObject(setObj, operation, out nextItem);
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

        private async Task Start()
        {
            // Main loop logic
            try
            {
                Task dequeueTask = null;
                while (!disposed && !cts.IsCancellationRequested)
                {
                    var currTicks = DateTime.Now.Ticks;
                    long nextExpiryInTicks;

                    // Handle any expired observers
                    expiringObserversLock.EnterUpgradeableReadLock();
                    try
                    {
                        // Peek at first observer in queue and check if it's expired
                        if (expiringObservers.TryPeek(out var observer, out nextExpiryInTicks) && nextExpiryInTicks <= currTicks)
                        {
                            expiringObserversLock.EnterWriteLock();
                            try
                            {
                                // Dequeue and handle all expired observers
                                while (nextExpiryInTicks <= currTicks)
                                {
                                    expiringObservers.Dequeue();
                                    HandleExpiredObserver(observer);
                                    if (!expiringObservers.TryPeek(out observer, out nextExpiryInTicks))
                                        break;
                                }
                            }
                            finally
                            {
                                expiringObserversLock.ExitWriteLock();
                            }
                        }
                    }
                    finally
                    {
                        expiringObserversLock.ExitUpgradeableReadLock();
                    }

                    // Check if current dequeue task is done
                    if (dequeueTask == null || dequeueTask.IsCompleted)
                    {
                        // Set dequeue task to get next updated key and call assignment method
                        dequeueTask = updatedKeysQueue.DequeueAsync(cts.Token).ContinueWith(t =>
                        {
                            if (t.Status == TaskStatus.RanToCompletion)
                                TryAssignItemFromKey(t.Result);
                        }, cts.Token);
                    }

                    // If next observer expiry exists, wait until the expiry or until dequeue task is done
                    await (nextExpiryInTicks == default
                        ? dequeueTask
                        : Task.WhenAny(dequeueTask, Task.Delay(TimeSpan.FromTicks(nextExpiryInTicks - currTicks))));
                }
            }
            finally
            {
                done.Set();
            }
        }

        public void Dispose()
        {
            disposed = true;
            cts.Cancel();
            done.Wait();
        }

        /// <summary>
        /// This class defines an observer for a specific blocking command
        /// </summary>
        private class CollectionItemObserver
        {
            // The session in which the blocking command was called
            internal RespServerSession Session { get; }

            // The object type of the blocking command
            internal GarnetObjectType ObjectType { get; }

            // The operation type for the blocking command
            internal byte Operation { get; }

            // Status of the observer
            internal ObserverStatus Status { get; set; } = ObserverStatus.Ready;

            // Lock for the status of the observer
            internal ReaderWriterLockSlim ObserverStatusLock { get; } = new();

            internal CollectionItemObserver(RespServerSession session, GarnetObjectType objestType, byte operation)
            {
                Session = session;
                ObjectType = objestType;
                Operation = operation;
            }
        }

        private enum ObserverStatus
        {
            Ready,
            SessionDisposed,
        }
    }
}
