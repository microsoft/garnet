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
    public class CollectionItemBroker : IDisposable
    {
        private readonly AsyncQueue<byte[]> updatedKeysQueue = new();

        private readonly CancellationTokenSource cts = new();
        private readonly ManualResetEvent done = new(true);
        private readonly PriorityQueue<CollectionItemObserver, long> expiringObservers = new();
        private readonly ConcurrentDictionary<byte[], ConcurrentQueue<CollectionItemObserver>> keysToObservers = new(new ByteArrayComparer());
        private readonly ConcurrentDictionary<int, CollectionItemObserver> sessionIdToObserver = new();

        private bool disposed = false;
        private bool isStarted = false;
        private readonly object isStartedLock = new();
        private readonly ReaderWriterLockSlim expiringObserversLock = new();

        internal void ListSubscribe(byte[][] keys, ListOperation operation, RespServerSession session, double timeoutInSeconds)
        {
            Subscribe(keys, GarnetObjectType.List, (byte)operation, session, timeoutInSeconds);
        }

        internal void SortedSetSubscribe(byte[][] keys, SortedSetOperation operation, RespServerSession session, double timeoutInSeconds)
        {
            Subscribe(keys, GarnetObjectType.SortedSet, (byte)operation, session, timeoutInSeconds);
        }

        private void Subscribe(byte[][] keys, GarnetObjectType objectType, byte operation, RespServerSession session, double timeoutInSeconds)
        {
            var observer = new CollectionItemObserver(session, objectType, operation);

            sessionIdToObserver.TryAdd(session.ObjectStoreSessionID, observer);

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

            foreach (var key in keys)
            {
                var queue = keysToObservers.GetOrAdd(key, new ConcurrentQueue<CollectionItemObserver>());
                queue.Enqueue(observer);

                updatedKeysQueue.Enqueue(key);
            }

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

        internal void RemoveSubscription(RespServerSession session)
        {
            if (!sessionIdToObserver.TryGetValue(session.ObjectStoreSessionID, out var observer))
                return;

            observer.ObserverStatusLock.EnterWriteLock();
            try
            {
                observer.Status = ObserverStatus.SessionDisposed;
            }
            finally
            {
                observer.ObserverStatusLock.ExitWriteLock();
            }

            sessionIdToObserver.TryRemove(session.ObjectStoreSessionID, out _);
        }

        internal void Publish(byte[] key)
        {
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

            updatedKeysQueue.Enqueue(key);
        }

        private void HandleExpiredObserver(CollectionItemObserver observer)
        {
            if (observer.Status == ObserverStatus.SessionDisposed) return;

            observer.ObserverStatusLock.EnterReadLock();
            try
            {
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
            if (!keysToObservers.ContainsKey(key)) return false;

            while (keysToObservers[key].TryPeek(out var observer))
            {
                if (observer.Status == ObserverStatus.SessionDisposed)
                {
                    keysToObservers[key].TryDequeue(out observer);
                    continue;
                }

                observer.ObserverStatusLock.EnterReadLock();
                try
                {
                    if (observer.Status == ObserverStatus.SessionDisposed)
                    {
                        keysToObservers[key].TryDequeue(out observer);
                        continue;
                    }

                    if (!TryGetNextItem(key, observer.Session.storageSession, observer.ObjectType, observer.Operation,
                            out var currCount, out var nextItem))
                    {
                        if (currCount > 0) continue;
                        return false;
                    }

                    keysToObservers[key].TryDequeue(out observer);
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

            if (listObj.LnkList.Count == 0) return false;

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

            if (sortedSetObj.Dictionary.Count == 0) return false;

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
                var statusOp = storageSession.GET(key, out var osList, ref objectLockableContext);
                if (statusOp == GarnetStatus.NOTFOUND) return false;

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
            try
            {
                Task dequeueTask = null;
                while (!disposed && !cts.IsCancellationRequested)
                {
                    var currTicks = DateTime.Now.Ticks;

                    long nextExpiryInTicks;
                    expiringObserversLock.EnterUpgradeableReadLock();
                    try
                    {
                        if (expiringObservers.TryPeek(out var observer, out nextExpiryInTicks) && nextExpiryInTicks <= currTicks)
                        {
                            expiringObserversLock.EnterWriteLock();
                            try
                            {
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

                    if (dequeueTask == null || dequeueTask.IsCompleted)
                    {
                        dequeueTask = updatedKeysQueue.DequeueAsync(cts.Token).ContinueWith(t =>
                        {
                            if (t.Status == TaskStatus.RanToCompletion)
                                TryAssignItemFromKey(t.Result);
                        }, cts.Token);
                    }

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
            done.WaitOne();
        }

        private class CollectionItemObserver
        {
            internal RespServerSession Session { get; }

            internal GarnetObjectType ObjectType { get; }

            internal byte Operation { get; }

            internal ReaderWriterLockSlim ObserverStatusLock { get; } = new();

            internal ObserverStatus Status { get; set; } = ObserverStatus.Ready;

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
