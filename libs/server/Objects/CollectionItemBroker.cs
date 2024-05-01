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
        private readonly ConcurrentDictionary<byte[], ConcurrentQueue<CollectionItemObserver>> keysToObservers = new();
        private readonly ConcurrentDictionary<RespServerSession, bool> activeObserverSessions;

        private bool disposed = false;
        private bool isStarted = false;
        private readonly object isStartedLock = new();

        internal void Subscribe(byte[][] keys, byte operation, RespServerSession session, double timeoutInSeconds)
        {
            var observer = new CollectionItemObserver(session, operation);

            activeObserverSessions.TryAdd(session, true);

            if (timeoutInSeconds > 0)
                expiringObservers.Enqueue(observer, DateTime.Now.AddSeconds(timeoutInSeconds).Ticks);

            foreach (var key in keys)
            {
                keysToObservers.GetOrAdd(key, new ConcurrentQueue<CollectionItemObserver>());
                keysToObservers[key].Enqueue(observer);

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
            activeObserverSessions.TryRemove(session, out _);
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
            observer.Session.WriteBlockedOperationResult(null);
        }

        private bool TryAssignItemFromKey(byte[] key)
        {
            if (!keysToObservers.ContainsKey(key)) return false;

            CollectionItemObserver observer = default;
            while (keysToObservers[key].TryPeek(out observer))
            {
                if (!activeObserverSessions.ContainsKey(observer.Session))
                {
                    keysToObservers[key].TryDequeue(out observer);
                    continue;
                }

                break;
            }

            if (observer == default || !TryGetNextItem(key, observer.Session.storageSession, observer.Operation, out var nextItem))
                return false;

            observer.Session.WriteBlockedOperationResult(nextItem);

            return true;
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

        private bool TryGetNextItem(byte[] key, StorageSession storageSession, byte operation, out byte[] nextItem)
        {
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
                        return TryGetNextListItem(listObj, operation, out nextItem);
                    case SortedSetObject setObj:
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

                    if (expiringObservers.TryPeek(out var observer, out var nextExpiryInTicks))
                    {
                        while (nextExpiryInTicks <= currTicks)
                        {
                            expiringObservers.Dequeue();
                            HandleExpiredObserver(observer);
                            expiringObservers.TryPeek(out observer, out nextExpiryInTicks);
                        }
                    }

                    dequeueTask ??= updatedKeysQueue.DequeueAsync(cts.Token).ContinueWith(t =>
                    {
                        dequeueTask = null;
                        if (t.Status == TaskStatus.RanToCompletion)
                            TryAssignItemFromKey(t.Result);
                    }, cts.Token);

                    await (nextExpiryInTicks == default
                        ? dequeueTask
                        : Task.WhenAny(dequeueTask, Task.Delay(TimeSpan.FromTicks(currTicks - nextExpiryInTicks))));
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
            internal byte Operation { get; }

            internal CollectionItemObserver(RespServerSession session, byte operation)
            {
                Session = session;
                Operation = operation;
            }
        }
    }


}
