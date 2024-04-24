// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Tsavorite.core;

namespace Garnet.server.Objects.List
{
    internal class ListObserver : IObserver<byte[]>, IDisposable
    {
        private CancellationTokenSource cancellationTokenSource;
        private byte[] nextItem;
        private readonly TimeSpan timeout;
        private readonly ListItemBroker broker;
        private IDisposable unsubscriber;

        public byte[][] Keys { get; }
        public ListOperation ListOperation { get; }

        public ListObserver(byte[][] keys, ListOperation lop, double timeout, ListItemBroker broker)
        {
            Keys = keys;
            ListOperation = lop;
            this.timeout = timeout > 0 ? TimeSpan.FromSeconds(timeout) : Timeout.InfiniteTimeSpan;
            this.broker = broker;
        }

        public async Task<byte[]> GetNextItemAsync()
        {
            cancellationTokenSource = new CancellationTokenSource();
            
            using (unsubscriber = broker.Subscribe(this))
            {
                await Task.Delay(timeout, cancellationTokenSource.Token);
            }
            return nextItem;
        }

        public void OnCompleted()
        {
            cancellationTokenSource.Cancel(false);
        }

        public void OnError(Exception error)
        {
            cancellationTokenSource.Cancel(false);
        }

        public void OnNext(byte[] value)
        {
            nextItem = value;
            unsubscriber.Dispose();
            cancellationTokenSource.Cancel(false);
        }

        public void Dispose()
        {
            cancellationTokenSource?.Dispose();
            unsubscriber?.Dispose();
        }
    }

    public class ListItemBroker : IObservable<byte[]>
    {
        private readonly ConcurrentDictionary<byte[], ConcurrentQueue<ListObserver>> keysToObservers = new(new ByteArrayComparer());
        private readonly ConcurrentDictionary<ListObserver, byte> activeObservers = new();

        private readonly StorageSession storageSession;

        internal ListItemBroker(StorageSession storageSession)
        {
            this.storageSession = storageSession;
        }

        public IDisposable Subscribe(IObserver<byte[]> observer)
        {
            if (observer is not ListObserver listObserver) 
                throw new ArgumentException(nameof(observer));

            activeObservers.TryAdd(listObserver, 0);

            foreach (var key in listObserver.Keys)
            {
                EnqueueObserver(key, listObserver);
                TryAssignNextItem(key, listObserver.ListOperation);
            }

            return new ListUnsubscriber(activeObservers, listObserver);
        }

        public bool TryAssignNextItem(byte[] key, ListOperation lop = ListOperation.BRPOP)
        {
            if (!keysToObservers.ContainsKey(key)) return false;

            var nextItem = TryGetNextItem(key, lop);

            if (nextItem == null) return false;

            while (true)
            {
                if (!TryDequeueObserver(key, out var observer)) break;
                if (!activeObservers.TryGetValue(observer, out _)) continue;
                
                observer.OnNext(nextItem);
                return true;
            }

            return false;
        }

        private byte[] TryGetNextItem(byte[] key, ListOperation lop)
        {
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

            byte[] nextItem = null;
            try
            {
                var statusOp = storageSession.GET(key.ToArray(), out var osList, ref objectLockableContext);

                var listObj = (ListObject)osList.garnetObject;
                if (statusOp == GarnetStatus.NOTFOUND || listObj.LnkList.Count == 0)
                {
                    return null;
                }
                else if (statusOp == GarnetStatus.OK)
                {
                    switch (lop)
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
                            return null;
                    }

                    listObj.UpdateSize(nextItem, false);
                }
            }
            finally
            {
                if (createTransaction)
                    storageSession.txnManager.Commit(true);
            }

            return nextItem;
        }

        private void EnqueueObserver(byte[] key, ListObserver observer)
        {
            var queue = keysToObservers.GetOrAdd(key, new ConcurrentQueue<ListObserver>());
            lock (queue)
            {
                keysToObservers[key].Enqueue(observer);
            }
        }

        private bool TryDequeueObserver(byte[] key, out ListObserver observer)
        {
            observer = default;
            if (!keysToObservers.TryGetValue(key, out var queue)) return false;

            lock (queue)
            {
                queue.TryDequeue(out observer);
                if (queue.IsEmpty)
                    keysToObservers.TryRemove(key, out _);
            }

            return true;
        }
    }

    internal sealed class ListUnsubscriber : IDisposable
    {
        private readonly ConcurrentDictionary<ListObserver, byte> observers;
        private readonly ListObserver observer;

        internal ListUnsubscriber(ConcurrentDictionary<ListObserver, byte> observers,
            ListObserver observer) => (this.observers, this.observer) = (observers, observer);

        public void Dispose()
        {
            observers.TryRemove(observer, out _);
        }
    }
}
