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
    /// <remarks>
    /// <para>
    /// The main loop serves one event at a time, and its attempts to serve an observer may wait on the device:
    /// an attempt that finds a collection on disk awaits the read rather than holding a thread on it. The loop
    /// alone mutates the observer map, so network threads reporting a collection update take no lock: they
    /// only look the key up, and raise an event if observers are waiting on it.
    /// </para>
    /// <para>
    /// An update cannot be lost between those two sides. The loop registers an observer before it attempts to
    /// serve it, and the store serializes the attempt with the update, as it does any two operations on one key.
    /// Either the update comes first, and the attempt sees the item, or the attempt does, and the update - which
    /// looks the key up once it has been applied - sees the registration and raises an event.
    /// </para>
    /// <para>
    /// An attempt for a single-key command is one pop, which locks nothing. A BLMOVE attempt locks its source and
    /// destination and keeps them locked across a device wait, as a transaction does. An update to one of those
    /// keys meanwhile retries until the attempt ends.
    /// </para>
    /// </remarks>
    public class CollectionItemBroker : IDisposable
    {
        // Minimum amount of seconds between cleanings of keysToObservers map
        private const int MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS = 5 * 60;

        // Queue of events to be handled by the main loops
        readonly AsyncQueue<CollectionItemBrokerEvent> brokerEventsQueue = new();

        // Mapping of RespServerSession ID (ObjectStoreSessionID) to observer instance
        readonly ConcurrentDictionary<int, CollectionItemObserver> sessionIdToObserver = new();

        // Mapping of observed keys to queue of observers, by order of subscription. Created by the main loop on
        // first use and mutated only by it; network threads only look keys up. A queue is published only once it
        // holds an observer.
        ConcurrentDictionary<byte[], ConcurrentQueue<CollectionItemObserver>> keysToObservers;

        // Last time keysToObservers was cleaned (in ticks)
        long keysToObserversTimeLastClean = DateTime.Now.Ticks;

        // Minimum amount of time between cleanings of keysToObservers (in ticks)
        readonly long keysToObserversTimeBetweenCleans = TimeSpan.FromSeconds(MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS).Ticks;

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

        readonly StoreWrapper storeWrapper;

        /// <summary>
        /// Storage sessions the main loop runs its attempts on, one per database it has served.
        /// </summary>
        /// <remarks>
        /// The loop cannot borrow the waiting session's. A storage session and its transaction manager are
        /// single-threaded state belonging to the network thread that owns them, and an attempt runs a
        /// transaction: it drives that session's Tsavorite contexts, takes its locks, and leaves its
        /// transaction state machine behind. A blocking command registers its observer before its handler
        /// returns, so the loop can be inside an attempt while that network thread is still unwinding the
        /// batch it parked in, trimming the transaction manager the attempt is using. Created lazily and
        /// reached only from the main loop, so neither the map nor the sessions it holds need a lock.
        /// </remarks>
        readonly Dictionary<int, StorageSession> loopStorageSessions = [];

        /// <summary>
        /// Constructor for CollectionItemBroker
        /// </summary>
        /// <param name="storeWrapper">The store the main loop runs its attempts against</param>
        public CollectionItemBroker(StoreWrapper storeWrapper) => this.storeWrapper = storeWrapper;

        /// <summary>
        /// The main loop's storage session for one database, created on first use.
        /// </summary>
        StorageSession LoopStorageSession(int dbId)
        {
            if (loopStorageSessions.TryGetValue(dbId, out var session))
                return session;

            var dbFound = storeWrapper.TryGetDatabase(dbId, out var db);
            Debug.Assert(dbFound, "A blocking command waited on a database the store does not have");

            var scratchBufferAllocator = new ScratchBufferAllocator();
            session = new StorageSession(storeWrapper, new ScratchBufferBuilder(), scratchBufferAllocator, sessionMetrics: null,
                    LatencyMetrics: null, dbId, readSessionState: null, db.VectorManager, storeWrapper.logger);

            // An attempt wraps its reads and writes in an internal transaction, so the session needs a
            // transaction manager of its own. The manager is given no RESP session because the attempt locks
            // the keys it was handed rather than deriving them from a parse state, which is the only thing a
            // RESP session is consulted for here.
            session.txnManager = new TransactionManager(storeWrapper, respSession: null,
                    new BasicGarnetApi(session, session.stringBasicContext, session.objectBasicContext, session.unifiedBasicContext),
                    new TransactionalGarnetApi(session, session.stringTransactionalContext, session.objectTransactionalContext, session.unifiedTransactionalContext),
                    session, scratchBufferAllocator, storeWrapper.serverOptions.EnableCluster, logger: storeWrapper.logger, dbId: dbId);

            loopStorageSessions[dbId] = session;
            return session;
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
        internal ValueTask<CollectionItemResult> GetCollectionItemAsync(RespCommand command, byte[][] keys,
            RespServerSession session, double timeoutInSeconds, CollectionItemArgs cmdArgs = default)
            => WaitForResultAsync(new CollectionItemObserver(session, command, cmdArgs), keys, timeoutInSeconds);

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
        internal ValueTask<CollectionItemResult> MoveCollectionItemAsync(RespCommand command, byte[] srcKey,
            RespServerSession session, double timeoutInSeconds, CollectionItemArgs cmdArgs)
            => WaitForResultAsync(new CollectionItemObserver(session, command, cmdArgs), [srcKey], timeoutInSeconds);

        /// <summary>
        /// Makes the single attempt of a blocking command that may not wait, trying <paramref name="keys"/> in
        /// order on the calling session's own storage session.
        /// </summary>
        /// <remarks>
        /// For a command inside a transaction or a script (<see cref="RespServerSession.DenyBlocking"/>). It runs
        /// on the caller's session, so inside a transaction it works under the locks the transaction already
        /// holds, which the main loop's session would wait on. No observer is registered, so nothing is left
        /// behind for a timeout, CLIENT UNBLOCK or the session's disposal to complete.
        /// </remarks>
        /// <param name="command">RESP command</param>
        /// <param name="keys">Keys to try, in order; the source key alone for BLMOVE</param>
        /// <param name="storageSession">The calling session's storage session</param>
        /// <param name="cmdArgs">Additional arguments for command</param>
        /// <param name="waitInPlace">True to wait on the device on this thread, for a caller that cannot suspend</param>
        /// <returns>The item or a type mismatch from the first key that has one, otherwise an empty result</returns>
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        internal async ValueTask<CollectionItemResult> TryGetCollectionItemNowAsync(RespCommand command, byte[][] keys,
            StorageSession storageSession, CollectionItemArgs cmdArgs, bool waitInPlace)
        {
            foreach (var key in keys)
            {
                var result = await TryGetResultAsync(key, storageSession, command, cmdArgs, observer: null, failOnSrcTypeMismatch: true, waitInPlace).ConfigureAwait(false);
                if (result.Found || result.IsTypeMismatch)
                    return result;
            }

            return CollectionItemResult.Empty;
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

        /// <summary>
        /// Registers <paramref name="observer"/> against the keys it observes and waits for it to be completed,
        /// by a result arriving, by CLIENT UNBLOCK, by its session being disposed, or by its timeout expiring.
        /// </summary>
        /// <remarks>
        /// Pools its state machine: this is the only await on the blocking-command path, and the returned
        /// <see cref="ValueTask{TResult}"/> is awaited exactly once by each of its callers.
        /// </remarks>
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        private async ValueTask<CollectionItemResult> WaitForResultAsync(CollectionItemObserver observer,
            byte[][] keys, double timeoutInSeconds)
        {
            // Add the session ID to observer mapping
            sessionIdToObserver.TryAdd(observer.Session.ObjectStoreSessionID, observer);

            // Start the main loop task if it hasn't been started yet
            StartMainLoop();

            // Begin the wait before the observer is published, so that a result or a timeout arriving
            // immediately completes this wait rather than racing its creation.
            var wait = observer.WaitAsync(timeoutInSeconds);

            // Add a new observer event to the event queue
            brokerEventsQueue.Enqueue(CollectionItemBrokerEvent.CreateNewObserverEvent(observer, keys));

            var result = await wait.ConfigureAwait(false);

            sessionIdToObserver.TryRemove(observer.Session.ObjectStoreSessionID, out _);

            return result;
        }

        /// <summary>
        /// Notify broker that an item was added to a collection object in specified key
        /// </summary>
        /// <param name="key">Key of the updated collection object</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void HandleCollectionUpdate(byte[] key)
        {
            var map = Volatile.Read(ref keysToObservers);
            if (map is null)
                return;
            HandleCollectionUpdateWorker(map, key);
        }

        void HandleCollectionUpdateWorker(ConcurrentDictionary<byte[], ConcurrentQueue<CollectionItemObserver>> map, byte[] key)
        {
            if (map.TryGetValue(key, out var observers) && !observers.IsEmpty)
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
        private ValueTask HandleBrokerEventAsync(in CollectionItemBrokerEvent brokerEvent)
            => brokerEvent.EventType switch
            {
                CollectionItemBrokerEventType.NewObserver => InitializeObserverAsync(brokerEvent.Observer, brokerEvent.Keys),
                CollectionItemBrokerEventType.CollectionUpdated => TryAssignItemFromKeyAsync(brokerEvent.Key),
                _ => default,
            };

        /// <summary>
        /// Registers a new observer against every key it observes, then tries to serve it from those keys, in order,
        /// where it is first in line.
        /// </summary>
        /// <param name="observer">The new observer instance</param>
        /// <param name="keys">Keys observed by the new observer</param>
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        private async ValueTask InitializeObserverAsync(CollectionItemObserver observer, byte[][] keys)
        {
            var map = keysToObservers;
            if (map is null)
            {
                map = new ConcurrentDictionary<byte[], ConcurrentQueue<CollectionItemObserver>>(ByteArrayComparer.Instance);
                Volatile.Write(ref keysToObservers, map);
            }

            // Register before attempting, so that an update to any of the keys from here on raises an event.
            foreach (var key in keys)
            {
                if (map.TryGetValue(key, out var observers))
                {
                    observers.Enqueue(observer);
                }
                else
                {
                    observers = new ConcurrentQueue<CollectionItemObserver>();
                    observers.Enqueue(observer);
                    map[key] = observers;
                }
            }

            foreach (var key in keys)
            {
                // A key whose queue holds a waiting observer ahead of this one has no item to serve it
                if (!TryPeekWaitingHead(map, key, out var head) || head != observer)
                    continue;

                if (!await AttemptForObserverAsync(observer, key, failOnSrcTypeMismatch: true).ConfigureAwait(false))
                    break;
            }

            // Leaves the keys whose queue no longer holds a waiting observer
            foreach (var key in keys)
                _ = TryPeekWaitingHead(map, key, out _);
        }

        /// <summary>
        /// Try to assign the items available (if any) in the collection at the given key to the observers waiting on
        /// it, in queue order, until either no observer remains or the collection can no longer serve one.
        /// </summary>
        /// <param name="key">Key</param>
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
        private async ValueTask TryAssignItemFromKeyAsync(byte[] key)
        {
            var map = keysToObservers;
            if (map is null)
                return;

            // Keep serving: one update event can make several items available (e.g. LPUSH with multiple elements),
            // and a no-op BLMOVE leaves the item in place for the next observer. Stop at the first observer the
            // collection cannot serve rather than skipping it, since the queue is FIFO.
            while (TryPeekWaitingHead(map, key, out var observer))
            {
                if (await AttemptForObserverAsync(observer, key, failOnSrcTypeMismatch: false).ConfigureAwait(false))
                    break;
            }
        }

        /// <summary>
        /// Drops the observers at the head of <paramref name="key"/>'s queue that are no longer waiting, and the key
        /// itself if none remain.
        /// </summary>
        /// <returns>True if an observer is waiting, with the first one in <paramref name="head"/></returns>
        private static bool TryPeekWaitingHead(ConcurrentDictionary<byte[], ConcurrentQueue<CollectionItemObserver>> map,
            byte[] key, out CollectionItemObserver head)
        {
            if (!map.TryGetValue(key, out var observers))
            {
                head = null;
                return false;
            }

            while (observers.TryPeek(out head))
            {
                if (!head.IsCompleted)
                    return true;

                _ = observers.TryDequeue(out _);
            }

            // Only the main loop adds to a queue, so an empty one stays empty
            _ = map.TryRemove(key, out _);
            return false;
        }

        /// <summary>
        /// Attempts to serve <paramref name="observer"/> from <paramref name="key"/>, holding it claimed for the
        /// attempt. The pop commits the observer to the items it takes, unless a completion arriving during the
        /// claim got there first, in which case the attempt takes nothing.
        /// </summary>
        /// <returns>True if the observer is still waiting</returns>
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        private async ValueTask<bool> AttemptForObserverAsync(CollectionItemObserver observer, byte[] key, bool failOnSrcTypeMismatch)
        {
            if (!observer.TryClaim())
                return false;

            var storageSession = LoopStorageSession(observer.DatabaseId);
            CollectionItemResult result = default;
            bool stillWaiting;
            try
            {
                result = await TryGetResultAsync(key, storageSession, observer.Command, observer.CommandArgs, observer,
                    failOnSrcTypeMismatch, waitInPlace: false).ConfigureAwait(false);
            }
            finally
            {
                // The result holds its own copies of what the attempt read into the scratch buffers
                storageSession.scratchBufferAllocator.Reset();
                stillWaiting = observer.ReleaseClaim(result);
            }

            return stillWaiting;
        }

        /// <summary>
        /// Completes the one object-store operation a context has pending, on this thread if
        /// <paramref name="waitInPlace"/> is set and otherwise without holding a thread on the device.
        /// </summary>
        private static ValueTask<CompletedOutputIterator<ObjectInput, ObjectOutput, long>> DrainAsync<TObjectContext>(StorageSession storageSession,
            ref TObjectContext context, bool waitInPlace)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            if (!waitInPlace)
                return storageSession.Object_CompletePendingAsync(ref context);

            _ = storageSession.Object_CompletePending(out var completed, wait: true, ref context);
            return new ValueTask<CompletedOutputIterator<ObjectInput, ObjectOutput, long>>(completed);
        }

        /// <summary>
        /// Takes the status and output of the one operation <paramref name="completed"/> carries, and disposes it.
        /// </summary>
        private static GarnetStatus TakeOutput(CompletedOutputIterator<ObjectInput, ObjectOutput, long> completed, out ObjectOutput output)
        {
            using (completed)
            {
                var more = completed.Next();
                Debug.Assert(more, "A pending object-store operation completed without producing an output");

                var status = completed.Current.Status;
                output = completed.Current.Output;

                Debug.Assert(!completed.Next(), "A pending object-store operation produced more than one output");
                return StorageSession.InterpretObjectStoreStatus(status, ref output);
            }
        }

        /// <summary>
        /// Parses a score emitted by the sorted set pop output.
        /// The members have already been popped, so a parse failure must still deliver the member to the blocked
        /// client rather than discard it. Scores are written by <see cref="RespWriteUtils.TryWriteDoubleBulkString"/>
        /// using the shortest round-trippable form and NaN is rejected at insert time, so this fallback is not
        /// expected to be reachable.
        /// </summary>
        private static double ParseScoreOrDefault(PinnedSpanByte score)
        {
            if (ParseUtils.TryReadDouble(score, out var parsed, canBeInfinite: true))
                return parsed;

            Debug.Assert(false, "Sorted set score emitted by the object store failed to round-trip");
            return default;
        }

        /// <summary>
        /// Takes the next item(s) <paramref name="command"/> would take from the collection at <paramref name="key"/>,
        /// on <paramref name="storageSession"/>, within its transaction if one is running.
        /// </summary>
        /// <param name="key">Key of the collection</param>
        /// <param name="storageSession">Storage session to run on</param>
        /// <param name="command">RESP command</param>
        /// <param name="cmdArgs">Additional arguments for command</param>
        /// <param name="observer">The claimed observer the items are taken for, which the take commits; null for a caller that cannot wait</param>
        /// <param name="failOnSrcTypeMismatch">True to report a source of the wrong type, false to treat it as empty</param>
        /// <param name="waitInPlace">True to wait on the device on this thread</param>
        /// <returns>The item(s) taken, a type mismatch, or the default result if nothing was taken</returns>
        private ValueTask<CollectionItemResult> TryGetResultAsync(byte[] key, StorageSession storageSession, RespCommand command,
            CollectionItemArgs cmdArgs, CollectionItemObserver observer, bool failOnSrcTypeMismatch, bool waitInPlace)
        {
            if (command == RespCommand.BLMOVE)
                return TryMoveAsync(key, storageSession, cmdArgs, observer, failOnSrcTypeMismatch, waitInPlace);

            var txnManager = storageSession.txnManager;
            return txnManager.state == TxnState.Running
                ? TryPopAsync(key, storageSession, txnManager.ObjectTransactionalContext, command, cmdArgs, observer, failOnSrcTypeMismatch, waitInPlace)
                : TryPopAsync(key, storageSession, storageSession.objectBasicContext, command, cmdArgs, observer, failOnSrcTypeMismatch, waitInPlace);
        }

        /// <summary>
        /// Pops the next item(s) a single-key blocking command takes from the collection at <paramref name="key"/>.
        /// </summary>
        /// <remarks>
        /// The pop is one RMW, which the store serializes with every other update of the key, so it locks nothing
        /// and holds nothing while it waits on the device. A missing key, a collection of another type and an
        /// empty collection all leave the pop with nothing to take.
        /// </remarks>
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        private static async ValueTask<CollectionItemResult> TryPopAsync<TObjectContext>(byte[] key, StorageSession storageSession,
            TObjectContext context, RespCommand command, CollectionItemArgs cmdArgs, CollectionItemObserver observer,
            bool failOnSrcTypeMismatch, bool waitInPlace)
            where TObjectContext : ITsavoriteContext<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator>
        {
            var asKey = BlockingKeys.AsSpanByte(key);
            var popOutput = new ObjectOutput();
            GarnetStatus status;
            bool pending;

            var isSortedSet = command is RespCommand.BZPOPMIN or RespCommand.BZPOPMAX or RespCommand.BZMPOP;

            // Set across a device wait too, since the pop then runs when the read completes
            storageSession.functionsState.itemRecipient = observer;
            try
            {
                if (isSortedSet)
                {
                    var lowScoresFirst = command == RespCommand.BZMPOP ? cmdArgs.LowScoresFirst : command == RespCommand.BZPOPMIN;
                    var count = command == RespCommand.BZMPOP ? cmdArgs.Count : 1;
                    status = storageSession.SortedSetPop_WithPending(asKey, count, lowScoresFirst, ref popOutput, out pending, ref context);
                }
                else
                {
                    Debug.Assert(command is RespCommand.BLPOP or RespCommand.BRPOP or RespCommand.BLMPOP);
                    var fromLeft = command == RespCommand.BLMPOP ? cmdArgs.SourceDirection == OperationDirection.Left : command == RespCommand.BLPOP;
                    var count = command == RespCommand.BLMPOP ? cmdArgs.Count : 1;
                    status = storageSession.ListPop_WithPending(asKey, count, fromLeft ? ListOperation.LPOP : ListOperation.RPOP,
                        ref popOutput, out pending, ref context);
                }

                if (pending)
                    status = TakeOutput(await DrainAsync(storageSession, ref context, waitInPlace).ConfigureAwait(false), out popOutput);
            }
            finally
            {
                storageSession.functionsState.itemRecipient = null;
            }

            if (status == GarnetStatus.WRONGTYPE)
                return failOnSrcTypeMismatch ? CollectionItemResult.TypeMismatch : default;

            // A pop refused by a completion that arrived first leaves the output unwritten
            if (status != GarnetStatus.OK || popOutput.result1 <= 0)
                return default;

            if (isSortedSet)
            {
                var pairs = storageSession.ProcessRespArrayOutputAsPairs(popOutput, out _);
                if (pairs is null || pairs.Length == 0)
                    return default;

                if (command != RespCommand.BZMPOP)
                    return new CollectionItemResult(key, ParseScoreOrDefault(pairs[0].score), pairs[0].member.ToArray());

                var scores = new double[pairs.Length];
                var members = new byte[pairs.Length][];
                for (var i = 0; i < pairs.Length; i++)
                {
                    scores[i] = ParseScoreOrDefault(pairs[i].score);
                    members[i] = pairs[i].member.ToArray();
                }

                return new CollectionItemResult(key, scores, members);
            }

            // The elements are in the session's scratch buffers until its next reset, so the result copies them
            var elements = storageSession.ProcessRespArrayOutput(popOutput, out _);
            if (elements is null || elements.Length == 0)
                return default;

            if (command != RespCommand.BLMPOP)
                return new CollectionItemResult(key, elements[0].ToArray());

            var items = new byte[elements.Length][];
            for (var i = 0; i < elements.Length; i++)
                items[i] = elements[i].ToArray();

            return new CollectionItemResult(key, items);
        }

        /// <summary>
        /// Moves the next item BLMOVE takes from the list at <paramref name="key"/> to its destination list, inside a
        /// transaction on <paramref name="storageSession"/>: its own if none is running, else the running one.
        /// </summary>
        /// <remarks>
        /// The source and destination are locked together so that the item is never seen in neither or both, and
        /// the destination's type is checked before the pop so that the item is never popped with nowhere to go.
        /// Both lists are read only for their type and length; all mutation is performed through RMW so that
        /// Tsavorite controls whether a record is updated in place or copied into the mutable region.
        /// </remarks>
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        private async ValueTask<CollectionItemResult> TryMoveAsync(byte[] key, StorageSession storageSession,
            CollectionItemArgs cmdArgs, CollectionItemObserver observer, bool failOnSrcTypeMismatch, bool waitInPlace)
        {
            var asKey = BlockingKeys.AsSpanByte(key);
            var dstKey = BlockingKeys.AsSpanByte(cmdArgs.DestinationKey);

            var txnManager = storageSession.txnManager;
            var createTransaction = false;

            // Create a transaction if not currently in a running transaction
            if (txnManager.state != TxnState.Running)
            {
                Debug.Assert(txnManager.state == TxnState.None);
                createTransaction = true;
                txnManager.AddTransactionStoreTypes(TransactionStoreTypes.Object | TransactionStoreTypes.Unified);
                txnManager.SaveKeyEntryToLock(asKey, LockType.Exclusive);
                txnManager.SaveKeyEntryToLock(dstKey, LockType.Exclusive);
                _ = txnManager.Run(true);
            }

            // Set once the push succeeds, so that observers of the destination are woken after the commit
            var notifyDestination = false;

            try
            {
                var context = txnManager.ObjectTransactionalContext;

                // Get the object stored at key
                ObjectInput readInput = default;
                ObjectOutput srcOutput = default;
                var status = storageSession.Read_ObjectStore_WithPending(key, ref readInput, ref srcOutput, out var pending, ref context);
                if (pending)
                    status = TakeOutput(await DrainAsync(storageSession, ref context, waitInPlace).ConfigureAwait(false), out srcOutput);

                if (status == GarnetStatus.NOTFOUND)
                    return default;

                if (status == GarnetStatus.WRONGTYPE || srcOutput.GarnetObject is not ListObject srcList)
                    return failOnSrcTypeMismatch ? CollectionItemResult.TypeMismatch : default;

                ObjectOutput dstOutput = default;
                status = storageSession.Read_ObjectStore_WithPending(cmdArgs.DestinationKey, ref readInput, ref dstOutput, out pending, ref context);
                if (pending)
                    status = TakeOutput(await DrainAsync(storageSession, ref context, waitInPlace).ConfigureAwait(false), out dstOutput);

                // A destination of another type is always reported, whatever the source
                if (status != GarnetStatus.NOTFOUND && (status == GarnetStatus.WRONGTYPE || dstOutput.GarnetObject is not ListObject))
                    return CollectionItemResult.TypeMismatch;

                var currCount = srcList.LnkList.Count;
                if (currCount == 0)
                    return default;

                var srcDirection = cmdArgs.SourceDirection;
                var dstDirection = cmdArgs.DestinationDirection;

                if (srcDirection == OperationDirection.Unknown || dstDirection == OperationDirection.Unknown)
                    return default;

                // Moving between the same ends of one list, or rotating a single-element list, leaves the list
                // unchanged. Returning without the pop/push also avoids emptying the key, which would drop its TTL.
                // The keys are locked, so committing the observer here is as final as a pop that commits it.
                if (key.AsSpan().SequenceEqual(cmdArgs.DestinationKey) && (srcDirection == dstDirection || currCount == 1))
                {
                    if (observer is not null && !observer.TryCommit())
                        return default;

                    var unchanged = srcDirection == OperationDirection.Right ? srcList.LnkList.Last.Value : srcList.LnkList.First.Value;
                    return new CollectionItemResult(key, unchanged.AsSpan().ToArray());
                }

                var popOutput = new ObjectOutput();
                storageSession.functionsState.itemRecipient = observer;
                try
                {
                    status = storageSession.ListPop_WithPending(asKey, 1,
                        srcDirection == OperationDirection.Left ? ListOperation.LPOP : ListOperation.RPOP, ref popOutput, out pending, ref context);
                    if (pending)
                        status = TakeOutput(await DrainAsync(storageSession, ref context, waitInPlace).ConfigureAwait(false), out popOutput);
                }
                finally
                {
                    storageSession.functionsState.itemRecipient = null;
                }

                // A pop refused by a completion that arrived first leaves the output unwritten
                if (status != GarnetStatus.OK || popOutput.result1 <= 0)
                    return default;

                // The element is in the session's scratch buffers until its next reset, so the result copies it
                var elements = storageSession.ProcessRespArrayOutput(popOutput, out _);
                if (elements is null || elements.Length == 0)
                    return default;

                var pushOutput = new ObjectOutput();
                status = storageSession.ListPush_WithPending(dstKey, elements[0],
                    dstDirection == OperationDirection.Left ? ListOperation.LPUSH : ListOperation.RPUSH,
                    ref pushOutput, out pending, ref context);
                if (pending)
                    status = TakeOutput(await DrainAsync(storageSession, ref context, waitInPlace).ConfigureAwait(false), out pushOutput);

                // The destination type was validated above and the push creates the destination if it is absent,
                // so the push cannot fail. If it ever did, the element has already been popped and the transaction
                // is committed unconditionally, so deliver it to the blocked client rather than discard it.
                Debug.Assert(status == GarnetStatus.OK, "List push to the BLMOVE destination failed after the source pop");
                notifyDestination = status == GarnetStatus.OK;

                return new CollectionItemResult(key, elements[0].ToArray());
            }
            finally
            {
                if (createTransaction)
                    txnManager.Commit(true);

                // Wake observers blocked on the destination key, now that its lock is released
                if (notifyDestination)
                    HandleCollectionUpdate(cmdArgs.DestinationKey);
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
                        // Park until an event arrives. The wait is uncancellable so that the queue serves it
                        // from its single-waiter gate rather than from a semaphore waiter plus a token
                        // registration; Dispose wakes this loop with a sentinel event instead.
                        await brokerEventsQueue.WaitForEntryAsync().ConfigureAwait(false);
                        continue;
                    }

                    await HandleBrokerEventAsync(nextEvent).ConfigureAwait(false);

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
            var map = keysToObservers;
            if (map is not null)
            {
                foreach (var kvp in map)
                    _ = TryPeekWaitingHead(map, kvp.Key, out _);
            }

            // Update last cleaned time
            keysToObserversTimeLastClean = DateTime.Now.Ticks;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            cts.Cancel();

            // Wake the main loop, which parks on an uncancellable wait for the next event.
            brokerEventsQueue.Enqueue(default);

            foreach (var observer in sessionIdToObserver.Values)
                observer.HandleSessionDisposed();

            var prevMainLoopStatus = Interlocked.Exchange(ref mainLoopTaskStatus, MAIN_LOOP_DISPOSED);

            if (prevMainLoopStatus == MAIN_LOOP_STARTED)
            {
                done.Wait();
            }

            // The main loop has finished, so its storage sessions are no longer in use.
            foreach (var session in loopStorageSessions.Values)
                session.Dispose();
            loopStorageSessions.Clear();

            done.Dispose();
            cts.Dispose();
        }
    }
}