// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Broker used for pub/sub
    /// </summary>
    /// <remarks>
    /// <para>
    /// A message published on this node is delivered by the publisher itself (<see cref="PublishNoWait"/>). One
    /// forwarded from another cluster node is appended to a log (<see cref="PublishAsync"/>), which a background
    /// consumer delivers from in order.
    /// </para>
    /// <para>
    /// Neither waits on a thread for a subscriber whose connection is full. The consumer delivers to everyone else,
    /// then waits on that connection without a thread, and moves to the next message only once it has delivered
    /// this one. The log keeps only its last <see cref="MemoryPages"/> pages in memory and drops older ones from
    /// under a consumer that lags further, so a producer that would take the log that far ahead of the consumer
    /// waits instead, without a thread, until the consumer catches up.
    /// </para>
    /// </remarks>
    public sealed class SubscribeBroker : IDisposable, ILogEntryConsumer
    {
        /// <summary>The number of pages the log keeps in memory.</summary>
        const int MemoryPages = 4;

        /// <summary>
        /// The most pages the tail may be ahead of the consumer when a producer appends. An append moves the tail
        /// onto at most one new page, and turning onto a page drops the page <see cref="MemoryPages"/> behind it, so
        /// the page the consumer reads next stays in memory.
        /// </summary>
        const int MaxPagesAhead = MemoryPages - 2;

        /// <summary>
        /// Stands in the log for <see cref="oversizedEntry"/>, where an entry starts with the length of its channel.
        /// </summary>
        const int OversizedMarker = -1;

        int sid = 0;
        bool initialized = false;
        ConcurrentDictionary<ByteArrayWrapper, ReadOptimizedConcurrentSet<ServerSessionBase>> subscriptions;
        ReadOptimizedConcurrentSet<PatternSubscriptionEntry> patternSubscriptions;
        readonly TsavoriteLog aof;
        readonly IDevice device;
        readonly CancellationTokenSource cts = new();
        readonly ManualResetEvent done = new(true);
        long previousAddress = 0;
        volatile bool disposed = false;
        readonly int pageSizeBits;
        readonly ILogger logger;

        /// <summary>Serializes producers, so that each sees the tail the one before it left.</summary>
        readonly object enqueueLock = new();

        /// <summary>The address the consumer reads next.</summary>
        long consumedUntil;

        /// <summary>Completed when the consumer makes room for a producer that found none.</summary>
        TaskCompletionSource roomFreed;

        /// <summary>
        /// A message too large for a page of the log, laid out as a log entry is, which the consumer takes when it
        /// reaches the <see cref="OversizedMarker"/> that holds its place in the log. One at a time.
        /// </summary>
        byte[] oversizedEntry;

        /// <summary>The deliveries of the message being consumed that a full connection held back.</summary>
        DeferredDeliveries held;

        /// <summary>The message whose deliveries are held back, laid out as a log entry is.</summary>
        byte[] heldEntry;

        /// <summary>Where a message read from the log is copied when its deliveries are held back.</summary>
        byte[] heldCopy;

        /// <summary>What the consumer waits on for room on a subscriber's connection.</summary>
        readonly SendCapacityWait capacityWait = new();

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="logDir">Directory where the log will be stored</param>
        /// <param name="pageSize">Page size of log used for pub/sub</param>
        /// <param name="startFresh">start the log from scratch, do not continue</param>
        public SubscribeBroker(string logDir, long pageSize, LightEpoch epoch, bool startFresh = true, ILogger logger = null)
        {
            device = logDir == null ? new NullDevice() : Devices.CreateLogDevice(Path.Combine(logDir, "pubsubkv"), preallocateFile: false);
            device.Initialize((long)(1 << 30) * 64);
            aof = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSize = pageSize, MemorySize = pageSize * MemoryPages, Epoch = epoch });
            pageSizeBits = aof.UnsafeGetLogPageSizeBits();
            if (startFresh)
                aof.TruncateUntil(aof.CommittedUntilAddress);
            consumedUntil = aof.BeginAddress;
            this.logger = logger;
        }

        /// <summary>
        /// Remove all subscriptions for a session,
        /// called during dispose of server session
        /// </summary>
        /// <param name="session">server session</param>
        public unsafe void RemoveSubscription(IMessageConsumer session)
        {
            if (subscriptions != null)
            {
                foreach (var subscribedKey in subscriptions.Keys)
                {
                    this.Unsubscribe(subscribedKey, (ServerSessionBase)session);
                }
            }

            if (patternSubscriptions != null)
            {
                int index = 0;
                while (patternSubscriptions.Iterate(ref index, out var entry))
                {
                    entry.subscriptions.TryRemove((ServerSessionBase)session);
                }
            }
        }

        /// <summary>
        /// Delivers to every subscriber, without waiting for a subscriber's connection to have room. A subscriber
        /// whose connection has none has its delivery recorded in <paramref name="deferred"/>, for the caller to
        /// complete, and every later delivery to that subscriber is recorded behind it, so the subscriber still
        /// receives them in the order they were made.
        /// </summary>
        /// <returns>The number of subscribers reached, counting those whose delivery was deferred.</returns>
        unsafe int Broadcast(PinnedSpanByte key, PinnedSpanByte value, ref DeferredDeliveries deferred)
        {
            var numSubscribers = 0;

            if (subscriptions != null)
            {
                if (subscriptions.TryGetValue(new ByteArrayWrapper(key), out var sessions))
                {
                    var index = 0;
                    while (sessions.Iterate(ref index, out var session))
                    {
                        if (!session.TryPublish(key, value))
                            (deferred ??= new()).Add(new(session));
                        numSubscribers++;
                    }
                }
            }

            if (patternSubscriptions != null)
            {
                var index1 = 0;
                while (patternSubscriptions.Iterate(ref index1, out var entry))
                {
                    var pattern = entry.pattern;
                    fixed (byte* patternPtr = pattern.ReadOnlySpan)
                    {
                        var patternSlice = PinnedSpanByte.FromPinnedPointer(patternPtr, pattern.ReadOnlySpan.Length);
                        if (Match(key, patternSlice))
                        {
                            var sessions = entry.subscriptions;
                            var index2 = 0;
                            while (sessions.Iterate(ref index2, out var session))
                            {
                                if ((deferred is not null && deferred.Holds(session)) || !session.TryPatternPublish(patternSlice, key, value))
                                    (deferred ??= new()).Add(new(session, pattern));
                                numSubscribers++;
                            }
                        }
                    }
                }
            }
            return numSubscribers;
        }

        /// <summary>
        /// Delivers a message laid out as a log entry is: the channel and then the message, each after its length.
        /// </summary>
        unsafe void Broadcast(byte* entry)
        {
            var key = PinnedSpanByte.FromPinnedPointer(entry + sizeof(int), *(int*)entry);
            entry += sizeof(int) + key.Length;
            var value = PinnedSpanByte.FromPinnedPointer(entry + sizeof(int), *(int*)entry);
            _ = Broadcast(key, value, ref held);
        }

        async Task StartAsync()
        {
            try
            {
                using var iterator = aof.ScanSingle(aof.BeginAddress, long.MaxValue, scanUncommitted: true);
                var signal = iterator.Signal;
                using var registration = cts.Token.Register(signal);

                while (!disposed && !cts.IsCancellationRequested)
                {
                    if (!iterator.TryConsumeNext(this))
                    {
                        if (!await iterator.WaitAsync(cts.Token).ConfigureAwait(false))
                            break;
                        continue;
                    }

                    if (held is not { Count: > 0 })
                        continue;

                    // Out here, where the log's epoch is not held, the consumer waits for each connection that had
                    // no room, in the order the deliveries were held back. A completion that frees room can precede
                    // the registration, so the connection is tried again after each wait rather than trusting the
                    // signal alone.
                    for (var i = 0; i < held.Count; i++)
                    {
                        while (!TryDeliverHeld(i))
                        {
                            if (capacityWait.TryRegisterWithPeer(held[i].Session.PublishSender))
                                await capacityWait.WaitAsync().ConfigureAwait(false);
                        }
                    }

                    // Lets go of the subscribers' sessions and the message
                    held.Clear();
                    heldEntry = null;
                }
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
            }
            finally
            {
                done.Set();
            }
        }

        /// <summary>
        /// Makes one of the deliveries held back, if the subscriber's connection has room for it now.
        /// </summary>
        /// <param name="index">Which delivery, in the order they were held back.</param>
        /// <returns>False if the connection is full and nothing was written.</returns>
        unsafe bool TryDeliverHeld(int index)
        {
            var delivery = held[index];
            fixed (byte* entry = heldEntry)
            {
                var key = PinnedSpanByte.FromPinnedPointer(entry + sizeof(int), *(int*)entry);
                var valuePtr = entry + sizeof(int) + key.Length;
                var value = PinnedSpanByte.FromPinnedPointer(valuePtr + sizeof(int), *(int*)valuePtr);

                if (!delivery.IsPattern)
                    return delivery.Session.TryPublish(key, value);

                // Patterns are stored unpinned
                var pattern = delivery.Pattern.ReadOnlySpan;
                fixed (byte* patternPtr = pattern)
                    return delivery.Session.TryPatternPublish(PinnedSpanByte.FromPinnedPointer(patternPtr, pattern.Length), key, value);
            }
        }

        /// <summary>
        /// Delivers one entry of the log. This runs under the log's epoch, which holds back the log's progress for
        /// every producer, so it does not wait: a delivery a full connection holds back is made by
        /// <see cref="StartAsync"/>, from a copy of the message.
        /// </summary>
        public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
        {
            try
            {
                if (previousAddress > 0 && currentAddress > previousAddress)
                {
                    if (
                        (currentAddress % (1 << pageSizeBits) != 0) || // the skip was to a non-page-boundary
                        (currentAddress >= previousAddress + payloadLength) // we skipped beyond the expected next address
                    )
                    {
                        logger?.LogWarning("SubscribeBroker: Skipping from {previousAddress} to {currentAddress}", previousAddress, currentAddress);
                    }
                }

                Debug.Assert(held is null or { Count: 0 }, "An entry was consumed before the deliveries held back by the last were made");
                var freesRoom = (nextAddress >> pageSizeBits) > (consumedUntil >> pageSizeBits);
                if (*(int*)payloadPtr == OversizedMarker)
                {
                    var entry = Interlocked.Exchange(ref oversizedEntry, null);
                    Debug.Assert(entry is not null, "The log holds the place of a message that is not there");
                    fixed (byte* entryPtr = entry)
                        Broadcast(entryPtr);
                    if (held is { Count: > 0 })
                        heldEntry = entry;
                    freesRoom = true;
                }
                else
                {
                    Broadcast(payloadPtr);
                    if (held is { Count: > 0 })
                    {
                        // The entry fits on a page of the log, which is reused once the consumer moves on
                        heldCopy ??= new byte[1 << pageSizeBits];
                        new ReadOnlySpan<byte>(payloadPtr, payloadLength).CopyTo(heldCopy);
                        heldEntry = heldCopy;
                    }
                }

                if (nextAddress > aof.BeginAddress)
                    aof.TruncateUntil(nextAddress);
                previousAddress = nextAddress;
                Volatile.Write(ref consumedUntil, nextAddress);

                if (freesRoom)
                {
                    // Pairs with the barrier in WatchForRoom: a producer publishes its wait and then looks for room, and
                    // this makes room and then looks for a wait, so one of the two sees the other.
                    Interlocked.MemoryBarrier();
                    if (Volatile.Read(ref roomFreed) is not null)
                        Interlocked.Exchange(ref roomFreed, null)?.TrySetResult();
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at SubscribeBroker.Consume");
                throw;
            }
        }

        void Initialize()
        {
            done.Reset();
            subscriptions = new ConcurrentDictionary<ByteArrayWrapper, ReadOptimizedConcurrentSet<ServerSessionBase>>(ByteArrayWrapperComparer.Instance);
            patternSubscriptions = new ReadOptimizedConcurrentSet<PatternSubscriptionEntry>();
            _ = Task.Run(StartAsync);
            initialized = true;
        }

        /// <summary>
        /// Subscribe to a particular Key
        /// </summary>
        /// <param name="key">Key to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe bool Subscribe(PinnedSpanByte key, ServerSessionBase session)
        {
            if (!initialized && Interlocked.Increment(ref sid) == 1)
                Initialize();
            else
                while (!initialized) Thread.Yield();

            var subscriptionKey = ByteArrayWrapper.CopyFrom(key.ReadOnlySpan, false);
            if (!subscriptions.ContainsKey(subscriptionKey))
                subscriptions.TryAdd(subscriptionKey, new ReadOptimizedConcurrentSet<ServerSessionBase>());

            return subscriptions[subscriptionKey].TryAdd(session);
        }

        /// <summary>
        /// Subscribe to a particular pattern
        /// </summary>
        /// <param name="pattern">Pattern to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe bool PatternSubscribe(PinnedSpanByte pattern, ServerSessionBase session)
        {
            if (!initialized && Interlocked.Increment(ref sid) == 1)
                Initialize();
            else
                while (!initialized) Thread.Yield();

            var subscriptionPattern = ByteArrayWrapper.CopyFrom(pattern.ReadOnlySpan, false);
            patternSubscriptions.TryAddAndGet(new PatternSubscriptionEntry { pattern = subscriptionPattern, subscriptions = new ReadOptimizedConcurrentSet<ServerSessionBase>() }, out var addedEntry);
            return addedEntry.subscriptions.TryAdd(session);
        }

        /// <summary>
        /// Unsubscribe to a particular key
        /// </summary>
        /// <param name="key">Key to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe bool Unsubscribe(ByteArrayWrapper key, ServerSessionBase session)
        {
            if (subscriptions == null) return false;
            if (subscriptions.TryGetValue(key, out var sessions))
            {
                return sessions.TryRemove(session);
            }
            return false;
        }

        /// <summary>
        /// Unsubscribe to a particular pattern
        /// </summary>
        /// <param name="key">Pattern to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe bool PatternUnsubscribe(ByteArrayWrapper key, ServerSessionBase session)
        {
            if (patternSubscriptions == null) return false;
            int index = 0;
            while (patternSubscriptions.Iterate(ref index, out var entry))
            {
                if (entry.pattern.ReadOnlySpan.SequenceEqual(key.ReadOnlySpan))
                {
                    return entry.subscriptions.TryRemove(session);
                }
            }
            return false;
        }

        /// <summary>
        /// List all subscriptions made by a session
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public unsafe List<ByteArrayWrapper> ListAllSubscriptions(ServerSessionBase session)
        {
            List<ByteArrayWrapper> sessionSubscriptions = new();
            if (subscriptions != null)
            {
                foreach (var subscription in subscriptions)
                {
                    if (subscription.Value.Count > 0)
                        sessionSubscriptions.Add(subscription.Key);
                }
            }
            return sessionSubscriptions;
        }

        /// <summary>
        /// List all pattern subscriptions made by a session
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public unsafe List<ByteArrayWrapper> ListAllPatternSubscriptions(ServerSessionBase session)
        {
            List<ByteArrayWrapper> sessionPatternSubscriptions = new();
            if (patternSubscriptions == null) return sessionPatternSubscriptions;
            var index = 0;
            while (patternSubscriptions.Iterate(ref index, out var entry))
            {
                if (entry.subscriptions.Count > 0)
                    sessionPatternSubscriptions.Add(entry.pattern);
            }
            return sessionPatternSubscriptions;
        }

        /// <summary>
        /// Publish the update made to key to all the subscribers, without waiting for any subscriber's
        /// connection to have room. A subscriber whose connection has none is left in
        /// <paramref name="deferred"/>, for the publisher to deliver to once the connection does.
        /// </summary>
        /// <param name="key">key that has been updated</param>
        /// <param name="value">value that has been updated</param>
        /// <param name="deferred">
        /// The deliveries held back, which must be empty or not yet created. It is created when the first delivery
        /// is held back, so a caller that publishes repeatedly can pass the same one each time, once it has
        /// delivered what it held back and cleared it.
        /// </param>
        /// <returns>Number of subscribers the update reaches, counting those it is held back for</returns>
        internal unsafe int PublishNoWait(PinnedSpanByte key, PinnedSpanByte value, ref DeferredDeliveries deferred)
        {
            Debug.Assert(deferred is null or { Count: 0 }, "A publish started before the deliveries held back by the last were made");
            if (subscriptions == null && patternSubscriptions == null) return 0;
            return Broadcast(key, value, ref deferred);
        }

        /// <summary>
        /// Publish the update made to key to all the subscribers, through the log the broker delivers from in order.
        /// </summary>
        /// <param name="key">key that has been updated</param>
        /// <param name="value">value that has been updated</param>
        /// <returns>
        /// A task that completes once the update is in the log. It is already complete unless the log is as far
        /// ahead of the delivery as it may be, in which case it completes when the delivery catches up.
        /// </returns>
        /// <remarks>
        /// The key and value are copied before the call returns an incomplete task, so they need not outlive the
        /// call.
        /// </remarks>
        public ValueTask PublishAsync(PinnedSpanByte key, PinnedSpanByte value)
        {
            if (subscriptions == null && patternSubscriptions == null)
                return default;

            if (aof.EntryFitsOnPage(key.Length + value.Length + (2 * sizeof(int))))
            {
                return TryEnqueue(key.ReadOnlySpan, value.ReadOnlySpan)
                    ? default
                    : new(EnqueueWhenRoomAsync(ToEntry(key.ReadOnlySpan, value.ReadOnlySpan), oversized: false));
            }

            // A message too large for a page of the log waits beside it, and a marker holds its place in the log, so
            // it is delivered in order with the rest
            var entry = ToEntry(key.ReadOnlySpan, value.ReadOnlySpan);
            return TryEnqueueOversized(entry) ? default : new(EnqueueWhenRoomAsync(entry, oversized: true));
        }

        /// <summary>
        /// Whether a producer may append to the log without dropping the page the consumer reads next. Called
        /// under <see cref="enqueueLock"/>.
        /// </summary>
        bool HasRoom()
            => (aof.TailAddress >> pageSizeBits) - (Volatile.Read(ref consumedUntil) >> pageSizeBits) <= MaxPagesAhead;

        /// <summary>Appends a message to the log, if there is room for it.</summary>
        bool TryEnqueue(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            lock (enqueueLock)
            {
                if (!HasRoom())
                    return false;

                // The append can wait for the log to close a page, which takes only until the threads in the log's
                // epoch move on, and none of them waits while it is there
                aof.Enqueue(key, value, out _);
                return true;
            }
        }

        /// <summary>Appends a message laid out as a log entry is, if there is room for it.</summary>
        bool TryEnqueue(byte[] entry)
        {
            var keyLength = MemoryMarshal.Read<int>(entry);
            var key = new ReadOnlySpan<byte>(entry, sizeof(int), keyLength);
            var value = new ReadOnlySpan<byte>(entry, (2 * sizeof(int)) + keyLength, entry.Length - (2 * sizeof(int)) - keyLength);
            return TryEnqueue(key, value);
        }

        /// <summary>
        /// Places a message too large for a page of the log beside it, and appends a marker that holds its place,
        /// if there is room for both.
        /// </summary>
        bool TryEnqueueOversized(byte[] entry)
        {
            lock (enqueueLock)
            {
                if (Volatile.Read(ref oversizedEntry) is not null || !HasRoom())
                    return false;

                // Placed before the marker, which the consumer may read as soon as it is appended
                Volatile.Write(ref oversizedEntry, entry);
                aof.Enqueue(OversizedMarker, out _);
                return true;
            }
        }

        /// <summary>
        /// Appends a message once the consumer makes room for it, without holding a thread meanwhile.
        /// </summary>
        async Task EnqueueWhenRoomAsync(byte[] entry, bool oversized)
        {
            while (true)
            {
                // Published before looking for room, so the consumer that makes room next cannot miss this producer
                var room = WatchForRoom();
                if (disposed || (oversized ? TryEnqueueOversized(entry) : TryEnqueue(entry)))
                    return;

                await room.Task.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// The promise the consumer completes when it next makes room, or <see cref="Dispose"/> does.
        /// </summary>
        TaskCompletionSource WatchForRoom()
        {
            var room = Volatile.Read(ref roomFreed);
            if (room is null)
            {
                var created = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                room = Interlocked.CompareExchange(ref roomFreed, created, null) ?? created;
            }

            // Pairs with the barrier in Consume and the exchange in Dispose
            Interlocked.MemoryBarrier();
            return room;
        }

        /// <summary>
        /// Lays out a message as a log entry is: the channel and then the message, each after its length.
        /// </summary>
        static byte[] ToEntry(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            var entry = new byte[(2 * sizeof(int)) + key.Length + value.Length];
            MemoryMarshal.Write(entry, key.Length);
            key.CopyTo(entry.AsSpan(sizeof(int)));
            MemoryMarshal.Write(entry.AsSpan(sizeof(int) + key.Length), value.Length);
            value.CopyTo(entry.AsSpan((2 * sizeof(int)) + key.Length));
            return entry;
        }

        /// <summary>
        /// Get the number of channels subscribed to
        /// </summary>
        /// <returns></returns>
        public List<ByteArrayWrapper> GetChannels()
        {
            if (subscriptions is null || subscriptions.IsEmpty)
                return [];

            List<ByteArrayWrapper> channels = [];
            foreach (var entry in subscriptions)
            {
                if (entry.Value.Count > 0)
                    channels.Add(entry.Key);
            }
            return channels;
        }

        /// <summary>
        /// Get the number of channels subscribed to, matching the given pattern
        /// </summary>
        /// <param name="pattern"></param>
        /// <returns></returns>
        public unsafe List<ByteArrayWrapper> GetChannels(PinnedSpanByte pattern)
        {
            if (subscriptions is null || subscriptions.IsEmpty)
                return [];

            List<ByteArrayWrapper> channels = [];
            foreach (var entry in subscriptions)
            {
                if (entry.Value.Count > 0)
                {
                    fixed (byte* keyPtr = entry.Key.ReadOnlySpan)
                    {
                        if (Match(PinnedSpanByte.FromPinnedPointer(keyPtr, entry.Key.ReadOnlySpan.Length), pattern))
                            channels.Add(entry.Key);
                    }
                }
            }
            return channels;
        }

        /// <summary>
        /// Retrieves the number of pattern subscriptions.
        /// </summary>
        /// <returns>The number of pattern subscriptions.</returns>
        public int NumPatternSubscriptions()
        {
            if (patternSubscriptions is null)
                return 0;

            var count = 0;
            var index = 0;
            while (patternSubscriptions.Iterate(ref index, out var item))
            {
                if (item.subscriptions.Count > 0)
                    count++;
            }
            return count;
        }

        /// <summary>
        /// Retrieves the number of subscriptions for a given channel.
        /// </summary>
        /// <param name="channel"></param>
        /// <returns></returns>
        public int NumSubscriptions(PinnedSpanByte channel)
        {
            if (subscriptions is null)
                return 0;
            _ = subscriptions.TryGetValue(new ByteArrayWrapper(channel), out var sessions);
            return sessions?.Count ?? 0;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            // The consumer stops at its next wait: for the log, for a subscriber's connection, or before either.
            // A producer waiting for room stops without appending.
            disposed = true;
            cts.Cancel();
            capacityWait.Cancel();
            Interlocked.Exchange(ref roomFreed, null)?.TrySetResult();
            done.WaitOne();
            subscriptions?.Clear();
            patternSubscriptions?.Clear();
            aof.Dispose();
            device.Dispose();
        }

        unsafe bool Match(PinnedSpanByte key, PinnedSpanByte pattern)
            => GlobUtils.Match(pattern.ToPointer(), pattern.Length, key.ToPointer(), key.Length);
    }
}