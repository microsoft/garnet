﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
    public sealed class SubscribeBroker : IDisposable, IAofEntryConsumer
    {
        int sid = 0;
        bool initialized = false;
        ConcurrentDictionary<ByteArrayWrapper, ReadOptimizedConcurrentSet<ServerSessionBase>> subscriptions;
        ReadOptimizedConcurrentSet<PatternSubscriptionEntry> patternSubscriptions;
        readonly TsavoriteAof aof;
        readonly IDevice device;
        readonly CancellationTokenSource cts = new();
        readonly ManualResetEvent done = new(true);
        long previousAddress = 0;
        bool disposed = false;
        readonly int pageSizeBits;
        readonly ILogger logger;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="logDir">Directory where the log will be stored</param>
        /// <param name="pageSize">Page size of log used for pub/sub</param>
        /// <param name="subscriberRefreshFrequencyMs">Subscriber log refresh frequency</param>
        /// <param name="startFresh">start the log from scratch, do not continue</param>
        public SubscribeBroker(string logDir, long pageSize, int subscriberRefreshFrequencyMs, bool startFresh = true, ILogger logger = null)
        {
            device = logDir == null ? new NullDevice() : Devices.CreateLogDevice(logDir + "/pubsubkv", preallocateFile: false);
            device.Initialize((long)(1 << 30) * 64);
            aof = new TsavoriteAof(new TsavoriteAofLogSettings { LogDevice = device, PageSize = pageSize, MemorySize = pageSize * 4, SafeTailRefreshFrequencyMs = subscriberRefreshFrequencyMs });
            pageSizeBits = aof.UnsafeGetLogPageSizeBits();
            if (startFresh)
                aof.TruncateUntil(aof.CommittedUntilAddress);
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

        unsafe int Broadcast(PinnedSpanByte key, PinnedSpanByte value)
        {
            var numSubscribers = 0;

            if (subscriptions != null)
            {
                if (subscriptions.TryGetValue(new ByteArrayWrapper(key), out var sessions))
                {
                    var index = 0;
                    while (sessions.Iterate(ref index, out var session))
                    {
                        session.Publish(key, value);
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
                                session.PatternPublish(patternSlice, key, value);
                                numSubscribers++;
                            }
                        }
                    }
                }
            }
            return numSubscribers;
        }

        async Task Start(CancellationToken cancellationToken = default)
        {
            try
            {
                using var iterator = aof.ScanSingle(aof.BeginAddress, long.MaxValue, scanUncommitted: true);
                var signal = iterator.Signal;
                using var registration = cts.Token.Register(signal);

                while (!disposed)
                {
                    if (cts.Token.IsCancellationRequested) break;
                    await iterator.ConsumeAllAsync(this, token: cts.Token).ConfigureAwait(false);
                }
            }
            finally
            {
                done.Set();
            }
        }

        public unsafe void Consume(byte* payloadPtr, int payloadLength, long currentAddress, long nextAddress, bool isProtected)
        {
            try
            {
                cts.Token.ThrowIfCancellationRequested();

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

                var ptr = payloadPtr;
                var key = PinnedSpanByte.FromPinnedPointer(ptr + sizeof(int), *(int*)ptr);
                ptr += sizeof(int) + key.Length;
                var value = PinnedSpanByte.FromPinnedPointer(ptr + sizeof(int), *(int*)ptr);
                _ = Broadcast(key, value);
                if (nextAddress > aof.BeginAddress)
                    aof.TruncateUntil(nextAddress);
                previousAddress = nextAddress;
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
            Task.Run(() => Start(cts.Token));
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
        /// Publish the update made to key to all the subscribers, synchronously
        /// </summary>
        /// <param name="key">key that has been updated</param>
        /// <param name="value">value that has been updated</param>
        /// <returns>Number of subscribers notified</returns>
        public unsafe int PublishNow(PinnedSpanByte key, PinnedSpanByte value)
        {
            if (subscriptions == null && patternSubscriptions == null) return 0;
            return Broadcast(key, value);
        }

        /// <summary>
        /// Publish the update made to key to all the subscribers, asynchronously
        /// </summary>
        /// <param name="key">key that has been updated</param>
        /// <param name="value">value that has been updated</param>
        public unsafe void Publish(PinnedSpanByte key, PinnedSpanByte value)
        {
            if (subscriptions == null && patternSubscriptions == null)
                return;

            aof.Enqueue(key.ReadOnlySpan, value.ReadOnlySpan, out _);
        }

        /// <summary>
        /// Get the number of channels subscribed to
        /// </summary>
        /// <returns></returns>
        public List<ByteArrayWrapper> GetChannels()
        {
            if (subscriptions is null || subscriptions.Count == 0)
                return [];
            return [.. subscriptions.Keys];
        }

        /// <summary>
        /// Get the number of channels subscribed to, matching the given pattern
        /// </summary>
        /// <param name="pattern"></param>
        /// <returns></returns>
        public unsafe List<ByteArrayWrapper> GetChannels(PinnedSpanByte pattern)
        {
            if (subscriptions is null || subscriptions.Count == 0)
                return [];

            List<ByteArrayWrapper> matches = [];
            foreach (var key in subscriptions.Keys)
            {
                fixed (byte* keyPtr = key.ReadOnlySpan)
                {
                    if (Match(PinnedSpanByte.FromPinnedPointer(keyPtr, key.ReadOnlySpan.Length), pattern))
                        matches.Add(key);
                }
            }
            return matches;
        }

        /// <summary>
        /// Retrieves the number of pattern subscriptions.
        /// </summary>
        /// <returns>The number of pattern subscriptions.</returns>
        public int NumPatternSubscriptions()
            => patternSubscriptions?.Count ?? 0;

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
            disposed = true;
            cts.Cancel();
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