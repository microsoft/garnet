// Copyright (c) Microsoft Corporation.
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
    public sealed class SubscribeBroker : IDisposable, ILogEntryConsumer
    {
        int sid = 0;
        bool initialized = false;
        ConcurrentDictionary<ByteArrayWrapper, ReadOptimizedConcurrentSet<ServerSessionBase>> subscriptions;
        ReadOptimizedConcurrentSet<PatternSubscriptionEntry> patternSubscriptions;
        readonly TsavoriteLog log;
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
        public SubscribeBroker(string logDir, long pageSize, int subscriberRefreshFrequencyMs, LightEpoch epoch, bool startFresh = true, ILogger logger = null)
        {
            device = logDir == null ? new NullDevice() : Devices.CreateLogDevice(logDir + "/pubsubkv", preallocateFile: false);
            device.Initialize((long)(1 << 30) * 64);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSize = pageSize, MemorySize = pageSize * 4, SafeTailRefreshFrequencyMs = subscriberRefreshFrequencyMs, Epoch = epoch });
            pageSizeBits = log.UnsafeGetLogPageSizeBits();
            if (startFresh)
                log.TruncateUntil(log.CommittedUntilAddress);
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

        unsafe int Broadcast(ArgSlice key, ArgSlice value)
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
                        var patternSlice = new ArgSlice(patternPtr, pattern.ReadOnlySpan.Length);
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
                using var iterator = log.ScanSingle(log.BeginAddress, long.MaxValue, scanUncommitted: true);
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
                var key = new ArgSlice(ptr + sizeof(int), *(int*)ptr);
                ptr += sizeof(int) + key.length;
                var value = new ArgSlice(ptr + sizeof(int), *(int*)ptr);
                _ = Broadcast(key, value);
                if (nextAddress > log.BeginAddress)
                    log.TruncateUntil(nextAddress);
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
        public unsafe bool Subscribe(ArgSlice key, ServerSessionBase session)
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
        public unsafe bool PatternSubscribe(ArgSlice pattern, ServerSessionBase session)
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
        public unsafe int PublishNow(ArgSlice key, ArgSlice value)
        {
            if (subscriptions == null && patternSubscriptions == null) return 0;
            return Broadcast(key, value);
        }

        /// <summary>
        /// Publish the update made to key to all the subscribers, asynchronously
        /// </summary>
        /// <param name="key">key that has been updated</param>
        /// <param name="value">value that has been updated</param>
        public unsafe void Publish(ArgSlice key, ArgSlice value)
        {
            if (subscriptions == null && patternSubscriptions == null) return;

            var keySB = key.SpanByte;
            var valueSB = value.SpanByte;
            log.Enqueue(ref keySB, ref valueSB, out _);
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
        public unsafe List<ByteArrayWrapper> GetChannels(ArgSlice pattern)
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
                        if (Match(new ArgSlice(keyPtr, entry.Key.ReadOnlySpan.Length), pattern))
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
        public int NumSubscriptions(ArgSlice channel)
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
            log.Dispose();
            device.Dispose();
        }

        unsafe bool Match(ArgSlice key, ArgSlice pattern)
            => GlobUtils.Match(pattern.ptr, pattern.length, key.ptr, key.length);
    }
}