// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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
        ConcurrentDictionary<ByteArrayWrapper, ConcurrentList<ServerSessionBase>> subscriptions;
        ConcurrentDictionary<ByteArrayWrapper, ConcurrentList<ServerSessionBase>> patternSubscriptions;
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
        public SubscribeBroker(string logDir, long pageSize, int subscriberRefreshFrequencyMs, bool startFresh = true, ILogger logger = null)
        {
            device = logDir == null ? new NullDevice() : Devices.CreateLogDevice(logDir + "/pubsubkv", preallocateFile: false);
            device.Initialize((long)(1 << 30) * 64);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSize = pageSize, MemorySize = pageSize * 4, SafeTailRefreshFrequencyMs = subscriberRefreshFrequencyMs });
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
                foreach (var subscribedKey in patternSubscriptions.Keys)
                {
                    this.PatternUnsubscribe(subscribedKey, (ServerSessionBase)session);
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
                    int index = 0;
                    while (sessions.Get(ref index, out var session))
                    {
                        session.Publish(key, value);
                        numSubscribers++;
                    }
                }
            }

            if (patternSubscriptions != null && patternSubscriptions.Count > 0)
            {
                foreach (var kvp in patternSubscriptions)
                {
                    var pattern = kvp.Key;
                    fixed (byte* patternPtr = pattern.ReadOnlySpan)
                    {
                        var patternSlice = new ArgSlice(patternPtr, pattern.ReadOnlySpan.Length);
                        if (Match(key, patternSlice))
                        {
                            var sessions = kvp.Value;
                            int index = 0;
                            while (sessions.Get(ref index, out var session))
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
            subscriptions = new ConcurrentDictionary<ByteArrayWrapper, ConcurrentList<ServerSessionBase>>(ByteArrayWrapperComparer.Instance);
            patternSubscriptions = new ConcurrentDictionary<ByteArrayWrapper, ConcurrentList<ServerSessionBase>>(ByteArrayWrapperComparer.Instance);
            Task.Run(() => Start(cts.Token));
        }

        /// <summary>
        /// Subscribe to a particular Key
        /// </summary>
        /// <param name="key">Key to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe int Subscribe(ArgSlice key, ServerSessionBase session)
        {
            var id = Interlocked.Increment(ref sid);

            if (id == 1)
            {
                Initialize();
            }
            else
            {
                while (patternSubscriptions == null) Thread.Yield();
            }
            var subscriptionKey = ByteArrayWrapper.CopyFrom(key.ReadOnlySpan, false);
            subscriptions.TryAdd(subscriptionKey, new ConcurrentList<ServerSessionBase>());
            if (subscriptions.TryGetValue(subscriptionKey, out var sessions))
                sessions.Add(session);
            return id;
        }

        /// <summary>
        /// Subscribe to a particular pattern
        /// </summary>
        /// <param name="pattern">Pattern to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe int PatternSubscribe(ArgSlice pattern, ServerSessionBase session)
        {
            var id = Interlocked.Increment(ref sid);
            if (id == 1)
            {
                Initialize();
            }
            else
            {
                while (patternSubscriptions == null) Thread.Yield();
            }
            var subscriptionPattern = ByteArrayWrapper.CopyFrom(pattern.ReadOnlySpan, false);
            patternSubscriptions.TryAdd(subscriptionPattern, new ConcurrentList<ServerSessionBase>());
            if (patternSubscriptions.TryGetValue(subscriptionPattern, out var sessions))
                sessions.Add(session);
            return id;
        }

        /// <summary>
        /// Unsubscribe to a particular key
        /// </summary>
        /// <param name="key">Key to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe bool Unsubscribe(ByteArrayWrapper key, ServerSessionBase session)
        {
            bool ret = false;
            if (subscriptions == null) return ret;
            if (subscriptions.TryGetValue(key, out var sessions))
            {
                return sessions.RemoveAll(session);
            }
            return ret;
        }

        /// <summary>
        /// Unsubscribe to a particular pattern
        /// </summary>
        /// <param name="key">Pattern to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe void PatternUnsubscribe(ByteArrayWrapper key, ServerSessionBase session)
        {
            if (patternSubscriptions == null) return;
            if (patternSubscriptions.ContainsKey(key))
            {
                if (patternSubscriptions.TryGetValue(key, out var sessions))
                {
                    sessions.RemoveAll(session);
                }
            }
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
            foreach (var patternSubscription in patternSubscriptions)
            {
                if (patternSubscription.Value.Count > 0)
                    sessionPatternSubscriptions.Add(patternSubscription.Key);
            }

            return sessionPatternSubscriptions;
        }

        /// <summary>
        /// Publish the update made to key to all the subscribers, synchronously
        /// </summary>
        /// <param name="key">key that has been updated</param>
        /// <param name="value">value that has been updated</param>
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
            if (subscriptions is null || subscriptions.Count == 0)
                return [];
            return [.. subscriptions.Keys];
        }

        /// <summary>
        /// Get the number of channels subscribed to, matching the given pattern
        /// </summary>
        /// <param name="pattern"></param>
        /// <returns></returns>
        public unsafe List<ByteArrayWrapper> GetChannels(ArgSlice pattern)
        {
            if (subscriptions is null || subscriptions.Count == 0)
                return [];

            List<ByteArrayWrapper> matches = [];
            foreach (var key in subscriptions.Keys)
            {
                fixed (byte* keyPtr = key.ReadOnlySpan)
                {
                    if (Match(new ArgSlice(keyPtr, key.ReadOnlySpan.Length), pattern))
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