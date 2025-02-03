﻿// Copyright (c) Microsoft Corporation.
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
    public sealed class SubscribeBroker : IDisposable, IBulkLogEntryConsumer
    {
        int sid = 0;
        ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>> subscriptions;
        ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>> patternSubscriptions;
        readonly TsavoriteLog log;
        readonly IDevice device;
        readonly CancellationTokenSource cts = new();
        readonly ManualResetEvent done = new(true);
        bool disposed = false;
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
                    fixed (byte* keyPtr = subscribedKey)
                        this.Unsubscribe(new ArgSlice(keyPtr, subscribedKey.Length), (ServerSessionBase)session);
                }
            }

            if (patternSubscriptions != null)
            {
                foreach (var subscribedKey in patternSubscriptions.Keys)
                {
                    fixed (byte* keyPtr = subscribedKey)
                        this.PatternUnsubscribe(new ArgSlice(keyPtr, subscribedKey.Length), (ServerSessionBase)session);
                }
            }
        }

        unsafe int Broadcast(ArgSlice key, ArgSlice value)
        {
            var numSubscribers = 0;

            if (subscriptions != null)
            {
                if (subscriptions.TryGetValue(key.ToArray(), out var subscriptionServerSessionDict))
                {
                    foreach (var sub in subscriptionServerSessionDict)
                    {
                        sub.Value.Publish(key, value);
                        numSubscribers++;
                    }
                }
            }

            if (patternSubscriptions != null)
            {
                foreach (var kvp in patternSubscriptions)
                {
                    var pattern = kvp.Key;
                    fixed (byte* patternPtr = pattern)
                    {
                        var patternSlice = new ArgSlice(patternPtr, pattern.Length);
                        if (Match(key, patternSlice))
                        {
                            foreach (var sub in kvp.Value)
                            {
                                sub.Value.PatternPublish(patternSlice, key, value);
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
                    await iterator.BulkConsumeAllAsync(this, token: cts.Token).ConfigureAwait(false);
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
                var src = payloadPtr;
                while (src < payloadPtr + payloadLength)
                {
                    cts.Token.ThrowIfCancellationRequested();
                    var entryPayloadLength = log.UnsafeGetLength(src);
                    src += log.HeaderSize;
                    if (entryPayloadLength > 0)
                    {
                        var ptr = src;
                        var key = new ArgSlice(ptr + sizeof(int), *(int*)ptr);
                        ptr += sizeof(int) + key.length;
                        var value = new ArgSlice(ptr + sizeof(int), *(int*)ptr);
                        _ = Broadcast(key, value);
                        src += TsavoriteLog.UnsafeAlign(entryPayloadLength);
                    }
                    else
                    {
                        src += TsavoriteLog.UnsafeAlign(-entryPayloadLength);
                    }
                }
                if (nextAddress > log.BeginAddress)
                    log.TruncateUntil(nextAddress);
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at SubscribeBroker.Consume");
                throw;
            }
        }

        public void Throttle()
        {
        }

        void Initialize()
        {
            done.Reset();
            subscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>>(ByteArrayComparer.Instance);
            patternSubscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>>(ByteArrayComparer.Instance);
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
            var subscriptionKey = key.ReadOnlySpan.ToArray();
            subscriptions.TryAdd(subscriptionKey, new ConcurrentDictionary<int, ServerSessionBase>());
            if (subscriptions.TryGetValue(subscriptionKey, out var val))
                val.TryAdd(id, session);
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
            var subscriptionPattern = pattern.ToArray();
            patternSubscriptions.TryAdd(subscriptionPattern, new ConcurrentDictionary<int, ServerSessionBase>());
            if (patternSubscriptions.TryGetValue(subscriptionPattern, out var val))
                val.TryAdd(id, session);
            return id;
        }

        /// <summary>
        /// Unsubscribe to a particular key
        /// </summary>
        /// <param name="key">Key to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe bool Unsubscribe(ArgSlice key, ServerSessionBase session)
        {
            bool ret = false;
            if (subscriptions == null) return ret;
            if (subscriptions.TryGetValue(key.ToArray(), out var subscriptionDict))
            {
                foreach (var sid in subscriptionDict.Keys)
                {
                    if (subscriptionDict.TryGetValue(sid, out var _session))
                    {
                        if (_session == session)
                        {
                            subscriptionDict.TryRemove(sid, out _);
                            ret = true;
                            break;
                        }
                    }
                }
            }
            return ret;
        }

        /// <summary>
        /// Unsubscribe to a particular pattern
        /// </summary>
        /// <param name="key">Pattern to subscribe to</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe void PatternUnsubscribe(ArgSlice key, ServerSessionBase session)
        {
            if (patternSubscriptions == null) return;
            var subscriptionKey = key.ToArray();
            if (patternSubscriptions.ContainsKey(subscriptionKey))
            {
                if (patternSubscriptions.TryGetValue(subscriptionKey, out var subscriptionDict))
                {
                    foreach (var sid in subscriptionDict.Keys)
                    {
                        if (subscriptionDict.TryGetValue(sid, out var _session))
                        {
                            if (_session == session)
                            {
                                subscriptionDict.TryRemove(sid, out _);
                                break;
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// List all subscriptions made by a session
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        public unsafe List<byte[]> ListAllSubscriptions(ServerSessionBase session)
        {
            List<byte[]> sessionSubscriptions = new();
            if (subscriptions != null)
            {
                foreach (var subscription in subscriptions)
                {
                    if (subscription.Value.Values.Contains(session))
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
        public unsafe List<byte[]> ListAllPatternSubscriptions(ServerSessionBase session)
        {
            List<byte[]> sessionPatternSubscriptions = new();
            foreach (var patternSubscription in patternSubscriptions)
            {
                if (patternSubscription.Value.Values.Contains(session))
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
        public List<byte[]> GetChannels()
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
        public unsafe List<byte[]> GetChannels(ArgSlice pattern)
        {
            if (subscriptions is null || subscriptions.Count == 0)
                return [];

            List<byte[]> matches = [];
            foreach (var key in subscriptions.Keys)
            {
                fixed (byte* keyPtr = key)
                {
                    if (Match(new ArgSlice(keyPtr, key.Length), pattern))
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
            _ = subscriptions.TryGetValue(channel.ToArray(), out var subscriptionDict);
            return subscriptionDict?.Count ?? 0;
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