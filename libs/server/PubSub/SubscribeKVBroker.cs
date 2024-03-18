// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Broker used for PUB-SUB to Tsavorite KV store. There is a broker per TsavoriteKV instance.
    /// A single broker can be used with multiple TsavoriteKVProviders. 
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="KeyInputSerializer"></typeparam>
    public sealed class SubscribeKVBroker<Key, Value, Input, KeyInputSerializer> : IDisposable
        where KeyInputSerializer : IKeyInputSerializer<Key, Input>
    {
        private int sid = 0;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<int, (ServerSessionBase, byte[])>> subscriptions;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<int, (ServerSessionBase, byte[])>> prefixSubscriptions;
        private AsyncQueue<byte[]> publishQueue;
        readonly IKeyInputSerializer<Key, Input> keyInputSerializer;
        readonly TsavoriteLog log;
        readonly IDevice device;
        readonly CancellationTokenSource cts = new();
        readonly ManualResetEvent done = new(true);
        bool disposed = false;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="keyInputSerializer">Serializer for Prefix Match and serializing Key and Input</param>
        /// <param name="logDir">Directory where the log will be stored</param>
        /// <param name="pageSize">Page size of log used for pub/sub</param>
        /// <param name="startFresh">start the log from scratch, do not continue</param>
        public SubscribeKVBroker(IKeyInputSerializer<Key, Input> keyInputSerializer, string logDir, long pageSize, bool startFresh = true)
        {
            this.keyInputSerializer = keyInputSerializer;
            device = logDir == null ? new NullDevice() : Devices.CreateLogDevice(logDir + "/pubsubkv", preallocateFile: false);
            device.Initialize((long)(1 << 30) * 64);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSize = pageSize, MemorySize = pageSize * 4, AutoRefreshSafeTailAddress = true });
            if (startFresh)
                log.TruncateUntil(log.CommittedUntilAddress);
        }

        /// <summary>
        /// Remove all subscriptions for a session,
        /// called during dispose of server session
        /// </summary>
        /// <param name="session">server session</param>
        public void RemoveSubscription(IMessageConsumer session)
        {
            if (subscriptions != null)
            {
                foreach (var kvp in subscriptions)
                {
                    foreach (var sub in kvp.Value)
                    {
                        if (sub.Value.Item1 == session)
                        {
                            kvp.Value.TryRemove(sub.Key, out _);
                            break;
                        }
                    }
                }
            }

            if (prefixSubscriptions != null)
            {
                foreach (var kvp in prefixSubscriptions)
                {
                    foreach (var sub in kvp.Value)
                    {
                        if (sub.Value.Item1 == session)
                        {
                            kvp.Value.TryRemove(sub.Key, out _);
                            break;
                        }
                    }
                }
            }
        }

        internal async Task Start(CancellationToken cancellationToken = default)
        {
            try
            {
                var uniqueKeys = new HashSet<byte[]>(new ByteArrayComparer());
                var uniqueKeySubscriptions = new List<(ServerSessionBase, int, bool)>();
                long truncateUntilAddress = log.BeginAddress;

                while (true)
                {
                    if (disposed)
                        break;

                    using var iter = log.Scan(log.BeginAddress, long.MaxValue, scanUncommitted: true);
                    await iter.WaitAsync(cancellationToken).ConfigureAwait(false);
                    while (iter.GetNext(out byte[] subscriptionKey, out int entryLength, out long currentAddress, out long nextAddress))
                    {
                        if (currentAddress >= long.MaxValue) return;
                        uniqueKeys.Add(subscriptionKey);
                        truncateUntilAddress = nextAddress;
                    }

                    if (truncateUntilAddress > log.BeginAddress)
                        log.TruncateUntil(truncateUntilAddress);

                    unsafe
                    {
                        foreach (var keyBytes in uniqueKeys)
                        {
                            fixed (byte* ptr = &keyBytes[0])
                            {
                                byte* keyPtr = ptr;
                                bool foundSubscription = subscriptions.TryGetValue(keyBytes, out var subscriptionServerSessionDict);
                                if (foundSubscription)
                                {
                                    foreach (var sub in subscriptionServerSessionDict)
                                    {
                                        byte* keyBytePtr = ptr;
                                        var serverSession = sub.Value.Item1;
                                        byte* nullBytePtr = null;

                                        fixed (byte* inputPtr = &sub.Value.Item2[0])
                                        {
                                            byte* inputBytePtr = inputPtr;
                                            serverSession.Publish(ref keyBytePtr, keyBytes.Length, ref nullBytePtr, 0, ref inputBytePtr, sub.Key);
                                        }
                                    }
                                }

                                foreach (var kvp in prefixSubscriptions)
                                {
                                    var subscribedPrefixBytes = kvp.Key;
                                    var prefixSubscriptionServerSessionDict = kvp.Value;
                                    fixed (byte* subscribedPrefixPtr = &subscribedPrefixBytes[0])
                                    {
                                        byte* subPrefixPtr = subscribedPrefixPtr;
                                        byte* reqKeyPtr = ptr;

                                        bool match = keyInputSerializer.Match(ref keyInputSerializer.ReadKeyByRef(ref reqKeyPtr), false,
                                            ref keyInputSerializer.ReadKeyByRef(ref subPrefixPtr), false);
                                        if (match)
                                        {
                                            foreach (var sub in prefixSubscriptionServerSessionDict)
                                            {
                                                byte* keyBytePtr = ptr;
                                                var serverSession = sub.Value.Item1;
                                                byte* nullBytrPtr = null;

                                                fixed (byte* inputPtr = &sub.Value.Item2[0])
                                                {
                                                    byte* inputBytePtr = inputPtr;
                                                    serverSession.PrefixPublish(subPrefixPtr, kvp.Key.Length, ref keyBytePtr, keyBytes.Length, ref nullBytrPtr, 0, ref inputBytePtr, sub.Key);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            uniqueKeySubscriptions.Clear();
                        }
                        uniqueKeys.Clear();
                    }
                }
            }
            finally
            {
                done.Set();
            }
        }

        /// <summary>
        /// Subscribe to a particular Key
        /// </summary>
        /// <param name="key">Key to subscribe to</param>
        /// <param name="input">Input from subscriber</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe int Subscribe(ref byte* key, ref byte* input, ServerSessionBase session)
        {
            var start = key;
            var inputStart = input;
            keyInputSerializer.ReadKeyByRef(ref key);
            keyInputSerializer.ReadInputByRef(ref input);
            var id = Interlocked.Increment(ref sid);
            if (Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<byte[]>(), null) == null)
            {
                done.Reset();
                subscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<int, (ServerSessionBase, byte[])>>(new ByteArrayComparer());
                prefixSubscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<int, (ServerSessionBase, byte[])>>(new ByteArrayComparer());
                Task.Run(() => Start(cts.Token));
            }
            else
            {
                while (prefixSubscriptions == null) Thread.Yield();
            }
            var subscriptionKey = new Span<byte>(start, (int)(key - start)).ToArray();
            var subscriptionInput = new Span<byte>(inputStart, (int)(input - inputStart)).ToArray();
            subscriptions.TryAdd(subscriptionKey, new ConcurrentDictionary<int, (ServerSessionBase, byte[])>());
            if (subscriptions.TryGetValue(subscriptionKey, out var val))
                val.TryAdd(sid, (session, subscriptionInput));
            return id;
        }

        /// <summary>
        /// Subscribe to a particular prefix
        /// </summary>
        /// <param name="prefix">prefix to subscribe to</param>
        /// <param name="input">Input from subscriber</param>
        /// <param name="session">Server session</param>
        /// <returns></returns>
        public unsafe int PSubscribe(ref byte* prefix, ref byte* input, ServerSessionBase session)
        {
            var start = prefix;
            var inputStart = input;
            keyInputSerializer.ReadKeyByRef(ref prefix);
            keyInputSerializer.ReadInputByRef(ref input);
            var id = Interlocked.Increment(ref sid);
            if (Interlocked.CompareExchange(ref publishQueue, new AsyncQueue<byte[]>(), null) == null)
            {
                done.Reset();
                subscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<int, (ServerSessionBase, byte[])>>(new ByteArrayComparer());
                prefixSubscriptions = new ConcurrentDictionary<byte[], ConcurrentDictionary<int, (ServerSessionBase, byte[])>>(new ByteArrayComparer());
                Task.Run(() => Start(cts.Token));
            }
            else
            {
                while (prefixSubscriptions == null) Thread.Yield();
            }
            var subscriptionPrefix = new Span<byte>(start, (int)(prefix - start)).ToArray();
            var subscriptionInput = new Span<byte>(inputStart, (int)(input - inputStart)).ToArray();
            prefixSubscriptions.TryAdd(subscriptionPrefix, new ConcurrentDictionary<int, (ServerSessionBase, byte[])>());
            if (prefixSubscriptions.TryGetValue(subscriptionPrefix, out var val))
                val.TryAdd(sid, (session, subscriptionInput));
            return id;
        }

        /// <summary>
        /// Publish the update made to key to all the subscribers
        /// </summary>
        /// <param name="key">key that has been updated</param>
        public unsafe void Publish(byte* key)
        {
            if (subscriptions == null && prefixSubscriptions == null) return;

            var start = key;
            ref Key k = ref keyInputSerializer.ReadKeyByRef(ref key);
            log.Enqueue(new Span<byte>(start, (int)(key - start)));
        }

        /// <inheritdoc />
        public void Dispose()
        {
            disposed = true;
            cts.Cancel();
            done.WaitOne();
            subscriptions?.Clear();
            prefixSubscriptions?.Clear();
            log.Dispose();
            device.Dispose();
        }
    }
}