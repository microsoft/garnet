﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Broker used for pub/sub
    /// </summary>
    public sealed class SubscribeBroker : IDisposable
    {
        private int sid = 0;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>> subscriptions;
        private ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>> patternSubscriptions;
        readonly SpanByteKeySerializer spanByteSerializer;
        readonly TsavoriteLog log;
        readonly IDevice device;
        readonly CancellationTokenSource cts = new();
        readonly ManualResetEvent done = new(false);
        bool disposed = false;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="logDir">Directory where the log will be stored</param>
        /// <param name="pageSize">Page size of log used for pub/sub</param>
        /// <param name="subscriberRefreshFrequencyMs">Subscriber log refresh frequency</param>
        /// <param name="startFresh">start the log from scratch, do not continue</param>
        public SubscribeBroker(string logDir, long pageSize, int subscriberRefreshFrequencyMs, bool startFresh = true)
        {
            this.spanByteSerializer = new();
            device = logDir == null ? new NullDevice() : Devices.CreateLogDevice(logDir + "/pubsubkv", preallocateFile: false);
            device.Initialize((long)(1 << 30) * 64);
            log = new TsavoriteLog(new TsavoriteLogSettings { LogDevice = device, PageSize = pageSize, MemorySize = pageSize * 4, SafeTailRefreshFrequencyMs = subscriberRefreshFrequencyMs });
            if (startFresh)
                log.TruncateUntil(log.CommittedUntilAddress);
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
                    fixed (byte* keyPtr = &subscribedKey[0])
                        this.Unsubscribe(keyPtr, (ServerSessionBase)session);
                }
            }

            if (patternSubscriptions != null)
            {
                foreach (var subscribedKey in patternSubscriptions.Keys)
                {
                    fixed (byte* keyPtr = &subscribedKey[0])
                        this.PUnsubscribe(keyPtr, (ServerSessionBase)session);
                }
            }
        }

        private unsafe int Broadcast(byte[] key, byte* valPtr, int valLength)
        {
            int numSubscribers = 0;

            fixed (byte* ptr = &key[0])
            {
                byte* keyPtr = ptr;

                if (subscriptions != null)
                {
                    bool foundSubscription = subscriptions.TryGetValue(key, out var subscriptionServerSessionDict);
                    if (foundSubscription)
                    {
                        foreach (var sub in subscriptionServerSessionDict)
                        {
                            byte* keyBytePtr = ptr;
                            byte* nullBytePtr = null;
                            byte* valBytePtr = valPtr;
                            sub.Value.Publish(ref keyBytePtr, key.Length, ref valBytePtr, valLength, ref nullBytePtr, sub.Key);
                            numSubscribers++;
                        }
                    }
                }

                if (patternSubscriptions != null)
                {
                    foreach (var kvp in patternSubscriptions)
                    {
                        fixed (byte* subscribedPatternPtr = &kvp.Key[0])
                        {
                            byte* subscribedPattern = subscribedPatternPtr;
                            byte* reqKeyPtr = ptr;

                            bool match = spanByteSerializer.Match(ref spanByteSerializer.ReadByRef(ref reqKeyPtr), ref spanByteSerializer.ReadByRef(ref subscribedPattern));
                            if (match)
                            {
                                foreach (var sub in kvp.Value)
                                {
                                    byte* keyBytePtr = ptr;
                                    byte* nullBytePtr = null;
                                    sub.Value.PatternPublish(subscribedPatternPtr, kvp.Key.Length, ref keyBytePtr, key.Length, ref valPtr, valLength, ref nullBytePtr, sub.Key);
                                    numSubscribers++;
                                }
                            }
                        }
                    }
                }
            }
            return numSubscribers;
        }

        private async Task Start(CancellationToken cancellationToken = default)
        {
            try
            {
                var uniqueKeys = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);
                long truncateUntilAddress = log.BeginAddress;

                using var iter = log.ScanSingle(log.BeginAddress, long.MaxValue, scanUncommitted: true);
                var signal = iter.Signal;
                using var registration = cts.Token.Register(signal);

                while (!disposed)
                {
                    await iter.WaitAsync(cancellationToken).ConfigureAwait(false);
                    if (cancellationToken.IsCancellationRequested) break;
                    while (iter.GetNext(out byte[] subscriptionKeyValueAscii, out _, out long currentAddress, out long nextAddress))
                    {
                        if (currentAddress >= long.MaxValue) return;

                        byte[] subscriptionKey;
                        byte[] subscriptionValue;

                        unsafe
                        {
                            fixed (byte* subscriptionKeyValueAsciiPtr = &subscriptionKeyValueAscii[0])
                            {
                                var keyPtr = subscriptionKeyValueAsciiPtr;
                                spanByteSerializer.Skip(ref keyPtr);
                                int subscriptionKeyLength = (int)(keyPtr - subscriptionKeyValueAsciiPtr);
                                int subscriptionValueLength = subscriptionKeyValueAscii.Length - (subscriptionKeyLength + sizeof(bool));
                                subscriptionKey = new byte[subscriptionKeyLength];
                                subscriptionValue = new byte[subscriptionValueLength];

                                fixed (byte* subscriptionKeyPtr = &subscriptionKey[0], subscriptionValuePtr = &subscriptionValue[0])
                                {
                                    Buffer.MemoryCopy(subscriptionKeyValueAsciiPtr, subscriptionKeyPtr, subscriptionKeyLength, subscriptionKeyLength);
                                    Buffer.MemoryCopy(subscriptionKeyValueAsciiPtr + subscriptionKeyLength, subscriptionValuePtr, subscriptionValueLength, subscriptionValueLength);
                                }
                            }
                        }
                        truncateUntilAddress = nextAddress;
                        if (!uniqueKeys.ContainsKey(subscriptionKey))
                            uniqueKeys.Add(subscriptionKey, subscriptionValue);
                    }

                    if (truncateUntilAddress > log.BeginAddress)
                        log.TruncateUntil(truncateUntilAddress);

                    unsafe
                    {
                        var enumerator = uniqueKeys.GetEnumerator();
                        while (enumerator.MoveNext())
                        {
                            byte[] keyBytes = enumerator.Current.Key;
                            byte[] valBytes = enumerator.Current.Value;
                            fixed (byte* valPtr = valBytes)
                                Broadcast(keyBytes, valPtr, valBytes.Length);
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

        void Initialize()
        {
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
        public unsafe int Subscribe(ref byte* key, ServerSessionBase session)
        {
            var start = key;
            spanByteSerializer.Skip(ref key);
            var id = Interlocked.Increment(ref sid);

            if (id == 1)
            {
                Initialize();
            }
            else
            {
                while (patternSubscriptions == null) Thread.Yield();
            }
            var subscriptionKey = new Span<byte>(start, (int)(key - start)).ToArray();
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
        public unsafe int PatternSubscribe(ref byte* pattern, ServerSessionBase session)
        {
            var start = pattern;
            spanByteSerializer.Skip(ref pattern);
            var id = Interlocked.Increment(ref sid);
            if (id == 1)
            {
                Initialize();
            }
            else
            {
                while (patternSubscriptions == null) Thread.Yield();
            }
            var subscriptionPattern = new Span<byte>(start, (int)(pattern - start)).ToArray();
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
        public unsafe bool Unsubscribe(byte* key, ServerSessionBase session)
        {
            bool ret = false;
            var start = key;
            spanByteSerializer.Skip(ref key);
            var subscriptionKey = new Span<byte>(start, (int)(key - start)).ToArray();
            if (subscriptions == null) return ret;
            if (subscriptions.TryGetValue(subscriptionKey, out var subscriptionDict))
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
        public unsafe void PUnsubscribe(byte* key, ServerSessionBase session)
        {
            var start = key;
            spanByteSerializer.Skip(ref key);
            var subscriptionKey = new Span<byte>(start, (int)(key - start)).ToArray();
            if (patternSubscriptions == null) return;
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
        /// <param name="valueLength">value length that has been updated</param>
        /// <param name="ascii">whether ascii</param>
        public unsafe int PublishNow(byte* key, byte* value, int valueLength, bool ascii)
        {
            if (subscriptions == null && patternSubscriptions == null) return 0;

            var start = key;
            spanByteSerializer.Skip(ref key);
            var keyBytes = new Span<byte>(start, (int)(key - start)).ToArray();
            return Broadcast(keyBytes, value, valueLength);
        }

        /// <summary>
        /// Publish the update made to key to all the subscribers, asynchronously
        /// </summary>
        /// <param name="parseState">ParseState for publish message</param>
        public unsafe void Publish(ref SessionParseState parseState)
        {
            if (subscriptions == null && patternSubscriptions == null) return;

            var key = parseState.GetArgSliceByRef(0).ptr - sizeof(int);
            var value = parseState.GetArgSliceByRef(1).ptr - sizeof(int);

            var kSize = parseState.GetArgSliceByRef(0).Length;
            var vSize = parseState.GetArgSliceByRef(1).Length;
            *(int*)key = kSize;
            *(int*)value = vSize;
            var valueLength = vSize + sizeof(int);

            var start = key;
            spanByteSerializer.Skip(ref key);

            // TODO: this needs to be a single atomic enqueue
            byte[] logEntryBytes = new byte[key - start + valueLength];
            fixed (byte* logEntryBytePtr = &logEntryBytes[0])
            {
                byte* dst = logEntryBytePtr;
                Buffer.MemoryCopy(start, dst, key - start, key - start);
                dst += key - start;
                Buffer.MemoryCopy(value, dst, valueLength, valueLength);
                dst += valueLength;
            }

            log.Enqueue(logEntryBytes);
        }

        /// <summary>
        /// Retrieves the collection of channels that have active subscriptions.
        /// </summary>
        /// <returns>The collection of channels.</returns>
        public unsafe void Channels(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            try
            {
                if (subscriptions is null || subscriptions.Count == 0)
                {
                    while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                if (input.parseState.Count == 0)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(subscriptions.Count, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    foreach (var key in subscriptions.Keys)
                    {
                        while (!RespWriteUtils.TryWriteBulkString(key.AsSpan().Slice(sizeof(int)), ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    return;
                }

                // Below WriteArrayLength is primarily to move the start of the buffer to the max length that is required to write the array length. The actual length is written in the below line.
                // This is done to avoid multiple two passes over the subscriptions or new array allocation if we use single pass over the subscriptions
                var totalArrayHeaderLen = 0;
                while (!RespWriteUtils.TryWriteArrayLength(subscriptions.Count, ref curr, end, out var _, out totalArrayHeaderLen))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                var noOfFoundChannels = 0;
                var pattern = input.parseState.GetArgSliceByRef(0).SpanByte;
                var patternPtr = pattern.ToPointer() - sizeof(int);
                *(int*)patternPtr = pattern.Length;

                foreach (var key in subscriptions.Keys)
                {
                    fixed (byte* keyPtr = key)
                    {
                        var endKeyPtr = keyPtr;
                        var _patternPtr = patternPtr;
                        if (spanByteSerializer.Match(ref spanByteSerializer.ReadByRef(ref endKeyPtr), ref spanByteSerializer.ReadByRef(ref _patternPtr)))
                        {
                            while (!RespWriteUtils.TryWriteSimpleString(key.AsSpan().Slice(sizeof(int)), ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            noOfFoundChannels++;
                        }
                    }
                }

                if (noOfFoundChannels == 0)
                {
                    curr = ptr;
                    while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                // Below code is to write the actual array length in the buffer
                // And move the array elements to the start of the new array length if new array length is less than the max array length that we orginally write in the above line
                var newTotalArrayHeaderLen = 0;
                var _ptr = ptr;
                // ReallocateOutput is not needed here as there should be always be available space in the output buffer as we have already written the max array length
                _ = RespWriteUtils.TryWriteArrayLength(noOfFoundChannels, ref _ptr, end, out var _, out newTotalArrayHeaderLen);

                Debug.Assert(totalArrayHeaderLen >= newTotalArrayHeaderLen, "newTotalArrayHeaderLen can't be bigger than totalArrayHeaderLen as we have already written max array lenght in the buffer");
                if (totalArrayHeaderLen != newTotalArrayHeaderLen)
                {
                    var remainingLength = curr - ptr - totalArrayHeaderLen;
                    Buffer.MemoryCopy(ptr + totalArrayHeaderLen, ptr + newTotalArrayHeaderLen, remainingLength, remainingLength);
                    curr = curr - (totalArrayHeaderLen - newTotalArrayHeaderLen);
                }
            }
            finally
            {
                if (isMemory)
                    ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
        }

        /// <summary>
        /// Retrieves the number of pattern subscriptions.
        /// </summary>
        /// <returns>The number of pattern subscriptions.</returns>
        public int NumPatternSubscriptions()
        {
            return patternSubscriptions?.Count ?? 0;
        }

        /// <summary>
        /// PUBSUB NUMSUB
        /// </summary>
        /// <param name="output"></param>
        /// <returns></returns>
        public unsafe void NumSubscriptions(ref ObjectInput input, ref SpanByteAndMemory output)
        {
            var isMemory = false;
            MemoryHandle ptrHandle = default;
            var ptr = output.SpanByte.ToPointer();

            var curr = ptr;
            var end = curr + output.Length;

            try
            {
                var numOfChannels = input.parseState.Count;
                if (subscriptions is null || numOfChannels == 0)
                {
                    while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                while (!RespWriteUtils.TryWriteArrayLength(numOfChannels * 2, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                var currChannelIdx = 0;
                while (currChannelIdx < numOfChannels)
                {
                    var channelArg = input.parseState.GetArgSliceByRef(currChannelIdx);
                    var channelSpan = channelArg.SpanByte;
                    var channelPtr = channelSpan.ToPointer() - sizeof(int);  // Memory would have been already pinned
                    *(int*)channelPtr = channelSpan.Length;

                    while (!RespWriteUtils.TryWriteBulkString(channelArg.ReadOnlySpan, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    var channel = new Span<byte>(channelPtr, channelSpan.Length + sizeof(int)).ToArray();

                    subscriptions.TryGetValue(channel, out var subscriptionDict);
                    while (!RespWriteUtils.TryWriteInt32(subscriptionDict is null ? 0 : subscriptionDict.Count, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    currChannelIdx++;
                }
            }
            finally
            {
                if (isMemory)
                    ptrHandle.Dispose();
                output.Length = (int)(curr - ptr);
            }
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
    }
}