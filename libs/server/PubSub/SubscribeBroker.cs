// Copyright (c) Microsoft Corporation.
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
        int sid = 0;
        ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>> subscriptions;
        ConcurrentDictionary<byte[], ConcurrentDictionary<int, ServerSessionBase>> patternSubscriptions;
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
            int numSubscribers = 0;

            if (subscriptions != null)
            {
                bool foundSubscription = subscriptions.TryGetValue(key.ToArray(), out var subscriptionServerSessionDict);
                if (foundSubscription)
                {
                    foreach (var sub in subscriptionServerSessionDict)
                    {
                        sub.Value.Publish(key, value, sub.Key);
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
                        bool match = Match(key, pattern);
                        if (match)
                        {
                            foreach (var sub in kvp.Value)
                            {
                                sub.Value.PatternPublish(new ArgSlice(patternPtr, pattern.Length), key, value, sub.Key);
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
                var uniqueKeys = new HashSet<byte[]>(ByteArrayComparer.Instance);
                long truncateUntilAddress = log.BeginAddress;

                using var iterator = log.ScanSingle(log.BeginAddress, long.MaxValue, scanUncommitted: true);
                var signal = iterator.Signal;
                using var registration = cts.Token.Register(signal);

                while (!disposed)
                {
                    await iterator.WaitAsync(cancellationToken).ConfigureAwait(false);
                    if (cancellationToken.IsCancellationRequested) break;
                    unsafe
                    {
                        while (iterator.GetNext(out var logEntry, out _, out long currentAddress, out long nextAddress))
                        {
                            if (currentAddress >= long.MaxValue) return;
                            fixed (byte* logEntryPtr = logEntry)
                            {
                                var key = new ArgSlice(logEntryPtr + sizeof(int), *(int*)logEntryPtr);
                                var value = new ArgSlice(logEntryPtr + sizeof(int) + key.length, *(int*)(logEntryPtr + sizeof(int) + key.Length));
                                truncateUntilAddress = nextAddress;
                                if (uniqueKeys.Add(key.ToArray()))
                                    Broadcast(key, value);
                            }
                        }

                        if (truncateUntilAddress > log.BeginAddress)
                            log.TruncateUntil(truncateUntilAddress);

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
        /// <param name="parseState">ParseState for publish message</param>
        public unsafe void Publish(ref SessionParseState parseState)
        {
            if (subscriptions == null && patternSubscriptions == null) return;

            var key = parseState.GetArgSliceByRef(0);
            var value = parseState.GetArgSliceByRef(1);

            var logEntry = new byte[sizeof(int) + key.length + sizeof(int) + value.length];
            fixed (byte* logEntryPtr = logEntry)
            {
                var dst = logEntryPtr;

                *(int*)dst = key.length;
                dst += sizeof(int);
                key.ReadOnlySpan.CopyTo(new Span<byte>(dst, key.length));
                dst += key.length;

                *(int*)dst = value.length;
                dst += sizeof(int);
                value.ReadOnlySpan.CopyTo(new Span<byte>(dst, value.length));
                dst += value.length;
            }

            log.Enqueue(logEntry);
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
                        while (!RespWriteUtils.TryWriteBulkString(key.AsSpan(), ref curr, end))
                            ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    }
                    return;
                }

                // Below WriteArrayLength is primarily to move the start of the buffer to the max length that is required to write the array length. The actual length is written in the below line.
                // This is done to avoid multiple two passes over the subscriptions or new array allocation if we use single pass over the subscriptions
                var totalArrayHeaderLen = 0;
                while (!RespWriteUtils.TryWriteArrayLength(subscriptions.Count, ref curr, end, out var _, out totalArrayHeaderLen))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                var foundChannels = 0;
                var pattern = input.parseState.GetArgSliceByRef(0);

                foreach (var key in subscriptions.Keys)
                {
                    fixed (byte* keyPtr = key)
                    {
                        var endKeyPtr = keyPtr;
                        if (Match(new ArgSlice(keyPtr, key.Length), pattern))
                        {
                            while (!RespWriteUtils.TryWriteSimpleString(key.AsSpan(), ref curr, end))
                                ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                            foundChannels++;
                        }
                    }
                }

                if (foundChannels == 0)
                {
                    curr = ptr;
                    while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                // Below code is to write the actual array length in the buffer
                // And move the array elements to the start of the new array length if new array length is less than the max array length that we originally write in the above line
                var newTotalArrayHeaderLen = 0;
                var _ptr = ptr;
                // ReallocateOutput is not needed here as there should be always be available space in the output buffer as we have already written the max array length
                _ = RespWriteUtils.TryWriteArrayLength(foundChannels, ref _ptr, end, out var _, out newTotalArrayHeaderLen);

                Debug.Assert(totalArrayHeaderLen >= newTotalArrayHeaderLen, "newTotalArrayHeaderLen can't be bigger than totalArrayHeaderLen as we have already written max array length in the buffer");
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
            => patternSubscriptions?.Count ?? 0;

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
                var numChannels = input.parseState.Count;
                if (subscriptions is null || numChannels == 0)
                {
                    while (!RespWriteUtils.TryWriteEmptyArray(ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
                    return;
                }

                while (!RespWriteUtils.TryWriteArrayLength(numChannels * 2, ref curr, end))
                    ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                for (int c = 0; c < numChannels; c++)
                {
                    var channel = input.parseState.GetArgSliceByRef(c);

                    while (!RespWriteUtils.TryWriteBulkString(channel.ReadOnlySpan, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);

                    subscriptions.TryGetValue(channel.ToArray(), out var subscriptionDict);
                    while (!RespWriteUtils.TryWriteInt32(subscriptionDict is null ? 0 : subscriptionDict.Count, ref curr, end))
                        ObjectUtils.ReallocateOutput(ref output, ref isMemory, ref ptr, ref ptrHandle, ref curr, ref end);
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

        unsafe bool Match(ArgSlice key, byte[] pattern)
        {
            fixed (byte* p = pattern)
                return Match(key, new ArgSlice(p, pattern.Length));
        }

        unsafe bool Match(ArgSlice key, ArgSlice pattern)
            => GlobUtils.Match(pattern.ptr, pattern.length, key.ptr, key.length);
    }
}