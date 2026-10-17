// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Server session for RESP protocol - pub/sub commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        readonly SubscribeBroker subscribeBroker;
        bool isSubscriptionSession = false;
        int numActiveChannels = 0;

        /// <inheritdoc />
        public override unsafe void Publish(PinnedSpanByte key, PinnedSpanByte value)
            => _ = TryPublishCore(parkIfSuspended: false, isPattern: false, default, key.ReadOnlySpan, value.ReadOnlySpan);

        /// <inheritdoc />
        public override unsafe void PatternPublish(PinnedSpanByte pattern, PinnedSpanByte key, PinnedSpanByte value)
            => _ = TryPublishCore(parkIfSuspended: false, isPattern: true, pattern.ReadOnlySpan, key.ReadOnlySpan, value.ReadOnlySpan);

        /// <inheritdoc />
        internal override unsafe bool TryPublish(PinnedSpanByte key, PinnedSpanByte value)
            => !MustWaitToPublish && TryPublishCore(parkIfSuspended: true, isPattern: false, default, key.ReadOnlySpan, value.ReadOnlySpan);

        /// <inheritdoc />
        internal override unsafe bool TryPatternPublish(PinnedSpanByte pattern, PinnedSpanByte key, PinnedSpanByte value)
            => !MustWaitToPublish && TryPublishCore(parkIfSuspended: true, isPattern: true, pattern.ReadOnlySpan, key.ReadOnlySpan, value.ReadOnlySpan);

        /// <summary>
        /// Whether a publisher has to hold its message back until this session's connection has room. A session
        /// publishing to itself writes into the batch it is already building, so it never waits.
        /// </summary>
        bool MustWaitToPublish => publishingThreadId != Environment.CurrentManagedThreadId && networkSender.IsSendSaturated;

        /// <summary>
        /// Writes a message to this subscriber, handing it to the sender without waiting for the connection to
        /// have room. A publisher that should not add to a full connection checks <see cref="MustWaitToPublish"/>
        /// first. A message that finds the response object taken is left for its holder to write out (see
        /// <see cref="heldPushes"/>).
        /// </summary>
        /// <param name="parkIfSuspended">
        /// Whether the publisher can park instead, for a session suspended holding its response object.
        /// </param>
        /// <returns>
        /// False, with nothing written, if <paramref name="parkIfSuspended"/> and the session is suspended holding
        /// its response object. A message to a connection that is gone counts as delivered.
        /// </returns>
        bool TryPublishCore(bool parkIfSuspended, bool isPattern, ReadOnlySpan<byte> pattern, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            // A session that publishes to a channel it is subscribed to re-enters this callback on the
            // thread already holding the network sender lock (from the outer command's response path). In
            // that reentrant case, write into the current output buffer and let the outer call own the lock
            // and flush; the sender lock is not reentrant.
            if (publishingThreadId == Environment.CurrentManagedThreadId)
            {
                if (reentrantPublishFailure != null)
                    return true;

                try
                {
                    WritePublishMessage(isPattern, pattern, key, value);
                }
                catch (Exception ex)
                {
                    // A failed send may have released the outer command's response buffer, so nothing more can be
                    // written to it. The outer command rethrows once the broadcast to other subscribers completes.
                    reentrantPublishFailure = ExceptionDispatchInfo.Capture(ex);
                }

                return true;
            }

            if (!TryEnterSenderForPush())
            {
                if (parkIfSuspended && pushesHeldBack)
                    return false;

                LeavePush(isPattern, pattern, key, value);
                return true;
            }

            try
            {
                networkSender.GetResponseObject();
            }
            catch
            {
                // The connection is gone, and the message with it
                networkSender.Exit();
                ServeSenderWaiters();
                return true;
            }

            dcurr = networkSender.GetResponseObjectHead();
            dend = networkSender.GetResponseObjectTail();
            try
            {
                writingOutOfBand = true;
                WriteHeldPushes();
                WritePublishMessage(isPattern, pattern, key, value);

                // Flush the publish message for this subscriber
                if (dcurr > networkSender.GetResponseObjectHead())
                    SendNow(networkSender.GetResponseObjectHead());
            }
            catch
            {
                // Ignore exceptions
            }
            finally
            {
                writingOutOfBand = false;
                networkSender.ExitAndReturnResponseObject();
            }

            ServeSenderWaiters();
            return true;
        }

        /// <summary>
        /// Writes a message to a channel, or to a pattern it matched, into the response buffer.
        /// </summary>
        void WritePublishMessage(bool isPattern, ReadOnlySpan<byte> pattern, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (isPattern)
            {
                WritePushLength(4);
                while (!RespWriteUtils.TryWriteBulkString("pmessage"u8, ref dcurr, dend))
                    SendAndReset();
                WriteDirectLargeRespString(pattern);
            }
            else
            {
                WritePushLength(3);
                while (!RespWriteUtils.TryWriteBulkString("message"u8, ref dcurr, dend))
                    SendAndReset();
            }

            WriteDirectLargeRespString(key);
            WriteDirectLargeRespString(value);
        }

        /// <summary>
        /// PUBLISH
        /// </summary>
        private bool NetworkPUBLISH(RespCommand cmd)
        {
            if (parseState.Count != 2)
            {
                var cmdName = cmd switch
                {
                    RespCommand.PUBLISH => nameof(RespCommand.PUBLISH),
                    RespCommand.SPUBLISH => nameof(RespCommand.SPUBLISH),
                    _ => throw new NotImplementedException()
                };
                return AbortWithWrongNumberOfArguments(cmdName);
            }

            if (cmd == RespCommand.SPUBLISH && clusterSession == null)
            {
                // Print error message
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_CLUSTER_DISABLED);
            }

            // A subscription-mode session may issue PUBLISH (RESP3, or RESP2 until rejected by
            // IsAllowedInSubscriptionMode); the broker can then deliver the message back to this same
            // session reentrantly, handled by the guard in Publish()/PatternPublish().
            // PUBLISH channel message => [*3\r\n$7\r\nPUBLISH\r\n$]7\r\nchannel\r\n$7\r\message\r\n

            var key = parseState.GetArgSliceByRef(0);
            var value = parseState.GetArgSliceByRef(1);

            if (subscribeBroker == null)
            {
                return AbortWithErrorMessage("ERR PUBLISH is disabled, enable it with --pubsub option."u8);
            }

            // Set the publishing thread id only for the duration of the synchronous broadcast, so a
            // reentrant self-delivery (publishing to a self-subscribed channel) can be detected in
            // Publish()/PatternPublish() without paying this cost on the general command path.
            //
            // The publisher does not wait inside the broadcast for a subscriber whose connection is full: it
            // delivers to everyone else first, and then parks for the ones it held back. One that may not wait at
            // all, inside a transaction or a script, sends those past the connection's limit instead.
            int numClients;
            publishingThreadId = Environment.CurrentManagedThreadId;
            try
            {
                numClients = subscribeBroker.PublishNoWait(key, value, ref deferredDeliveries);
            }
            finally
            {
                publishingThreadId = 0;
            }

            var selfDeliveryFailure = reentrantPublishFailure;
            reentrantPublishFailure = null;

            if (DenyBlocking && deferredDeliveries is { Count: > 0 })
                DeliverDeferredWithoutWaiting();

            var holdingDeliveries = deferredDeliveries is { Count: > 0 };
            if (storeWrapper.serverOptions.EnableCluster)
            {
                // Started here, where the spans are still addressable: the forward copies them itself if it
                // has to go async. The session then suspends on it rather than blocking the network thread.
                var forward = storeWrapper.clusterProvider.ClusterPublishAsync(cmd, key.Span, value.Span);
                return holdingDeliveries
                    ? CompleteDeferredPublish(forward, numClients)
                    : CompleteClusterPublish(forward, numClients);
            }

            if (holdingDeliveries)
                return CompleteDeferredPublish(default, numClients);

            // A failed delivery to this session ends it, before the reply is written into the response buffer
            // the failed send released
            selfDeliveryFailure?.Throw();

            WriteInt32(numClients);

            return true;
        }

        private bool NetworkSUBSCRIBE(RespCommand cmd)
        {
            if (parseState.Count < 1)
            {
                var cmdName = cmd switch
                {
                    RespCommand.SUBSCRIBE => nameof(RespCommand.SUBSCRIBE),
                    RespCommand.SSUBSCRIBE => nameof(RespCommand.SSUBSCRIBE),
                    _ => throw new NotImplementedException()
                };
                return AbortWithWrongNumberOfArguments(cmdName);
            }

            if (cmd == RespCommand.SSUBSCRIBE && clusterSession == null)
            {
                // Print error message
                return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_CLUSTER_DISABLED);
            }

            var disabledBroker = subscribeBroker == null;
            var header = cmd switch
            {
                RespCommand.SUBSCRIBE => CmdStrings.subscribe,
                RespCommand.SSUBSCRIBE => CmdStrings.ssubscribe,
                _ => throw new NotImplementedException()
            };

            // SUBSCRIBE|SUBSCRIBE channel1 channel2.. ==> [$9\r\nSUBSCRIBE\r\n$]8\r\nchannel1\r\n$8\r\nchannel2\r\n => Subscribe to channel1 and channel2
            for (var c = 0; c < parseState.Count; c++)
            {
                var key = parseState.GetArgSliceByRef(c);

                if (disabledBroker)
                    continue;

                while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteBulkString(header, ref dcurr, dend))
                    SendAndReset();

                WriteBulkString(key.ReadOnlySpan);

                if (subscribeBroker.Subscribe(key, this))
                    numActiveChannels++;

                while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                    SendAndReset();
            }

            if (disabledBroker)
            {
                while (!RespWriteUtils.TryWriteError("ERR SUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            isSubscriptionSession = true;
            return true;
        }

        private bool NetworkPSUBSCRIBE()
        {
            if (parseState.Count < 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PSUBSCRIBE));
            }

            // PSUBSCRIBE channel1 channel2.. ==> [$10\r\nPSUBSCRIBE\r\n$]8\r\nchannel1\r\n$8\r\nchannel2\r\n => PSubscribe to channel1 and channel2
            var disabledBroker = subscribeBroker == null;
            for (var c = 0; c < parseState.Count; c++)
            {
                var key = parseState.GetArgSliceByRef(c);

                if (disabledBroker)
                    continue;

                while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteBulkString("psubscribe"u8, ref dcurr, dend))
                    SendAndReset();
                WriteBulkString(key.ReadOnlySpan);

                if (subscribeBroker.PatternSubscribe(key, this))
                    numActiveChannels++;

                while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                    SendAndReset();
            }

            if (disabledBroker)
            {
                while (!RespWriteUtils.TryWriteError("ERR SUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            isSubscriptionSession = true;
            return true;
        }

        private bool NetworkUNSUBSCRIBE()
        {
            // UNSUBSCRIBE channel1 channel2.. ==> [$11\r\nUNSUBSCRIBE\r\n]$8\r\nchannel1\r\n$8\r\nchannel2\r\n => Subscribe to channel1 and channel2

            if (parseState.Count == 0)
            {
                if (subscribeBroker == null)
                {
                    return AbortWithErrorMessage("ERR UNSUBSCRIBE is disabled, enable it with --pubsub option."u8);
                }

                var channels = subscribeBroker.ListAllSubscriptions(this);
                foreach (var channel in channels)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteBulkString("unsubscribe"u8, ref dcurr, dend))
                        SendAndReset();

                    WriteBulkString(channel.ReadOnlySpan);

                    if (subscribeBroker.Unsubscribe(channel, this))
                        numActiveChannels--;
                    while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                        SendAndReset();
                }

                if (channels.Count == 0)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteBulkString("unsubscribe"u8, ref dcurr, dend))
                        SendAndReset();

                    WriteNull();

                    while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                        SendAndReset();
                }

                if (numActiveChannels == 0)
                    isSubscriptionSession = false;

                return true;
            }

            for (var c = 0; c < parseState.Count; c++)
            {
                var key = parseState.GetArgSliceByRef(c);

                if (subscribeBroker != null)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteBulkString("unsubscribe"u8, ref dcurr, dend))
                        SendAndReset();
                    WriteBulkString(key.ReadOnlySpan);

                    if (subscribeBroker.Unsubscribe(new ByteArrayWrapper(key), this))
                        numActiveChannels--;

                    while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                        SendAndReset();
                }
            }

            if (subscribeBroker == null)
            {
                while (!RespWriteUtils.TryWriteError("ERR UNSUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
            }

            if (numActiveChannels == 0)
                isSubscriptionSession = false;

            return true;
        }

        private bool NetworkPUNSUBSCRIBE()
        {
            // PUNSUBSCRIBE channel1 channel2.. ==> [$11\r\nPUNSUBSCRIBE\r\n]$8\r\nchannel1\r\n$8\r\nchannel2\r\n => Subscribe to channel1 and channel2

            if (parseState.Count == 0)
            {
                if (subscribeBroker == null)
                {
                    return AbortWithErrorMessage("ERR PUNSUBSCRIBE is disabled, enable it with --pubsub option."u8);
                }

                List<ByteArrayWrapper> channels = subscribeBroker.ListAllPatternSubscriptions(this);
                foreach (var channel in channels)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteBulkString("punsubscribe"u8, ref dcurr, dend))
                        SendAndReset();

                    WriteBulkString(channel.ReadOnlySpan);

                    if (subscribeBroker.PatternUnsubscribe(channel, this))
                        numActiveChannels--;

                    while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                        SendAndReset();
                }

                if (channels.Count == 0)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();

                    while (!RespWriteUtils.TryWriteBulkString("punsubscribe"u8, ref dcurr, dend))
                        SendAndReset();

                    WriteNull();

                    while (!RespWriteUtils.TryWriteInt32(0, ref dcurr, dend))
                        SendAndReset();
                }

                if (numActiveChannels == 0)
                    isSubscriptionSession = false;

                return true;
            }

            for (var c = 0; c < parseState.Count; c++)
            {
                var key = parseState.GetArgSliceByRef(c);

                if (subscribeBroker != null)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteBulkString("punsubscribe"u8, ref dcurr, dend))
                        SendAndReset();
                    WriteBulkString(key.ReadOnlySpan);

                    if (subscribeBroker.PatternUnsubscribe(new ByteArrayWrapper(key), this))
                        numActiveChannels--;

                    while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                        SendAndReset();
                }
            }

            if (subscribeBroker == null)
            {
                while (!RespWriteUtils.TryWriteError("ERR PUNSUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
            }

            if (numActiveChannels == 0)
                isSubscriptionSession = false;

            return true;
        }

        private bool NetworkPUBSUB_CHANNELS()
        {
            if (parseState.Count > 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PUBSUB_CHANNELS));
            }

            if (subscribeBroker is null)
            {
                return AbortWithErrorMessage(string.Format(CmdStrings.GenericPubSubCommandDisabled, "PUBSUB CHANNELS"));
            }

            List<ByteArrayWrapper> channels;
            if (parseState.Count == 0)
                channels = subscribeBroker.GetChannels();
            else
                channels = subscribeBroker.GetChannels(parseState.GetArgSliceByRef(0));

            while (!RespWriteUtils.TryWriteArrayLength(channels.Count, ref dcurr, dend))
                SendAndReset();

            foreach (var channel in channels)
            {
                WriteBulkString(channel.ReadOnlySpan);
            }
            return true;
        }

        private bool NetworkPUBSUB_NUMPAT()
        {
            if (parseState.Count > 0)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.PUBSUB_NUMPAT));
            }

            if (subscribeBroker is null)
            {
                return AbortWithErrorMessage(string.Format(CmdStrings.GenericPubSubCommandDisabled, "PUBSUB NUMPAT"));
            }

            var numPatSubs = subscribeBroker.NumPatternSubscriptions();

            while (!RespWriteUtils.TryWriteInt32(numPatSubs, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkPUBSUB_NUMSUB()
        {
            if (subscribeBroker is null)
            {
                return AbortWithErrorMessage(string.Format(CmdStrings.GenericPubSubCommandDisabled, "PUBSUB NUMSUB"));
            }

            var numChannels = parseState.Count;
            while (!RespWriteUtils.TryWriteArrayLength(numChannels * 2, ref dcurr, dend))
                SendAndReset();

            for (int c = 0; c < numChannels; c++)
            {
                var channel = parseState.GetArgSliceByRef(c);

                WriteBulkString(channel.ReadOnlySpan);
                while (!RespWriteUtils.TryWriteInt32(subscribeBroker.NumSubscriptions(channel), ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }
    }
}