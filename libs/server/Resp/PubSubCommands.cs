// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;

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
        public override unsafe void Publish(ArgSlice key, ArgSlice value)
        {
            try
            {
                networkSender.EnterAndGetResponseObject(out dcurr, out dend);

                WritePushLength(3);

                WriteBulkString("message"u8);
                // Write key and value to the network
                WriteDirectLargeRespString(key.ReadOnlySpan);
                WriteDirectLargeRespString(value.ReadOnlySpan);

                // Flush the publish message for this subscriber
                if (dcurr > networkSender.GetResponseObjectHead())
                    Send(networkSender.GetResponseObjectHead());
            }
            catch
            {
                // Ignore exceptions
            }
            finally
            {
                networkSender.ExitAndReturnResponseObject();
            }
        }

        /// <inheritdoc />
        public override unsafe void PatternPublish(ArgSlice pattern, ArgSlice key, ArgSlice value)
        {
            try
            {
                networkSender.EnterAndGetResponseObject(out dcurr, out dend);

                WritePushLength(4);

                WriteBulkString("pmessage"u8);
                // Write pattern, key, and value to the network
                WriteDirectLargeRespString(pattern.ReadOnlySpan);
                WriteDirectLargeRespString(key.ReadOnlySpan);
                WriteDirectLargeRespString(value.ReadOnlySpan);

                if (dcurr > networkSender.GetResponseObjectHead())
                    Send(networkSender.GetResponseObjectHead());
            }
            catch
            {
                // Ignore exceptions
            }
            finally
            {
                networkSender.ExitAndReturnResponseObject();
            }
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

            Debug.Assert(isSubscriptionSession == false);
            // PUBLISH channel message => [*3\r\n$7\r\nPUBLISH\r\n$]7\r\nchannel\r\n$7\r\message\r\n

            var key = parseState.GetArgSliceByRef(0);
            var value = parseState.GetArgSliceByRef(1);

            if (subscribeBroker == null)
            {
                return AbortWithErrorMessage("ERR PUBLISH is disabled, enable it with --pubsub option."u8);
            }

            var numClients = subscribeBroker.PublishNow(key, value);
            if (storeWrapper.serverOptions.EnableCluster)
            {
                var _key = parseState.GetArgSliceByRef(0).Span;
                var _val = parseState.GetArgSliceByRef(1).Span;
                storeWrapper.clusterProvider.ClusterPublish(cmd, ref _key, ref _val);
            }

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

                WriteArrayLength(3);
                WriteBulkString(header);
                WriteBulkString(key.ReadOnlySpan);

                if (subscribeBroker.Subscribe(key, this))
                    numActiveChannels++;

                WriteInt32(numActiveChannels);
            }

            if (disabledBroker)
            {
                WriteError("ERR SUBSCRIBE is disabled, enable it with --pubsub option."u8);
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

                WriteArrayLength(3);

                WriteBulkString("psubscribe"u8);
                WriteBulkString(key.ReadOnlySpan);

                if (subscribeBroker.PatternSubscribe(key, this))
                    numActiveChannels++;

                WriteInt32(numActiveChannels);
            }

            if (disabledBroker)
            {
                WriteError("ERR SUBSCRIBE is disabled, enable it with --pubsub option."u8);
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
                    WriteArrayLength(3);

                    WriteBulkString("unsubscribe"u8);
                    WriteBulkString(channel.ReadOnlySpan);

                    if (subscribeBroker.Unsubscribe(channel, this))
                        numActiveChannels--;
                    WriteInt32(numActiveChannels);
                }

                if (channels.Count == 0)
                {
                    WriteArrayLength(3);

                    WriteBulkString("unsubscribe"u8);
                    WriteNull();
                    WriteInt32(numActiveChannels);
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
                    WriteArrayLength(3);
                    WriteBulkString("unsubscribe"u8);
                    WriteBulkString(key.ReadOnlySpan);

                    if (subscribeBroker.Unsubscribe(new ByteArrayWrapper(key), this))
                        numActiveChannels--;

                    WriteInt32(numActiveChannels);
                }
            }

            if (subscribeBroker == null)
            {
                WriteError("ERR UNSUBSCRIBE is disabled, enable it with --pubsub option."u8);
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
                    WriteArrayLength(3);

                    WriteBulkString("punsubscribe"u8);
                    WriteBulkString(channel.ReadOnlySpan);

                    if (subscribeBroker.PatternUnsubscribe(channel, this))
                        numActiveChannels--;

                    WriteInt32(numActiveChannels);
                }

                if (channels.Count == 0)
                {
                    WriteArrayLength(3);
                    WriteBulkString("punsubscribe"u8);
                    WriteNull();
                    WriteInt32(0);
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
                    WriteArrayLength(3);
                    WriteBulkString("punsubscribe"u8);
                    WriteBulkString(key.ReadOnlySpan);

                    if (subscribeBroker.PatternUnsubscribe(new ByteArrayWrapper(key), this))
                        numActiveChannels--;

                    WriteInt32(numActiveChannels);
                }
            }

            if (subscribeBroker == null)
            {
                WriteError("ERR PUNSUBSCRIBE is disabled, enable it with --pubsub option."u8);
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

            WriteArrayLength(channels.Count);
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
            WriteInt32(numPatSubs);

            return true;
        }

        private bool NetworkPUBSUB_NUMSUB()
        {
            if (subscribeBroker is null)
            {
                return AbortWithErrorMessage(string.Format(CmdStrings.GenericPubSubCommandDisabled, "PUBSUB NUMSUB"));
            }

            var numChannels = parseState.Count;

            WriteArrayLength(numChannels * 2);
            for (var c = 0; c < numChannels; c++)
            {
                var channel = parseState.GetArgSliceByRef(c);

                WriteBulkString(channel.ReadOnlySpan);
                WriteInt32(subscribeBroker.NumSubscriptions(channel));
            }

            return true;
        }
    }
}