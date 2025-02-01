﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        public override unsafe void Publish(ArgSlice key, ArgSlice value, int sid)
        {
            try
            {
                networkSender.EnterAndGetResponseObject(out dcurr, out dend);
                if (respProtocolVersion == 2)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWritePushLength(3, ref dcurr, dend))
                        SendAndReset();
                }
                while (!RespWriteUtils.TryWriteBulkString("message"u8, ref dcurr, dend))
                    SendAndReset();

                // Write key and value to the network
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

        /// <inheritdoc />
        public override unsafe void PatternPublish(ArgSlice pattern, ArgSlice key, ArgSlice value, int sid)
        {
            try
            {
                networkSender.EnterAndGetResponseObject(out dcurr, out dend);
                if (respProtocolVersion == 2)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(4, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.TryWritePushLength(4, ref dcurr, dend))
                        SendAndReset();
                }
                while (!RespWriteUtils.TryWriteBulkString("pmessage"u8, ref dcurr, dend))
                    SendAndReset();

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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_CLUSTER_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            Debug.Assert(isSubscriptionSession == false);
            // PUBLISH channel message => [*3\r\n$7\r\nPUBLISH\r\n$]7\r\nchannel\r\n$7\r\message\r\n

            var key = parseState.GetArgSliceByRef(0);
            var value = parseState.GetArgSliceByRef(1);

            if (subscribeBroker == null)
            {
                while (!RespWriteUtils.TryWriteError("ERR PUBLISH is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var numClients = subscribeBroker.PublishNow(key, value);
            if (storeWrapper.serverOptions.EnableCluster)
            {
                var _key = parseState.GetArgSliceByRef(0).Span;
                var _val = parseState.GetArgSliceByRef(1).Span;
                storeWrapper.clusterProvider.ClusterPublish(cmd, ref _key, ref _val);
            }

            while (!RespWriteUtils.TryWriteInt32(numClients, ref dcurr, dend))
                SendAndReset();

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
                while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_GENERIC_CLUSTER_DISABLED, ref dcurr, dend))
                    SendAndReset();
                return true;
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

                while (!RespWriteUtils.TryWriteBulkString(key.ReadOnlySpan, ref dcurr, dend))
                    SendAndReset();

                numActiveChannels++;
                while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                    SendAndReset();

                _ = subscribeBroker.Subscribe(key, this);
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
                var key = parseState.GetArgSliceByRef(c).SpanByte;
                var keyPtr = key.ToPointer() - sizeof(int);
                var kSize = key.Length;

                if (disabledBroker)
                    continue;

                while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.TryWriteBulkString("psubscribe"u8, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.TryWriteBulkString(new Span<byte>(keyPtr + sizeof(int), kSize), ref dcurr, dend))
                    SendAndReset();

                numActiveChannels++;
                while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                    SendAndReset();

                *(int*)keyPtr = kSize;
                _ = subscribeBroker.PatternSubscribe(ref keyPtr, this);
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
                    while (!RespWriteUtils.TryWriteError("ERR UNSUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                var channels = subscribeBroker.ListAllSubscriptions(this);
                foreach (var channel in channels)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteBulkString("unsubscribe"u8, ref dcurr, dend))
                        SendAndReset();

                    fixed (byte* channelPtr = channel)
                    {
                        var channelSlice = new ArgSlice(channelPtr, channel.Length);
                        while (!RespWriteUtils.TryWriteBulkString(channelSlice.ReadOnlySpan, ref dcurr, dend))
                            SendAndReset();

                        if (subscribeBroker.Unsubscribe(channelSlice,this))
                            numActiveChannels--;
                        while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                            SendAndReset();
                    }
                }

                if (channels.Count == 0)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteBulkString("unsubscribe"u8, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteNull(ref dcurr, dend))
                        SendAndReset();
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
                    while (!RespWriteUtils.TryWriteBulkString(key.ReadOnlySpan, ref dcurr, dend))
                        SendAndReset();

                    if (subscribeBroker.Unsubscribe(key, this))
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
            return true;
        }

        private bool NetworkPUNSUBSCRIBE()
        {
            // PUNSUBSCRIBE channel1 channel2.. ==> [$11\r\nPUNSUBSCRIBE\r\n]$8\r\nchannel1\r\n$8\r\nchannel2\r\n => Subscribe to channel1 and channel2

            if (parseState.Count == 0)
            {
                if (subscribeBroker == null)
                {
                    while (!RespWriteUtils.TryWriteError("ERR PUNSUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                List<byte[]> channels = subscribeBroker.ListAllPatternSubscriptions(this);
                foreach (var channel in channels)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteBulkString("punsubscribe"u8, ref dcurr, dend))
                        SendAndReset();

                    fixed (byte* channelPtr = channel)
                    {
                        var channelSlice = new ArgSlice(channelPtr, channel.Length);
                        while (!RespWriteUtils.TryWriteBulkString(channelSlice.ReadOnlySpan, ref dcurr, dend))
                            SendAndReset();

                        numActiveChannels--;
                        while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                            SendAndReset();

                        subscribeBroker.PatternUnsubscribe(channelSlice, this);
                    }
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
                    while (!RespWriteUtils.TryWriteBulkString(key.ReadOnlySpan, ref dcurr, dend))
                        SendAndReset();

                    numActiveChannels--;
                    while (!RespWriteUtils.TryWriteInt32(numActiveChannels, ref dcurr, dend))
                        SendAndReset();

                    subscribeBroker.Unsubscribe(key, this);
                }
            }

            if (subscribeBroker == null)
            {
                while (!RespWriteUtils.TryWriteError("ERR PUNSUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
            }
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
                while (!RespWriteUtils.TryWriteError(string.Format(CmdStrings.GenericPubSubCommandDisabled, "PUBSUB CHANNELS"), ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var input = new ObjectInput()
            {
                parseState = parseState
            };
            var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            subscribeBroker.Channels(ref input, ref output);

            if (!output.IsSpanByte)
                SendAndReset(output.Memory, output.Length);
            else
                dcurr += output.Length;

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
                while (!RespWriteUtils.TryWriteError(string.Format(CmdStrings.GenericPubSubCommandDisabled, "PUBSUB NUMPAT"), ref dcurr, dend))
                    SendAndReset();
                return true;
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
                while (!RespWriteUtils.TryWriteError(string.Format(CmdStrings.GenericPubSubCommandDisabled, "PUBSUB NUMSUB"), ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var input = new ObjectInput
            {
                parseState = parseState
            };
            var output = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            subscribeBroker.NumSubscriptions(ref input, ref output);

            if (!output.IsSpanByte)
                SendAndReset(output.Memory, output.Length);
            else
                dcurr += output.Length;

            return true;
        }
    }
}