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
        readonly SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> subscribeBroker;
        bool isSubscriptionSession = false;
        int numActiveChannels = 0;

        /// <inheritdoc />
        public override unsafe void Publish(ref byte* keyPtr, int keyLength, ref byte* valPtr, int valLength, ref byte* inputPtr, int sid)
        {
            networkSender.EnterAndGetResponseObject(out dcurr, out dend);
            try
            {
                if (respProtocolVersion == 2)
                {
                    while (!RespWriteUtils.WriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WritePushLength(3, ref dcurr, dend))
                        SendAndReset();
                }
                while (!RespWriteUtils.WriteBulkString("message"u8, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.WriteBulkString(new Span<byte>(keyPtr + sizeof(int), keyLength - sizeof(int)), ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.WriteBulkString(new Span<byte>(valPtr + sizeof(int), valLength - sizeof(int)), ref dcurr, dend))
                    SendAndReset();

                if (dcurr > networkSender.GetResponseObjectHead())
                    Send(networkSender.GetResponseObjectHead());
            }
            finally
            {
                networkSender.ExitAndReturnResponseObject();
            }
        }

        /// <inheritdoc />
        public override unsafe void PrefixPublish(byte* patternPtr, int patternLength, ref byte* keyPtr, int keyLength, ref byte* valPtr, int valLength, ref byte* inputPtr, int sid)
        {
            networkSender.EnterAndGetResponseObject(out dcurr, out dend);
            try
            {
                if (respProtocolVersion == 2)
                {
                    while (!RespWriteUtils.WriteArrayLength(4, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WritePushLength(4, ref dcurr, dend))
                        SendAndReset();
                }
                while (!RespWriteUtils.WriteBulkString("pmessage"u8, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.WriteBulkString(new Span<byte>(patternPtr + sizeof(int), patternLength - sizeof(int)), ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.WriteBulkString(new Span<byte>(keyPtr + sizeof(int), keyLength - sizeof(int)), ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.WriteBulkString(new Span<byte>(valPtr + sizeof(int), valLength - sizeof(int)), ref dcurr, dend))
                    SendAndReset();

                if (dcurr > networkSender.GetResponseObjectHead())
                    Send(networkSender.GetResponseObjectHead());
            }
            finally
            {
                networkSender.ExitAndReturnResponseObject();
            }
        }

        /// <summary>
        /// PUBLISH
        /// </summary>
        private bool NetworkPUBLISH(byte* ptr)
        {
            Debug.Assert(isSubscriptionSession == false);
            // PUBLISH channel message => [*3\r\n$7\r\nPUBLISH\r\n$]7\r\nchannel\r\n$7\r\message\r\n

            byte* keyPtr = null, valPtr = null;
            int ksize = 0, vsize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;
            keyPtr -= sizeof(int);

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, recvBufferPtr + bytesRead))
                return false;
            valPtr -= sizeof(int);

            if (subscribeBroker == null)
            {
                while (!RespWriteUtils.WriteError("ERR PUBLISH is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
                readHead = (int)(ptr - recvBufferPtr);
                return true;
            }

            *(int*)keyPtr = ksize;
            *(int*)valPtr = vsize;

            int numClients = subscribeBroker.PublishNow(keyPtr, valPtr, vsize + sizeof(int), true);
            while (!RespWriteUtils.WriteInteger(numClients, ref dcurr, dend))
                SendAndReset();

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        private bool NetworkSUBSCRIBE(int count, byte* ptr, byte* dend)
        {
            // SUBSCRIBE channel1 channel2.. ==> [$9\r\nSUBSCRIBE\r\n$]8\r\nchannel1\r\n$8\r\nchannel2\r\n => Subscribe to channel1 and channel2

            bool disabledBroker = subscribeBroker == null;
            for (int c = 0; c < count; c++)
            {
                byte* keyPtr = null;
                int ksize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                keyPtr -= sizeof(int);

                if (disabledBroker)
                    continue;

                while (!RespWriteUtils.WriteArrayLength(3, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.WriteBulkString("subscribe"u8, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.WriteBulkString(new Span<byte>(keyPtr + sizeof(int), ksize), ref dcurr, dend))
                    SendAndReset();

                numActiveChannels++;
                while (!RespWriteUtils.WriteInteger(numActiveChannels, ref dcurr, dend))
                    SendAndReset();

                *(int*)keyPtr = ksize;
                _ = subscribeBroker.Subscribe(ref keyPtr, this);
                readHead = (int)(ptr - recvBufferPtr);
            }

            if (disabledBroker)
            {
                while (!RespWriteUtils.WriteError("ERR SUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
                readHead = (int)(ptr - recvBufferPtr);
                return true;
            }

            isSubscriptionSession = true;
            return true;
        }

        private bool NetworkPSUBSCRIBE(int count, byte* ptr, byte* dend)
        {
            // PSUBSCRIBE channel1 channel2.. ==> [$10\r\nPSUBSCRIBE\r\n$]8\r\nchannel1\r\n$8\r\nchannel2\r\n => PSubscribe to channel1 and channel2
            Debug.Assert(subscribeBroker != null);

            bool disabledBroker = subscribeBroker == null;
            for (int c = 0; c < count; c++)
            {
                byte* keyPtr = null;
                int ksize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                keyPtr -= sizeof(int);

                if (disabledBroker)
                    continue;

                while (!RespWriteUtils.WriteArrayLength(3, ref dcurr, dend))
                    SendAndReset();

                while (!RespWriteUtils.WriteBulkString("psubscribe"u8, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.WriteBulkString(new Span<byte>(keyPtr + sizeof(int), ksize), ref dcurr, dend))
                    SendAndReset();

                numActiveChannels++;
                while (!RespWriteUtils.WriteInteger(numActiveChannels, ref dcurr, dend))
                    SendAndReset();

                *(int*)keyPtr = ksize;
                _ = subscribeBroker.PSubscribe(ref keyPtr, this, true);
                readHead = (int)(ptr - recvBufferPtr);
            }

            if (disabledBroker)
            {
                while (!RespWriteUtils.WriteError("ERR SUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
                readHead = (int)(ptr - recvBufferPtr);
                return true;
            }

            isSubscriptionSession = true;
            return true;
        }

        private bool NetworkUNSUBSCRIBE(int count, byte* ptr, byte* dend)
        {
            // UNSUBSCRIBE channel1 channel2.. ==> [$11\r\nUNSUBSCRIBE\r\n]$8\r\nchannel1\r\n$8\r\nchannel2\r\n => Subscribe to channel1 and channel2
            Debug.Assert(subscribeBroker != null);

            if (count == 0)
            {
                if (subscribeBroker == null)
                {
                    while (!RespWriteUtils.WriteError("ERR UNSUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                List<byte[]> channels = subscribeBroker.ListAllSubscriptions(this);
                foreach (var channel in channels)
                {
                    while (!RespWriteUtils.WriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteBulkString("unsubscribe"u8, ref dcurr, dend))
                        SendAndReset();

                    var channelsize = channel.Length - sizeof(int);
                    fixed (byte* channelPtr = &channel[0])
                    {
                        while (!RespWriteUtils.WriteBulkString(new Span<byte>(channelPtr + sizeof(int), channelsize), ref dcurr, dend))
                            SendAndReset();

                        byte* delPtr = channelPtr;
                        if (subscribeBroker.Unsubscribe(delPtr, this))
                            numActiveChannels--;
                        while (!RespWriteUtils.WriteInteger(numActiveChannels, ref dcurr, dend))
                            SendAndReset();
                    }
                }

                if (channels.Count == 0)
                {
                    while (!RespWriteUtils.WriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteBulkString("unsubscribe"u8, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteNull(ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteInteger(numActiveChannels, ref dcurr, dend))
                        SendAndReset();
                }

                Debug.Assert(numActiveChannels == 0);
                if (numActiveChannels == 0)
                    isSubscriptionSession = false;

                return true;
            }

            for (int c = 0; c < count; c++)
            {
                byte* keyPtr = null;
                int ksize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                keyPtr -= sizeof(int);

                if (subscribeBroker != null)
                {
                    while (!RespWriteUtils.WriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteBulkString("unsubscribe"u8, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteBulkString(new Span<byte>(keyPtr + sizeof(int), ksize), ref dcurr, dend))
                        SendAndReset();

                    *(int*)keyPtr = ksize;
                    if (subscribeBroker.Unsubscribe(keyPtr, this))
                        numActiveChannels--;

                    while (!RespWriteUtils.WriteInteger(numActiveChannels, ref dcurr, dend))
                        SendAndReset();
                }
                readHead = (int)(ptr - recvBufferPtr);
            }

            if (subscribeBroker == null)
            {
                while (!RespWriteUtils.WriteError("ERR UNSUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        private bool NetworkPUNSUBSCRIBE(int count, byte* ptr, byte* dend)
        {
            // PUNSUBSCRIBE channel1 channel2.. ==> [$11\r\nPUNSUBSCRIBE\r\n]$8\r\nchannel1\r\n$8\r\nchannel2\r\n => Subscribe to channel1 and channel2
            Debug.Assert(subscribeBroker != null);

            if (count == 0)
            {
                if (subscribeBroker == null)
                {
                    while (!RespWriteUtils.WriteError("ERR PUNSUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                        SendAndReset();
                    return true;
                }

                List<byte[]> channels = subscribeBroker.ListAllPSubscriptions(this);
                foreach (var channel in channels)
                {
                    while (!RespWriteUtils.WriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteBulkString("punsubscribe"u8, ref dcurr, dend))
                        SendAndReset();

                    var channelsize = channel.Length - sizeof(int);
                    fixed (byte* channelPtr = &channel[0])
                    {
                        while (!RespWriteUtils.WriteBulkString(new Span<byte>(channelPtr + sizeof(int), channelsize), ref dcurr, dend))
                            SendAndReset();

                        numActiveChannels--;
                        while (!RespWriteUtils.WriteInteger(numActiveChannels, ref dcurr, dend))
                            SendAndReset();

                        byte* delPtr = channelPtr;
                        subscribeBroker.PUnsubscribe(delPtr, this);
                    }
                }

                if (numActiveChannels == 0)
                    isSubscriptionSession = false;

                return true;
            }

            for (int c = 0; c < count; c++)
            {
                byte* keyPtr = null;
                int ksize = 0;

                if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                keyPtr -= sizeof(int);

                if (subscribeBroker != null)
                {
                    while (!RespWriteUtils.WriteArrayLength(3, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteBulkString("punsubscribe"u8, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.WriteBulkString(new Span<byte>(keyPtr + sizeof(int), ksize), ref dcurr, dend))
                        SendAndReset();

                    numActiveChannels--;
                    while (!RespWriteUtils.WriteInteger(numActiveChannels, ref dcurr, dend))
                        SendAndReset();

                    *(int*)keyPtr = ksize;
                    subscribeBroker.Unsubscribe(keyPtr, this);
                }
                readHead = (int)(ptr - recvBufferPtr);
            }

            if (subscribeBroker == null)
            {
                while (!RespWriteUtils.WriteError("ERR PUNSUBSCRIBE is disabled, enable it with --pubsub option."u8, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }
    }
}