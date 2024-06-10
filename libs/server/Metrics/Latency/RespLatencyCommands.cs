// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using Garnet.common;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Processes LATENCY HELP subcommand.
        /// </summary>
        /// <param name="count">The number of arguments remaining in buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkLatencyHelp(int count)
        {
            // No additional arguments
            if (count != 0)
            {
                if (!DrainCommands(count))
                    return false;

                while (!RespWriteUtils.WriteError($"ERR Unknown subcommand or wrong number of arguments for LATENCY HELP.", ref dcurr, dend))
                    SendAndReset();
            }

            var ptr = recvBufferPtr + readHead;
            readHead = (int)(ptr - recvBufferPtr);
            List<string> latencyCommands = RespLatencyHelp.GetLatencyCommands();
            while (!RespWriteUtils.WriteArrayLength(latencyCommands.Count, ref dcurr, dend))
                SendAndReset();

            foreach (string command in latencyCommands)
            {
                while (!RespWriteUtils.WriteSimpleString(command, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Processes LATENCY HISTOGRAM subcommand.
        /// </summary>
        /// <param name="count">The number of arguments remaining in buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkLatencyHistogram(int count)
        {
            var ptr = recvBufferPtr + readHead;
            HashSet<LatencyMetricsType> events = null;
            bool invalid = false;
            string invalidEvent = null;
            if (count >= 1)
            {
                events = new();
                for (int i = 0; i < count; i++)
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var eventStr, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    if (Enum.TryParse(eventStr, ignoreCase: true, out LatencyMetricsType eventType))
                    {
                        events.Add(eventType);
                    }
                    else
                    {
                        invalid = true;
                        invalidEvent = eventStr;
                    }
                }
            }
            else
            {
                events = GarnetLatencyMetrics.defaultLatencyTypes.ToHashSet();
            }

            if (invalid)
            {
                while (!RespWriteUtils.WriteError($"ERR Invalid event {invalidEvent}. Try LATENCY HELP", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                var garnetLatencyMetrics = storeWrapper.monitor?.GlobalMetrics.globalLatencyMetrics;
                string response = garnetLatencyMetrics != null ? garnetLatencyMetrics.GetRespHistograms(events) : "*0\r\n";
                while (!RespWriteUtils.WriteAsciiDirect(response, ref dcurr, dend))
                    SendAndReset();
            }

            readHead = (int)(ptr - recvBufferPtr);

            return true;
        }

        /// <summary>
        /// Processes LATENCY RESET subcommand.
        /// </summary>
        /// <param name="count">The number of arguments remaining in buffer</param>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkLatencyReset(int count)
        {
            HashSet<LatencyMetricsType> events = null;
            var ptr = recvBufferPtr + readHead;
            bool invalid = false;
            string invalidEvent = null;
            if (count > 0)
            {
                events = new();
                for (int i = 0; i < count; i++)
                {
                    if (!RespReadUtils.ReadStringWithLengthHeader(out var eventStr, ref ptr, recvBufferPtr + bytesRead))
                        return false;

                    if (Enum.TryParse(eventStr, ignoreCase: true, out LatencyMetricsType eventType))
                    {
                        events.Add(eventType);
                    }
                    else
                    {
                        invalid = true;
                        invalidEvent = eventStr;
                    }
                }
            }
            else
            {
                events = GarnetLatencyMetrics.defaultLatencyTypes.ToHashSet();
            }

            if (invalid)
            {
                while (!RespWriteUtils.WriteError($"ERR Invalid type {invalidEvent}", ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                if (storeWrapper.monitor != null)
                {
                    foreach (var e in events)
                        storeWrapper.monitor.resetLatencyMetrics[e] = true;
                }

                while (!RespWriteUtils.WriteInteger(events.Count, ref dcurr, dend))
                    SendAndReset();
            }

            readHead = (int)(ptr - recvBufferPtr);

            return true;
        }
    }
}