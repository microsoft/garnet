// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Garnet.common;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Processes LATENCY HELP subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkLatencyHelp()
        {
            // No additional arguments
            if (parseState.Count != 0)
            {
                WriteError($"ERR Unknown subcommand or wrong number of arguments for LATENCY HELP.");
            }

            List<string> latencyCommands = RespLatencyHelp.GetLatencyCommands();
            WriteArrayLength(latencyCommands.Count);

            foreach (string command in latencyCommands)
            {
                WriteSimpleString(command);
            }

            return true;
        }

        /// <summary>
        /// Processes LATENCY HISTOGRAM subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkLatencyHistogram()
        {
            HashSet<LatencyMetricsType> events = null;
            bool invalid = false;
            string invalidEvent = null;
            if (parseState.Count >= 1)
            {
                events = new();
                for (int i = 0; i < parseState.Count; i++)
                {
                    if (parseState.TryGetLatencyMetricsType(i, out var eventType))
                    {
                        events.Add(eventType);
                    }
                    else
                    {
                        invalid = true;
                        invalidEvent = parseState.GetString(i);
                    }
                }
            }
            else
            {
                events = [.. GarnetLatencyMetrics.defaultLatencyTypes];
            }

            if (invalid)
            {
                WriteError($"ERR Invalid event {invalidEvent}. Try LATENCY HELP");
            }
            else
            {
                var garnetLatencyMetrics = storeWrapper.monitor?.GlobalMetrics.globalLatencyMetrics;
                string response = garnetLatencyMetrics != null ? garnetLatencyMetrics.GetRespHistograms(events) : "*0\r\n";
                WriteAsciiDirect(response);
            }

            return true;
        }

        /// <summary>
        /// Processes LATENCY RESET subcommand.
        /// </summary>
        /// <returns>true if parsing succeeded correctly, false if not all tokens could be consumed and further processing is necessary.</returns>
        private bool NetworkLatencyReset()
        {
            HashSet<LatencyMetricsType> events = null;
            bool invalid = false;
            string invalidEvent = null;
            if (parseState.Count > 0)
            {
                events = new();
                for (int i = 0; i < parseState.Count; i++)
                {
                    if (parseState.TryGetLatencyMetricsType(i, out var eventType))
                    {
                        events.Add(eventType);
                    }
                    else
                    {
                        invalid = true;
                        invalidEvent = parseState.GetString(i);
                    }
                }
            }
            else
            {
                events = [.. GarnetLatencyMetrics.defaultLatencyTypes];
            }

            if (invalid)
            {
                WriteError($"ERR Invalid type {invalidEvent}");
            }
            else
            {
                if (storeWrapper.monitor != null)
                {
                    foreach (var e in events)
                        storeWrapper.monitor.resetLatencyMetrics[e] = true;
                }

                WriteInt32(events.Count);
            }

            return true;
        }
    }
}