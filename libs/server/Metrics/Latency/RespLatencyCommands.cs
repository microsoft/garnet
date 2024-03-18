// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Garnet.common;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool ProcessLatencyCommands(ReadOnlySpan<byte> bufSpan, int count)
        {
            bool errorFlag = false;
            string errorCmd = string.Empty;

            if (count > 1)
            {
                var param = GetCommand(bufSpan, out bool success1);
                if (!success1) return false;

                if (param.SequenceEqual(CmdStrings.HISTOGRAM) || param.SequenceEqual(CmdStrings.histogram))
                {
                    if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                    {
                        return success;
                    }

                    var ptr = recvBufferPtr + readHead;
                    HashSet<LatencyMetricsType> events = null;
                    bool invalid = false;
                    string invalidEvent = null;
                    if (count > 2)
                    {
                        events = new();
                        for (int i = 0; i < count - 2; i++)
                        {
                            if (!RespReadUtils.ReadStringWithLengthHeader(out var eventStr, ref ptr, recvBufferPtr + bytesRead))
                                return false;
                            try
                            {
                                var eventType = (LatencyMetricsType)Enum.Parse(typeof(LatencyMetricsType), eventStr.ToUpper());
                                events.Add(eventType);
                            }
                            catch
                            {
                                invalid = true;
                                invalidEvent = eventStr;
                            }
                        }
                    }
                    else
                        events = GarnetLatencyMetrics.defaultLatencyTypes.ToHashSet();

                    byte[] response = null;
                    if (invalid)
                        response = Encoding.ASCII.GetBytes($"-ERR Invalid event {invalidEvent}. Try LATENCY HELP\r\n");
                    else
                    {
                        var garnetLatencyMetrics = storeWrapper.monitor?.GlobalMetrics.globalLatencyMetrics;
                        response = Encoding.ASCII.GetBytes(garnetLatencyMetrics != null ? garnetLatencyMetrics.GetRespHistograms(events) : "*0\r\n");
                    }
                    while (!RespWriteUtils.WriteDirect(response, ref dcurr, dend))
                        SendAndReset();
                    readHead = (int)(ptr - recvBufferPtr);
                }
                else if (param.SequenceEqual(CmdStrings.RESET) || param.SequenceEqual(CmdStrings.reset))
                {
                    if (!CheckACLAdminPermissions(bufSpan, count - 2, out bool success))
                    {
                        return success;
                    }

                    if (count < 2)
                    {
                        if (!DrainCommands(bufSpan, count - 2))
                            return false;
                        errorFlag = true;
                        errorCmd = Encoding.ASCII.GetString(param.ToArray());
                    }
                    else
                    {
                        HashSet<LatencyMetricsType> events = null;
                        var ptr = recvBufferPtr + readHead;
                        bool invalid = false;
                        string invalidEvent = null;
                        if (count - 2 > 0)
                        {
                            events = new();
                            for (int i = 0; i < count - 2; i++)
                            {
                                if (!RespReadUtils.ReadStringWithLengthHeader(out var eventStr, ref ptr, recvBufferPtr + bytesRead))
                                    return false;

                                try
                                {
                                    var eventType = (LatencyMetricsType)Enum.Parse(typeof(LatencyMetricsType), eventStr.ToUpper());
                                    events.Add(eventType);
                                }
                                catch
                                {
                                    invalid = true;
                                    invalidEvent = eventStr;
                                }
                            }
                        }
                        else
                            events = GarnetLatencyMetrics.defaultLatencyTypes.ToHashSet();

                        ReadOnlySpan<byte> response = null;
                        if (invalid)
                        {
                            response = new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes($"-ERR Invalid type {invalidEvent}\r\n"));
                        }
                        else
                        {
                            if (storeWrapper.monitor != null)
                            {
                                foreach (var e in events)
                                    storeWrapper.monitor.resetLatencyMetrics[e] = true;
                            }
                            response = CmdStrings.RESP_OK;
                        }
                        while (!RespWriteUtils.WriteResponse(response, ref dcurr, dend))
                            SendAndReset();

                        readHead = (int)(ptr - recvBufferPtr);
                    }
                }
                else if (param.SequenceEqual(CmdStrings.HELP) || param.SequenceEqual(CmdStrings.help))
                {
                    var ptr = recvBufferPtr + readHead;
                    readHead = (int)(ptr - recvBufferPtr);
                    List<string> latencyCommands = RespLatencyHelp.GetLatencyCommands();
                    while (!RespWriteUtils.WriteArrayLength(latencyCommands.Count, ref dcurr, dend))
                        SendAndReset();
                    foreach (String command in latencyCommands)
                    {
                        while (!RespWriteUtils.WriteSimpleString(Encoding.ASCII.GetBytes(command), ref dcurr, dend))
                            SendAndReset();
                    }
                }
                else
                {
                    if (!DrainCommands(bufSpan, count - 2))
                        return false;
                    string paramStr = Encoding.ASCII.GetString(param.ToArray());
                    while (!RespWriteUtils.WriteResponse(new ReadOnlySpan<byte>(Encoding.ASCII.GetBytes("-ERR Unknown subcommand. Try LATENCY HELP.\r\n")), ref dcurr, dend))
                        SendAndReset();
                }
            }
            else
            {
                errorFlag = true;
                errorCmd = "LATENCY";
            }

            if (errorFlag && !string.IsNullOrWhiteSpace(errorCmd))
            {
                var errorMsg = string.Format(CmdStrings.ErrMissingParam, errorCmd);
                var bresp_ERRMISSINGPARAM = Encoding.ASCII.GetBytes(errorMsg);
                bresp_ERRMISSINGPARAM.CopyTo(new Span<byte>(dcurr, bresp_ERRMISSINGPARAM.Length));
                dcurr += bresp_ERRMISSINGPARAM.Length;
            }

            return true;
        }

    }
}