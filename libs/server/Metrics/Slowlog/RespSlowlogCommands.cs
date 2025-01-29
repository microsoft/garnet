// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using HdrHistogram;

namespace Garnet.server
{
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        /// <summary>
        /// Processes SLOWLOG HELP subcommand.
        /// </summary>
        /// <returns>true</returns>
        private bool NetworkSlowLogHelp()
        {
            // No additional arguments
            if (parseState.Count != 0)
            {
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for SLOWLOG HELP.", ref dcurr, dend))
                    SendAndReset();
            }

            List<string> slowLogCommands = RespSlowLogHelp.GetSlowLogCommands();
            while (!RespWriteUtils.TryWriteArrayLength(slowLogCommands.Count, ref dcurr, dend))
                SendAndReset();

            foreach (string command in slowLogCommands)
            {
                while (!RespWriteUtils.TryWriteSimpleString(command, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// Processes SLOWLOG GET subcommand.
        /// </summary>
        private bool NetworkSlowLogGet()
        {
            // No additional arguments
            if (parseState.Count != 0)
            {
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for SLOWLOG GET.", ref dcurr, dend))
                    SendAndReset();
            }

            if (storeWrapper.slowLogContainer == null)
            {
                while (!RespWriteUtils.TryWriteArrayLength(0, ref dcurr, dend))
                    SendAndReset();
                return true;
            }
            storeWrapper.slowLogContainer.GetAllEntries(out var entries);
            while (!RespWriteUtils.TryWriteArrayLength(entries.Count, ref dcurr, dend))
                SendAndReset();
            foreach (var entry in entries)
            {
                while (!RespWriteUtils.TryWriteArrayLength(6, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.TryWriteInt32(entry.Id, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.TryWriteInt32(entry.Timestamp, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.TryWriteInt32(entry.Duration, ref dcurr, dend))
                    SendAndReset();
                if (entry.Arguments == null)
                {
                    while (!RespWriteUtils.TryWriteArrayLength(1, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteAsciiBulkString(entry.Command.ToString(), ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    SessionParseState sps = default;

                    // Deserialize the parse state
                    fixed (byte* ptr = entry.Arguments)
                    {
                        sps.DeserializeFrom(ptr);
                    }
                    while (!RespWriteUtils.TryWriteArrayLength(sps.Count + 1, ref dcurr, dend))
                        SendAndReset();
                    while (!RespWriteUtils.TryWriteAsciiBulkString(entry.Command.ToString(), ref dcurr, dend))
                        SendAndReset();
                    for (int i = 0; i < sps.Count; i++)
                    {
                        while (!RespWriteUtils.TryWriteAsciiBulkString(sps.GetString(i), ref dcurr, dend))
                            SendAndReset();
                    }
                }
                while (!RespWriteUtils.TryWriteAsciiBulkString(entry.ClientIpPort, ref dcurr, dend))
                    SendAndReset();
                while (!RespWriteUtils.TryWriteAsciiBulkString(entry.ClientName, ref dcurr, dend))
                    SendAndReset();
            }
            return true;
        }

        /// <summary>
        /// Processes SLOWLOG LEN subcommand.
        /// </summary>
        private bool NetworkSlowLogLen()
        {
            // No additional arguments
            if (parseState.Count != 0)
            {
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for SLOWLOG LEN.", ref dcurr, dend))
                    SendAndReset();
            }
            while (!RespWriteUtils.TryWriteInt32(storeWrapper.slowLogContainer?.Count ?? 0, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// Processes SLOWLOG RESET subcommand.
        /// </summary>
        private bool NetworkSlowLogReset()
        {
            // No additional arguments
            if (parseState.Count != 0)
            {
                while (!RespWriteUtils.TryWriteError($"ERR Unknown subcommand or wrong number of arguments for SLOWLOG RESET.", ref dcurr, dend))
                    SendAndReset();
            }
            storeWrapper.slowLogContainer?.Clear();
            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void HandleSlowLog(RespCommand cmd)
        {
            long currentTime = Stopwatch.GetTimestamp();

            // Only track valid commands in the slow log
            if (cmd != RespCommand.INVALID)
            {
                long elapsed = currentTime - slowLogStartTime;
                if (elapsed > slowLogThreshold)
                {
                    var entry = new SlowLogEntry
                    {
                        Timestamp = (int)(currentTime / OutputScalingFactor.TimeStampToSeconds),
                        Command = cmd,
                        Duration = (int)(elapsed / OutputScalingFactor.TimeStampToMicroseconds),
                        ClientIpPort = networkSender.RemoteEndpointName,
                        ClientName = clientName,
                    };
                    if (parseState.Count > 0)
                    {
                        // We store the parse state in serialized form, as a byte array
                        int len = parseState.GetSerializedLength();
                        byte[] args = new byte[len];
                        fixed (byte* argsPtr = args)
                        {
                            parseState.CopyTo(argsPtr, len);
                        }
                        entry.Arguments = args;
                    }
                    storeWrapper.slowLogContainer.Add(entry);
                }
            }

            // Update slowLogStartTime so that we can track the next command in the batch
            slowLogStartTime = currentTime;
        }
    }
}