// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SLOWLOG_HELP));
            }

            List<string> slowLogCommands = RespSlowLogHelp.GetSlowLogCommands();
            WriteArrayLength(slowLogCommands.Count);

            foreach (string command in slowLogCommands)
            {
                WriteSimpleString(command);
            }

            return true;
        }

        /// <summary>
        /// Processes SLOWLOG GET subcommand.
        /// </summary>
        private bool NetworkSlowLogGet()
        {
            // Check arguments
            if (parseState.Count > 1)
            {
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SLOWLOG_GET));
            }

            int count = 10;
            if (parseState.Count == 1)
            {
                if (!parseState.TryGetInt(0, out count))
                {
                    return AbortWithErrorMessage(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
                }
            }

            if (storeWrapper.slowLogContainer == null)
            {
                WriteEmptyArray();
                return true;
            }
            var entries = storeWrapper.slowLogContainer.GetEntries(count);
            WriteArrayLength(entries.Count);

            SessionParseState sps = default;
            foreach (var entry in entries)
            {
                WriteArrayLength(6);
                WriteInt32(entry.Id);
                WriteInt32(entry.Timestamp);
                WriteInt32(entry.Duration);
                if (entry.Arguments == null)
                {
                    WriteArrayLength(1);
                    WriteAsciiBulkString(entry.Command.ToString());
                }
                else
                {
                    // Deserialize the parse state
                    fixed (byte* ptr = entry.Arguments)
                    {
                        sps.DeserializeFrom(ptr);
                    }
                    WriteArrayLength(sps.Count + 1);
                    WriteAsciiBulkString(entry.Command.ToString());
                    for (int i = 0; i < sps.Count; i++)
                    {
                        WriteAsciiBulkString(sps.GetString(i));
                    }
                }

                WriteAsciiBulkString(entry.ClientIpPort);
                WriteAsciiBulkString(entry.ClientName);
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
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SLOWLOG_LEN));
            }

            WriteInt32(storeWrapper.slowLogContainer?.Count ?? 0);
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
                return AbortWithWrongNumberOfArguments(nameof(RespCommand.SLOWLOG_RESET));
            }
            storeWrapper.slowLogContainer?.Clear();

            WriteOK();
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