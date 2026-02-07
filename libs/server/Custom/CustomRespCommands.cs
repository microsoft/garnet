// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{

    /// <summary>
    /// Server session for RESP protocol - basic commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool TryTransactionProc(byte id, CustomTransactionProcedure proc, int startIdx = 0)
        {
            // Define output
            var output = new MemoryResult<byte>(null, 0);

            // Run procedure
            Debug.Assert(txnManager.state == TxnState.None);

            LatencyMetrics?.Start(LatencyMetricsType.TX_PROC_LAT);


            sessionMetrics?.incr_total_transaction_commands_received();

            var procInput = new CustomProcedureInput(ref parseState, startIdx: startIdx, respVersion: respProtocolVersion);
            if (txnManager.RunTransactionProc(id, ref procInput, proc, ref output))
            {
                // Write output to wire
                if (output.MemoryOwner != null)
                    SendAndReset(output.MemoryOwner, output.Length);
                else
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
            }
            else
            {
                sessionMetrics?.incr_total_transaction_execution_failed();
                // Write output to wire
                if (output.MemoryOwner != null)
                    SendAndReset(output.MemoryOwner, output.Length);
                else
                    while (!RespWriteUtils.TryWriteError($"ERR Transaction failed.", ref dcurr, dend))
                        SendAndReset();
            }
            LatencyMetrics?.Stop(LatencyMetricsType.TX_PROC_LAT);

            return true;
        }

        public bool RunTransactionProc(byte id, ref CustomProcedureInput procInput, ref MemoryResult<byte> output, bool isRecovering = false)
        {
            var proc = customCommandManagerSession
                .GetCustomTransactionProcedure(id, this, txnManager, scratchBufferAllocator, out _);
            return txnManager.RunTransactionProc(id, ref procInput, proc, ref output, isRecovering);
        }

        private void TryCustomProcedure(CustomProcedure proc, int startIdx = 0)
        {
            Debug.Assert(proc != null);

            var output = new MemoryResult<byte>(null, 0);

            var procInput = new CustomProcedureInput(ref parseState, startIdx: startIdx, respVersion: respProtocolVersion);
            if (proc.Execute(basicGarnetApi, ref procInput, ref output))
            {
                if (output.MemoryOwner != null)
                    SendAndReset(output.MemoryOwner, output.Length);
                else
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
            }
            else
            {
                if (output.MemoryOwner != null)
                    SendAndReset(output.MemoryOwner, output.Length);
                else
                    while (!RespWriteUtils.TryWriteError($"ERR Command failed.", ref dcurr, dend))
                        SendAndReset();
            }
        }

        /// <summary>
        /// Custom command
        /// </summary>
        private bool TryCustomRawStringCommand<TGarnetApi>(RespCommand cmd, long expirationTicks, CommandType type, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetAdvancedApi
        {
            var key = parseState.GetArgSliceByRef(0);

            var inputArg = expirationTicks > 0 ? DateTimeOffset.UtcNow.Ticks + expirationTicks : expirationTicks;
            var input = new StringInput(cmd, ref parseState, startIdx: 1, arg1: inputArg);

            var output = new StringOutput();
            if (type == CommandType.ReadModifyWrite)
            {
                _ = storageApi.RMW_MainStore(key, ref input, ref output);
                Debug.Assert(!output.SpanByteAndMemory.IsSpanByte);

                if (output.SpanByteAndMemory.Memory != null)
                    SendAndReset(output.SpanByteAndMemory.Memory, output.SpanByteAndMemory.Length);
                else
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
            }
            else
            {
                var status = storageApi.Read_MainStore(key, ref input, ref output);
                Debug.Assert(!output.SpanByteAndMemory.IsSpanByte);

                if (status == GarnetStatus.OK)
                {
                    if (output.SpanByteAndMemory.Memory != null)
                        SendAndReset(output.SpanByteAndMemory.Memory, output.SpanByteAndMemory.Length);
                    else
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                }
                else
                {
                    Debug.Assert(output.SpanByteAndMemory.Memory == null);
                    WriteNull();
                }
            }

            return true;
        }

        /// <summary>
        /// Custom object command
        /// </summary>
        private bool TryCustomObjectCommand<TGarnetApi>(GarnetObjectType objType, byte subid, CommandType type, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetAdvancedApi
        {
            var key = parseState.GetArgSliceByRef(0);

            // Prepare input

            var header = new RespInputHeader(objType) { SubId = subid };
            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            var output = new ObjectOutput();

            GarnetStatus status;

            if (type == CommandType.ReadModifyWrite)
            {
                status = storageApi.RMW_ObjectStore(key, ref input, ref output);
                Debug.Assert(!output.SpanByteAndMemory.IsSpanByte);

                switch (status)
                {
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                    default:
                        if (output.SpanByteAndMemory.Memory != null)
                            SendAndReset(output.SpanByteAndMemory.Memory, output.SpanByteAndMemory.Length);
                        else
                            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                                SendAndReset();
                        break;
                }
            }
            else
            {
                status = storageApi.Read_ObjectStore(key, ref input, ref output);
                Debug.Assert(!output.SpanByteAndMemory.IsSpanByte);

                switch (status)
                {
                    case GarnetStatus.OK:
                        if (output.SpanByteAndMemory.Memory != null)
                            SendAndReset(output.SpanByteAndMemory.Memory, output.SpanByteAndMemory.Length);
                        else
                            while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                                SendAndReset();
                        break;
                    case GarnetStatus.NOTFOUND:
                        Debug.Assert(output.SpanByteAndMemory.Memory == null);
                        WriteNull();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            return true;
        }

        /// <summary>Parse custom raw string command</summary>
        /// <param name="cmd">Command name</param>
        /// <param name="customCommand">Parsed raw string command</param>
        /// <returns>True if command found, false otherwise</returns>
        public bool ParseCustomRawStringCommand(string cmd, out CustomRawStringCommand customCommand) =>
            customCommandManagerSession.Match(new ReadOnlySpan<byte>(Encoding.UTF8.GetBytes(cmd)), out customCommand);

        /// <summary>Parse custom object command</summary>
        /// <param name="cmd">Command name</param>
        /// <param name="customObjCommand">Parsed object command</param>
        /// <returns>True if command found, false othrewise</returns>
        public bool ParseCustomObjectCommand(string cmd, out CustomObjectCommand customObjCommand) =>
            customCommandManagerSession.Match(new ReadOnlySpan<byte>(Encoding.UTF8.GetBytes(cmd)), out customObjCommand);

        /// <summary>Execute a specific custom raw string command</summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <param name="customCommand">Custom raw string command to execute</param>
        /// <param name="key">Key param</param>
        /// <param name="args">Args to the command</param>
        /// <param name="output">Output from the command</param>
        /// <returns>True if successful</returns>
        public bool InvokeCustomRawStringCommand<TGarnetApi>(ref TGarnetApi storageApi, CustomRawStringCommand customCommand, PinnedSpanByte key, PinnedSpanByte[] args, out PinnedSpanByte output)
            where TGarnetApi : IGarnetAdvancedApi
        {
            ArgumentNullException.ThrowIfNull(customCommand);

            var inputArg = customCommand.expirationTicks > 0 ? DateTimeOffset.UtcNow.Ticks + customCommand.expirationTicks : customCommand.expirationTicks;
            customCommandParseState.InitializeWithArguments(args);
            var cmd = customCommandManagerSession.GetCustomRespCommand(customCommand.id);
            var stringInput = new StringInput(cmd, ref customCommandParseState, arg1: inputArg);

            var _output = new StringOutput();
            if (customCommand.type == CommandType.ReadModifyWrite)
            {
                _ = storageApi.RMW_MainStore(key, ref stringInput, ref _output);
                Debug.Assert(!_output.SpanByteAndMemory.IsSpanByte);

                if (_output.SpanByteAndMemory.Memory != null)
                {
                    output = scratchBufferBuilder.FormatScratch(0, _output.SpanByteAndMemory.ReadOnlySpan);
                    _output.SpanByteAndMemory.Memory.Dispose();
                }
                else
                {
                    output = scratchBufferBuilder.CreateArgSlice(CmdStrings.RESP_OK);
                }
            }
            else
            {
                var status = storageApi.Read_MainStore(key, ref stringInput, ref _output);
                Debug.Assert(!_output.SpanByteAndMemory.IsSpanByte);

                if (status == GarnetStatus.OK)
                {
                    if (_output.SpanByteAndMemory.Memory != null)
                    {
                        output = scratchBufferBuilder.FormatScratch(0, _output.SpanByteAndMemory.ReadOnlySpan);
                        _output.SpanByteAndMemory.Memory.Dispose();
                    }
                    else
                    {
                        output = scratchBufferBuilder.CreateArgSlice(CmdStrings.RESP_OK);
                    }
                }
                else
                {
                    Debug.Assert(_output.SpanByteAndMemory.Memory == null);
                    if (respProtocolVersion >= 3)
                        output = scratchBufferBuilder.CreateArgSlice(CmdStrings.RESP3_NULL_REPLY);
                    else
                        output = scratchBufferBuilder.CreateArgSlice(CmdStrings.RESP_ERRNOTFOUND);
                }
            }

            return true;
        }

        /// <summary>Execute a specific custom object command</summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="storageApi"></param>
        /// <param name="customObjCommand">Custom object command to execute</param>
        /// <param name="key">Key parameter</param>
        /// <param name="args">Args to the command</param>
        /// <param name="output">Output from the command</param>
        /// <returns>True if successful</returns>
        public bool InvokeCustomObjectCommand<TGarnetApi>(ref TGarnetApi storageApi, CustomObjectCommand customObjCommand, PinnedSpanByte key, PinnedSpanByte[] args, out PinnedSpanByte output)
            where TGarnetApi : IGarnetAdvancedApi
        {
            ArgumentNullException.ThrowIfNull(customObjCommand);

            output = default;

            // Prepare input
            var type = customCommandManagerSession.GetCustomGarnetObjectType(customObjCommand.id);
            var header = new RespInputHeader(type) { SubId = customObjCommand.subid };
            customCommandParseState.InitializeWithArguments(args);
            var input = new ObjectInput(header, ref customCommandParseState);

            var _output = new ObjectOutput();
            GarnetStatus status;
            if (customObjCommand.type == CommandType.ReadModifyWrite)
            {
                status = storageApi.RMW_ObjectStore(key, ref input, ref _output);
                Debug.Assert(!_output.SpanByteAndMemory.IsSpanByte);

                switch (status)
                {
                    case GarnetStatus.WRONGTYPE:
                        output = scratchBufferBuilder.CreateArgSlice(CmdStrings.RESP_ERR_WRONG_TYPE);
                        break;
                    default:
                        if (_output.SpanByteAndMemory.Memory != null)
                            output = scratchBufferBuilder.FormatScratch(0, _output.SpanByteAndMemory.ReadOnlySpan);
                        else
                            output = scratchBufferBuilder.CreateArgSlice(CmdStrings.RESP_OK);
                        break;
                }
            }
            else
            {
                status = storageApi.Read_ObjectStore(key, ref input, ref _output);
                Debug.Assert(!_output.SpanByteAndMemory.IsSpanByte);

                switch (status)
                {
                    case GarnetStatus.OK:
                        if (_output.SpanByteAndMemory.Memory != null)
                            output = scratchBufferBuilder.FormatScratch(0, _output.SpanByteAndMemory.ReadOnlySpan);
                        else
                            output = scratchBufferBuilder.CreateArgSlice(CmdStrings.RESP_OK);
                        break;
                    case GarnetStatus.NOTFOUND:
                        Debug.Assert(_output.SpanByteAndMemory.Memory == null);
                        if (respProtocolVersion >= 3)
                            output = scratchBufferBuilder.CreateArgSlice(CmdStrings.RESP3_NULL_REPLY);
                        else
                            output = scratchBufferBuilder.CreateArgSlice(CmdStrings.RESP_ERRNOTFOUND);
                        break;
                    case GarnetStatus.WRONGTYPE:
                        output = scratchBufferBuilder.CreateArgSlice(CmdStrings.RESP_ERR_WRONG_TYPE);
                        break;
                }
            }

            return true;
        }
    }
}