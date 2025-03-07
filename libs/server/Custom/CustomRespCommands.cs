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

            latencyMetrics?.Start(LatencyMetricsType.TX_PROC_LAT);

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
                // Write output to wire
                if (output.MemoryOwner != null)
                    SendAndReset(output.MemoryOwner, output.Length);
                else
                    while (!RespWriteUtils.TryWriteError($"ERR Transaction failed.", ref dcurr, dend))
                        SendAndReset();
            }
            latencyMetrics?.Stop(LatencyMetricsType.TX_PROC_LAT);

            return true;
        }

        public bool RunTransactionProc(byte id, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var proc = customCommandManagerSession
                .GetCustomTransactionProcedure(id, this, txnManager, scratchBufferManager, out _);
            return txnManager.RunTransactionProc(id, ref procInput, proc, ref output);
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
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;

            var inputArg = expirationTicks > 0 ? DateTimeOffset.UtcNow.Ticks + expirationTicks : expirationTicks;
            var input = new RawStringInput(cmd, ref parseState, startIdx: 1, arg1: inputArg);

            var output = new SpanByteAndMemory(null);
            GarnetStatus status;
            if (type == CommandType.ReadModifyWrite)
            {
                status = storageApi.RMW_MainStore(ref sbKey, ref input, ref output);
                Debug.Assert(!output.IsSpanByte);

                if (output.Memory != null)
                    SendAndReset(output.Memory, output.Length);
                else
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
            }
            else
            {
                status = storageApi.Read_MainStore(ref sbKey, ref input, ref output);
                Debug.Assert(!output.IsSpanByte);

                if (status == GarnetStatus.OK)
                {
                    if (output.Memory != null)
                        SendAndReset(output.Memory, output.Length);
                    else
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                }
                else
                {
                    Debug.Assert(output.Memory == null);
                    while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
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
            var keyBytes = parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

            // Prepare input

            var header = new RespInputHeader(objType) { SubId = subid };
            var input = new ObjectInput(header, ref parseState, startIdx: 1);

            var output = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(null) };

            GarnetStatus status;

            if (type == CommandType.ReadModifyWrite)
            {
                status = storageApi.RMW_ObjectStore(ref keyBytes, ref input, ref output);
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
                status = storageApi.Read_ObjectStore(ref keyBytes, ref input, ref output);
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
                        while (!RespWriteUtils.TryWriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
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
        public bool InvokeCustomRawStringCommand<TGarnetApi>(ref TGarnetApi storageApi, CustomRawStringCommand customCommand, ArgSlice key, ArgSlice[] args, out ArgSlice output)
            where TGarnetApi : IGarnetAdvancedApi
        {
            ArgumentNullException.ThrowIfNull(customCommand);

            var sbKey = key.SpanByte;
            var inputArg = customCommand.expirationTicks > 0 ? DateTimeOffset.UtcNow.Ticks + customCommand.expirationTicks : customCommand.expirationTicks;
            customCommandParseState.InitializeWithArguments(args);
            var cmd = customCommandManagerSession.GetCustomRespCommand(customCommand.id);
            var rawStringInput = new RawStringInput(cmd, ref customCommandParseState, arg1: inputArg);

            var _output = new SpanByteAndMemory(null);
            GarnetStatus status;
            if (customCommand.type == CommandType.ReadModifyWrite)
            {
                status = storageApi.RMW_MainStore(ref sbKey, ref rawStringInput, ref _output);
                Debug.Assert(!_output.IsSpanByte);

                if (_output.Memory != null)
                {
                    output = scratchBufferManager.FormatScratch(0, _output.AsReadOnlySpan());
                    _output.Memory.Dispose();
                }
                else
                {
                    output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_OK);
                }
            }
            else
            {
                status = storageApi.Read_MainStore(ref sbKey, ref rawStringInput, ref _output);
                Debug.Assert(!_output.IsSpanByte);

                if (status == GarnetStatus.OK)
                {
                    if (_output.Memory != null)
                    {
                        output = scratchBufferManager.FormatScratch(0, _output.AsReadOnlySpan());
                        _output.Memory.Dispose();
                    }
                    else
                    {
                        output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_OK);
                    }
                }
                else
                {
                    Debug.Assert(_output.Memory == null);
                    output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_ERRNOTFOUND);
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
        public bool InvokeCustomObjectCommand<TGarnetApi>(ref TGarnetApi storageApi, CustomObjectCommand customObjCommand, ArgSlice key, ArgSlice[] args, out ArgSlice output)
            where TGarnetApi : IGarnetAdvancedApi
        {
            ArgumentNullException.ThrowIfNull(customObjCommand);

            output = default;

            var keyBytes = key.ToArray();

            // Prepare input
            var type = customCommandManagerSession.GetCustomGarnetObjectType(customObjCommand.id);
            var header = new RespInputHeader(type) { SubId = customObjCommand.subid };
            customCommandParseState.InitializeWithArguments(args);
            var input = new ObjectInput(header, ref customCommandParseState);

            var _output = new GarnetObjectStoreOutput { SpanByteAndMemory = new SpanByteAndMemory(null) };
            GarnetStatus status;
            if (customObjCommand.type == CommandType.ReadModifyWrite)
            {
                status = storageApi.RMW_ObjectStore(ref keyBytes, ref input, ref _output);
                Debug.Assert(!_output.SpanByteAndMemory.IsSpanByte);

                switch (status)
                {
                    case GarnetStatus.WRONGTYPE:
                        output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_ERR_WRONG_TYPE);
                        break;
                    default:
                        if (_output.SpanByteAndMemory.Memory != null)
                            output = scratchBufferManager.FormatScratch(0, _output.SpanByteAndMemory.AsReadOnlySpan());
                        else
                            output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_OK);
                        break;
                }
            }
            else
            {
                status = storageApi.Read_ObjectStore(ref keyBytes, ref input, ref _output);
                Debug.Assert(!_output.SpanByteAndMemory.IsSpanByte);

                switch (status)
                {
                    case GarnetStatus.OK:
                        if (_output.SpanByteAndMemory.Memory != null)
                            output = scratchBufferManager.FormatScratch(0, _output.SpanByteAndMemory.AsReadOnlySpan());
                        else
                            output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_OK);
                        break;
                    case GarnetStatus.NOTFOUND:
                        Debug.Assert(_output.SpanByteAndMemory.Memory == null);
                        output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_ERRNOTFOUND);
                        break;
                    case GarnetStatus.WRONGTYPE:
                        output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_ERR_WRONG_TYPE);
                        break;
                }
            }

            return true;
        }
    }
}