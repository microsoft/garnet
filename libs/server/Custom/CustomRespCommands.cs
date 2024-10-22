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

            var procInput = new CustomProcedureInput(ref parseState, startIdx: startIdx);
            if (txnManager.RunTransactionProc(id, ref procInput, proc, ref output))
            {
                // Write output to wire
                if (output.MemoryOwner != null)
                    SendAndReset(output.MemoryOwner, output.Length);
                else
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
            }
            else
            {
                // Write output to wire
                if (output.MemoryOwner != null)
                    SendAndReset(output.MemoryOwner, output.Length);
                else
                    while (!RespWriteUtils.WriteError($"ERR Transaction failed.", ref dcurr, dend))
                        SendAndReset();
            }
            latencyMetrics?.Stop(LatencyMetricsType.TX_PROC_LAT);

            return true;
        }

        public bool RunTransactionProc(byte id, ref CustomProcedureInput procInput, ref MemoryResult<byte> output)
        {
            var proc = customCommandManagerSession
                .GetCustomTransactionProcedure(id, this, txnManager, scratchBufferManager).Item1;
            return txnManager.RunTransactionProc(id, ref procInput, proc, ref output);

        }

        private void TryCustomProcedure(CustomProcedure proc, int startIdx = 0)
        {
            Debug.Assert(proc != null);

            var output = new MemoryResult<byte>(null, 0);

            var procInput = new CustomProcedureInput(ref parseState, startIdx: startIdx);
            if (proc.Execute(basicGarnetApi, ref procInput, ref output))
            {
                if (output.MemoryOwner != null)
                    SendAndReset(output.MemoryOwner, output.Length);
                else
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
            }
            else
            {
                if (output.MemoryOwner != null)
                    SendAndReset(output.MemoryOwner, output.Length);
                else
                    while (!RespWriteUtils.WriteError($"ERR Command failed.", ref dcurr, dend))
                        SendAndReset();
            }
        }

        public bool InvokeCustomRawStringCommand<TGarnetApi>(ref TGarnetApi storageApi, string cmd, ArgSlice key, ArgSlice[] args, out ArgSlice output)
            where TGarnetApi : IGarnetAdvancedApi
        {
            // TODO: check if cmd exists
            storeWrapper.customCommandManager.Match(new ReadOnlySpan<byte>(Encoding.UTF8.GetBytes(cmd)), out CustomRawStringCommand customCommand);

            var sbKey = key.SpanByte;

            var inputArg = customCommand.expirationTicks > 0 ? DateTimeOffset.UtcNow.Ticks + customCommand.expirationTicks : customCommand.expirationTicks;
            var sessionParseState = new SessionParseState();
            sessionParseState.InitializeWithArguments(args);
            var rawStringInput = new RawStringInput(customCommand.GetRespCommand(), ref sessionParseState, arg1: inputArg);

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
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
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
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                }
                else
                {
                    Debug.Assert(output.Memory == null);
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                }
            }

            return true;
        }

        public bool InvokeCustomObjectCommand<TGarnetApi>(ref TGarnetApi storageApi, string cmd, ArgSlice key, ArgSlice[] args, out ArgSlice output)
            where TGarnetApi : IGarnetAdvancedApi
        {
            output = default;
            // TODO: check if cmd exists
            storeWrapper.customCommandManager.Match(new ReadOnlySpan<byte>(Encoding.UTF8.GetBytes(cmd)), out CustomObjectCommand customObjCommand);

            var keyBytes = key.ToArray();

            // Prepare input
            var header = new RespInputHeader(customObjCommand.GetRespCommand()) { SubId = customObjCommand.subid };
            var sessionParseState = new SessionParseState();
            sessionParseState.InitializeWithArguments(args);
            var input = new ObjectInput(header, ref sessionParseState);

            var _output = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            GarnetStatus status;
            if (customObjCommand.type == CommandType.ReadModifyWrite)
            {
                status = storageApi.RMW_ObjectStore(ref keyBytes, ref input, ref _output);
                Debug.Assert(!_output.spanByteAndMemory.IsSpanByte);

                switch (status)
                {
                    case GarnetStatus.WRONGTYPE:
                        output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_ERR_WRONG_TYPE);
                        break;
                    default:
                        if (_output.spanByteAndMemory.Memory != null)
                            output = scratchBufferManager.FormatScratch(0, _output.spanByteAndMemory.AsReadOnlySpan());
                        else
                            output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_OK);
                        break;
                }
            }
            else
            {
                status = storageApi.Read_ObjectStore(ref keyBytes, ref input, ref _output);
                Debug.Assert(!_output.spanByteAndMemory.IsSpanByte);

                switch (status)
                {
                    case GarnetStatus.OK:
                        if (_output.spanByteAndMemory.Memory != null)
                            output = scratchBufferManager.FormatScratch(0, _output.spanByteAndMemory.AsReadOnlySpan());
                        else
                            output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_OK);
                        break;
                    case GarnetStatus.NOTFOUND:
                        Debug.Assert(_output.spanByteAndMemory.Memory == null);
                        output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_ERRNOTFOUND);
                        break;
                    case GarnetStatus.WRONGTYPE:
                        output = scratchBufferManager.CreateArgSlice(CmdStrings.RESP_ERR_WRONG_TYPE);
                        break;
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

            var output = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            GarnetStatus status;

            if (type == CommandType.ReadModifyWrite)
            {
                status = storageApi.RMW_ObjectStore(ref keyBytes, ref input, ref output);
                Debug.Assert(!output.spanByteAndMemory.IsSpanByte);

                switch (status)
                {
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                    default:
                        if (output.spanByteAndMemory.Memory != null)
                            SendAndReset(output.spanByteAndMemory.Memory, output.spanByteAndMemory.Length);
                        else
                            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                                SendAndReset();
                        break;
                }
            }
            else
            {
                status = storageApi.Read_ObjectStore(ref keyBytes, ref input, ref output);
                Debug.Assert(!output.spanByteAndMemory.IsSpanByte);

                switch (status)
                {
                    case GarnetStatus.OK:
                        if (output.spanByteAndMemory.Memory != null)
                            SendAndReset(output.spanByteAndMemory.Memory, output.spanByteAndMemory.Length);
                        else
                            while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                                SendAndReset();
                        break;
                    case GarnetStatus.NOTFOUND:
                        Debug.Assert(output.spanByteAndMemory.Memory == null);
                        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                        break;
                    case GarnetStatus.WRONGTYPE:
                        while (!RespWriteUtils.WriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                            SendAndReset();
                        break;
                }
            }

            return true;
        }
    }
}