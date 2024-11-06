// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{

    /// <summary>
    /// Server session for RESP protocol - basic commands are in this file
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        private bool TryTransactionProc(byte id, byte* ptr, byte* end, CustomTransactionProcedure proc)
        {
            // Define output
            var output = new MemoryResult<byte>(null, 0);

            // Run procedure
            Debug.Assert(txnManager.state == TxnState.None);

            latencyMetrics?.Start(LatencyMetricsType.TX_PROC_LAT);
            var input = new ArgSlice(ptr, (int)(end - ptr));

            if (txnManager.RunTransactionProc(id, input, proc, ref output))
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

        public bool RunTransactionProc(byte id, ArgSlice input, ref MemoryResult<byte> output)
        {
            var proc = customCommandManagerSession
                .GetCustomTransactionProcedure(id, txnManager, scratchBufferManager).Item1;
            return txnManager.RunTransactionProc(id, input, proc, ref output);

        }

        private void TryCustomProcedure(byte id, byte* ptr, byte* end, CustomProcedure proc)
        {
            Debug.Assert(proc != null);

            var output = new MemoryResult<byte>(null, 0);
            var input = new ArgSlice(ptr, (int)(end - ptr));
            if (proc.Execute(basicGarnetApi, input, ref output))
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

        /// <summary>
        /// Custom command
        /// </summary>
        private bool TryCustomRawStringCommand<TKeyLocker, TEpochGuard, TGarnetApi>(byte* ptr, byte* end, RespCommand cmd, long expirationTicks, CommandType type, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetAdvancedApi<TKeyLocker, TEpochGuard>
        {
            var sbKey = parseState.GetArgSliceByRef(0).SpanByte;
            var keyPtr = sbKey.ToPointer();
            var kSize = sbKey.Length;

            ptr = keyPtr + kSize + 2;

            var metadataSize = 8;
            if (expirationTicks == 0) metadataSize = 0;

            // Move key back if needed
            if (metadataSize > 0)
            {
                Buffer.MemoryCopy(keyPtr, keyPtr - metadataSize, kSize, kSize);
                keyPtr -= metadataSize;
            }

            // write key header size
            keyPtr -= sizeof(int);
            *(int*)keyPtr = kSize;

            var inputPtr = ptr;
            var iSize = (int)(end - ptr);

            inputPtr -= RespInputHeader.Size; // input header
            inputPtr -= metadataSize; // metadata header

            var input = new SpanByte(metadataSize + RespInputHeader.Size + iSize, (nint)inputPtr);

            ((RespInputHeader*)(inputPtr + metadataSize))->cmd = cmd;
            ((RespInputHeader*)(inputPtr + metadataSize))->flags = 0;

            if (expirationTicks == -1)
                input.ExtraMetadata = expirationTicks;
            else if (expirationTicks > 0)
                input.ExtraMetadata = DateTimeOffset.UtcNow.Ticks + expirationTicks;

            var output = new SpanByteAndMemory(null);
            GarnetStatus status;
            if (type == CommandType.ReadModifyWrite)
            {
                status = garnetApi.RMW_MainStore(ref Unsafe.AsRef<SpanByte>(keyPtr), ref input, ref output);
                Debug.Assert(!output.IsSpanByte);

                if (output.Memory != null)
                    SendAndReset(output.Memory, output.Length);
                else
                    while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
            }
            else
            {
                status = garnetApi.Read_MainStore(ref Unsafe.AsRef<SpanByte>(keyPtr), ref input, ref output);
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

        /// <summary>
        /// Custom object command
        /// </summary>
        private bool TryCustomObjectCommand<TKeyLocker, TEpochGuard, TGarnetApi>(byte* ptr, byte* end, RespCommand cmd, byte subid, CommandType type, ref TGarnetApi garnetApi)
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IGarnetEpochGuard
            where TGarnetApi : IGarnetAdvancedApi<TKeyLocker, TEpochGuard>
        {
            var keyBytes = parseState.GetArgSliceByRef(0).SpanByte.ToByteArray();

            // Prepare input
            var input = new ObjectInput
            {
                header = new RespInputHeader
                {
                    cmd = cmd,
                    SubId = subid
                },
                parseState = parseState,
                parseStateStartIdx = 1
            };

            var output = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };

            GarnetStatus status;

            if (type == CommandType.ReadModifyWrite)
            {
                status = garnetApi.RMW_ObjectStore(ref keyBytes, ref input, ref output);
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
                status = garnetApi.Read_ObjectStore(ref keyBytes, ref input, ref output);
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