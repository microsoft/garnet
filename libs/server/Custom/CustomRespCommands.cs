// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
            }
            else
            {
                // Write output to wire
                if (output.MemoryOwner != null)
                    SendAndReset(output.MemoryOwner, output.Length);
                else
                    while (!RespWriteUtils.WriteResponse(Encoding.ASCII.GetBytes($"-ERR Transaction failed.\r\n"), ref dcurr, dend))
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

        /// <summary>
        /// Custom command
        /// </summary>
        private bool TryCustomCommand<TGarnetApi>(byte* ptr, byte* end, RespCommand cmd, long expirationTicks, CommandType type, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetAdvancedApi
        {
            byte* keyPtr = null, inputPtr = null;
            int ksize = 0, isize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            int metadataSize = 8;
            if (expirationTicks == 0) metadataSize = 0;

            // Move key back if needed
            if (metadataSize > 0)
            {
                Buffer.MemoryCopy(keyPtr, keyPtr - metadataSize, ksize, ksize);
                keyPtr -= metadataSize;
            }

            // write key header size
            keyPtr -= sizeof(int);
            *(int*)keyPtr = ksize;

            inputPtr = ptr;
            isize = (int)(end - ptr);

            inputPtr -= RespInputHeader.Size; // input header
            inputPtr -= metadataSize; // metadata header

            var input = new SpanByte(metadataSize + RespInputHeader.Size + isize, (nint)inputPtr);

            ((RespInputHeader*)(inputPtr + metadataSize))->cmd = cmd;
            ((RespInputHeader*)(inputPtr + metadataSize))->flags = 0;

            if (expirationTicks == -1)
                input.ExtraMetadata = expirationTicks;
            else if (expirationTicks > 0)
                input.ExtraMetadata = DateTimeOffset.UtcNow.Ticks + expirationTicks;

            SpanByteAndMemory output = new SpanByteAndMemory(null);
            GarnetStatus status;
            if (type == CommandType.ReadModifyWrite)
            {
                status = storageApi.RMW_MainStore(ref Unsafe.AsRef<SpanByte>(keyPtr), ref input, ref output);
                Debug.Assert(!output.IsSpanByte);

                if (output.Memory != null)
                    SendAndReset(output.Memory, output.Length);
                else
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
            }
            else
            {
                status = storageApi.Read_MainStore(ref Unsafe.AsRef<SpanByte>(keyPtr), ref input, ref output);
                Debug.Assert(!output.IsSpanByte);

                if (status == GarnetStatus.OK)
                {
                    if (output.Memory != null)
                        SendAndReset(output.Memory, output.Length);
                    else
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();
                }
                else
                {
                    Debug.Assert(output.Memory == null);
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                }
            }

            return true;
        }

        /// <summary>
        /// Custom object command
        /// </summary>
        private bool TryCustomObjectCommand<TGarnetApi>(byte* ptr, byte* end, RespCommand cmd, byte subid, CommandType type, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetAdvancedApi
        {
            byte* keyPtr = null, inputPtr = null;
            int ksize = 0, isize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            byte[] key = new Span<byte>(keyPtr, ksize).ToArray();

            inputPtr = ptr;
            isize = (int)(end - ptr);
            inputPtr -= sizeof(int);
            inputPtr -= RespInputHeader.Size;
            *(int*)inputPtr = RespInputHeader.Size + isize;
            ((RespInputHeader*)(inputPtr + sizeof(int)))->cmd = cmd;
            ((RespInputHeader*)(inputPtr + sizeof(int)))->SubId = subid;

            var output = new GarnetObjectStoreOutput { spanByteAndMemory = new SpanByteAndMemory(null) };
            GarnetStatus status;
            if (type == CommandType.ReadModifyWrite)
            {
                status = storageApi.RMW_ObjectStore(ref key, ref Unsafe.AsRef<SpanByte>(inputPtr), ref output);
                Debug.Assert(!output.spanByteAndMemory.IsSpanByte);

                if (output.spanByteAndMemory.Memory != null)
                    SendAndReset(output.spanByteAndMemory.Memory, output.spanByteAndMemory.Length);
                else
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
            }
            else
            {
                status = storageApi.Read_ObjectStore(ref key, ref Unsafe.AsRef<SpanByte>(inputPtr), ref output);
                Debug.Assert(!output.spanByteAndMemory.IsSpanByte);

                if (status == GarnetStatus.OK)
                {
                    if (output.spanByteAndMemory.Memory != null)
                        SendAndReset(output.spanByteAndMemory.Memory, output.spanByteAndMemory.Length);
                    else
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                            SendAndReset();

                }
                else
                {
                    Debug.Assert(output.spanByteAndMemory.Memory == null);
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                }
            }

            return true;
        }
    }
}