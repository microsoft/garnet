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
        /// <summary>
        /// GET
        /// </summary>
        bool NetworkGET<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            if (storeWrapper.serverOptions.EnableScatterGatherGet)
                return NetworkGET_SG(ptr, ref storageApi);

            byte* keyPtr = null;
            int ksize = 0;

            ptr += 13;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
                return true;

            keyPtr -= sizeof(int); // length header
            *(int*)keyPtr = ksize;

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
            SpanByte input = default;
            var status = storageApi.GET(ref Unsafe.AsRef<SpanByte>(keyPtr), ref input, ref o);

            switch (status)
            {
                case GarnetStatus.OK:
                    if (!o.IsSpanByte)
                        SendAndReset(o.Memory, o.Length);
                    else
                        dcurr += o.Length;
                    break;
                case GarnetStatus.NOTFOUND:
                    Debug.Assert(o.IsSpanByte);
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            return true;
        }

        /// <summary>
        /// GET - scatter gather version
        /// </summary>
        bool NetworkGET_SG<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetAdvancedApi
        {
            byte* keyPtr = null;
            int ksize = 0;

            ptr += 13;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            SpanByte input = default;
            long ctx = default;
            int firstPending = -1;
            (GarnetStatus, SpanByteAndMemory)[] outputArr = null;
            SpanByteAndMemory o = new(dcurr, (int)(dend - dcurr));
            int c = 0;

            for (; ; c++)
            {
                if (c > 0 && !ParseGETAndKey(ref keyPtr, ref ksize, ref ptr))
                    break;
                readHead = (int)(ptr - recvBufferPtr);

                // Cluster verification
                if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
                    continue;

                // Store length header for the key
                keyPtr -= sizeof(int);
                *(int*)keyPtr = ksize;

                // Store index in context, since completions are not in order
                ctx = firstPending == -1 ? 0 : c - firstPending;

                var status = storageApi.GET_WithPending(ref Unsafe.AsRef<SpanByte>(keyPtr), ref input, ref o, ctx, out bool isPending);

                if (isPending)
                {
                    SetResult(c, ref firstPending, ref outputArr, status, default);
                    o = new SpanByteAndMemory();
                }
                else
                {
                    if (status == GarnetStatus.OK)
                    {
                        if (firstPending == -1)
                        {
                            // Found in memory without IO, and no earlier pending, so we can add directly to the output
                            if (!o.IsSpanByte)
                                SendAndReset(o.Memory, o.Length);
                            else
                                dcurr += o.Length;
                            o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                        }
                        else
                        {
                            SetResult(c, ref firstPending, ref outputArr, status, o);
                            o = new SpanByteAndMemory();
                        }
                    }
                    else
                    {
                        if (firstPending == -1)
                        {
                            // Realized not-found without IO, and no earlier pending, so we can add directly to the output
                            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                                SendAndReset();
                            o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                        }
                        else
                        {
                            SetResult(c, ref firstPending, ref outputArr, status, o);
                            o = new SpanByteAndMemory();
                        }
                    }
                }
            }

            if (firstPending != -1)
            {
                // First complete all pending ops
                storageApi.GET_CompletePending(outputArr, true);

                // Write the outputs to network buffer
                for (int i = 0; i < c - firstPending; i++)
                {
                    var status = outputArr[i].Item1;
                    var output = outputArr[i].Item2;
                    if (status == GarnetStatus.OK)
                    {
                        if (!output.IsSpanByte)
                            SendAndReset(output.Memory, output.Length);
                        else
                            dcurr += output.Length;
                    }
                    else
                    {
                        while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                            SendAndReset();
                    }
                }
            }

            if (c > 1)
            {
                // Update metrics (the first GET is accounted for by the caller)
                if (latencyMetrics != null) opCount += c - 1;
                if (sessionMetrics != null)
                {
                    sessionMetrics.total_commands_processed += (ulong)(c - 1);
                    sessionMetrics.total_read_commands_processed += (ulong)(c - 1);
                }
            }

            return true;
        }

        bool ParseGETAndKey(ref byte* keyPtr, ref int ksize, ref byte* ptr)
        {
            if (bytesRead - readHead >= 19)
            {
                // GET key1 value1 => [*2\r\n$3\r\nGET\r\n$]3\r\nkey\r\n
                if (*(long*)ptr == 724291344956994090L && *(2 + (int*)ptr) == 223626567 && *(ushort*)(12 + ptr) == 9226)
                    ptr += 13;
                else return false;
            }
            else
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;
            return true;
        }

        void SetResult(int c, ref int firstPending, ref (GarnetStatus, SpanByteAndMemory)[] outputArr, GarnetStatus status, SpanByteAndMemory output)
        {
            const int initialBatchSize = 8; // number of items in initial batch
            if (firstPending == -1)
            {
                outputArr = new (GarnetStatus, SpanByteAndMemory)[initialBatchSize];
                firstPending = c;
            }

            Debug.Assert(firstPending >= 0);
            Debug.Assert(c >= firstPending);
            Debug.Assert(outputArr != null);

            if (c - firstPending >= outputArr.Length)
            {
                int newCount = (int)NextPowerOf2(c - firstPending + 1);
                var outputArr2 = new (GarnetStatus, SpanByteAndMemory)[newCount];
                Array.Copy(outputArr, outputArr2, outputArr.Length);
                outputArr = outputArr2;
            }
            outputArr[c - firstPending] = (status, output);
        }


        static long NextPowerOf2(long v)
        {
            v--;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v |= v >> 32;
            return v + 1;
        }

        /// <summary>
        /// SET
        /// </summary>
        private bool NetworkSET<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 13;

            byte* keyPtr = null, valPtr = null;
            int ksize = 0, vsize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, false))
                return true;

            keyPtr -= sizeof(int);
            valPtr -= sizeof(int);
            *(int*)keyPtr = ksize;
            *(int*)valPtr = vsize;

            var status = storageApi.SET(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(valPtr));
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// SETRANGE
        /// </summary>
        private bool NetworkSetRange<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 18;

            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            byte* offsetPtr = null;
            int offsetSize = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref offsetPtr, ref offsetSize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            byte* valPtr = null;
            int vsize = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, false))
                return true;

            int offset = NumUtils.BytesToInt(offsetSize, offsetPtr);
            if (offset < 0)
            {
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERROFFSETOUTOFRANGE, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            var key = new ArgSlice(keyPtr, ksize);
            var value = new ArgSlice(valPtr, vsize);

            const int maxOutputSize = 20; // max byte length to store length of appended string in ASCII byte values
            byte* pbOutput = stackalloc byte[maxOutputSize];
            var output = new ArgSlice(pbOutput, maxOutputSize);

            var status = storageApi.SETRANGE(key, value, offset, ref output);

            while (!RespWriteUtils.WriteIntegerFromBytes(pbOutput, output.Length, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        private bool NetworkGetRange<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 18;

            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            byte* startPtr = null;
            int startSize = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref startPtr, ref startSize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            byte* endPtr = null;
            int endSize = 0;
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref endPtr, ref endSize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);

            keyPtr -= sizeof(int); // length header
            *(int*)keyPtr = ksize;
            if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
                return true;

            int sliceStart = NumUtils.BytesToInt(startSize, startPtr);
            int sliceLength = NumUtils.BytesToInt(endSize, endPtr);

            var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));

            var status = storageApi.GETRANGE(ref Unsafe.AsRef<SpanByte>(keyPtr), sliceStart, sliceLength, ref o);

            if (status == GarnetStatus.OK)
            {
                sessionMetrics?.incr_total_found();
                if (!o.IsSpanByte)
                    SendAndReset(o.Memory, o.Length);
                else
                    dcurr += o.Length;
            }
            else
            {
                sessionMetrics?.incr_total_notfound();
                Debug.Assert(o.IsSpanByte);
                while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                    SendAndReset();
            }

            return true;
        }

        /// <summary>
        /// SETEX
        /// </summary>
        private bool NetworkSETEX<TGarnetApi>(byte* ptr, bool highPrecision, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            ptr += 15;
            if (highPrecision) ptr++; // PSETEX

            byte* keyPtr = null, valPtr = null;
            int ksize = 0, vsize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadIntWithLengthHeader(out int expiry, ref ptr, recvBufferPtr + bytesRead))
                return false;

            // TODO: return error for 0 expiry time

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, false))
                return true;

            keyPtr -= sizeof(int);
            valPtr -= sizeof(int) + sizeof(long);
            *(int*)keyPtr = ksize;
            *(int*)valPtr = vsize + sizeof(long); // expiry info
            SpanByte.Reinterpret(valPtr).ExtraMetadata = DateTimeOffset.UtcNow.Ticks + (highPrecision ? TimeSpan.FromMilliseconds(expiry).Ticks : TimeSpan.FromSeconds(expiry).Ticks);

            var status = storageApi.SET(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(valPtr));
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        enum ExpirationOption : byte
        {
            None, EX, PX, EXAT, PXAT, KEEPTTL
        }

        enum ExistOptions : byte
        {
            None, NX, XX
        }

        /// <summary>
        /// SET EX NX
        /// </summary>
        private bool NetworkSETEXNX<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            var _ptr = ptr;
            int cmdcount = *(ptr + 1) - '0' - 3;
            ptr += 13;

            byte* keyPtr = null, valPtr = null;
            int ksize = 0, vsize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, false))
            {
                readHead = (int)(ptr - recvBufferPtr);
                return true;
            }

            int expiry = 0;
            bool error = false;
            ReadOnlySpan<byte> errorMessage = default;
            ExistOptions existOptions = ExistOptions.None;
            ExpirationOption expOption = ExpirationOption.None;
            bool getValue = false;

            while (cmdcount > 0)
            {
                if (error)
                {
                    Span<byte> tmp = default;
                    if (!RespReadUtils.ReadSpanByteWithLengthHeader(ref tmp, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    cmdcount--;
                    continue;
                }

                if (*(long*)ptr == 724332168621142564) // [EX]
                {
                    ptr += 8;
                    cmdcount--;

                    if (!RespReadUtils.ReadIntWithLengthHeader(out expiry, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    cmdcount--;

                    if (expOption != ExpirationOption.None)
                    {
                        errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                        error = true;
                        continue;
                    }
                    expOption = ExpirationOption.EX;
                    if (expiry <= 0)
                    {
                        errorMessage = CmdStrings.RESP_ERRINVALIDEXP_IN_SET;
                        error = true;
                        continue;
                    }
                }
                else if (*(long*)ptr == 724332215865782820) // [PX]
                {
                    ptr += 8;
                    cmdcount--;

                    if (!RespReadUtils.ReadIntWithLengthHeader(out expiry, ref ptr, recvBufferPtr + bytesRead))
                        return false;
                    cmdcount--;

                    if (expOption != ExpirationOption.None)
                    {
                        errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                        error = true;
                        continue;
                    }
                    expOption = ExpirationOption.PX;
                    if (expiry <= 0)
                    {
                        errorMessage = CmdStrings.RESP_ERRINVALIDEXP_IN_SET;
                        error = true;
                        continue;
                    }
                }
                else if (*(long*)ptr == 5784105485020772132 && *(int*)(ptr + 8) == 223106132 && *(ptr + 12) == 10) // [KEEPTTL]
                {
                    ptr += 13;
                    cmdcount--;
                    if (expOption != ExpirationOption.None)
                    {
                        errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                        error = true;
                        continue;
                    }
                    expOption = ExpirationOption.KEEPTTL;
                }
                else if (*(long*)ptr == 724332207275848228) // [NX]
                {
                    ptr += 8;
                    cmdcount--;
                    if (existOptions != ExistOptions.None)
                    {
                        errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                        error = true;
                        continue;
                    }
                    existOptions = ExistOptions.NX;
                }
                else if (*(long*)ptr == 724332250225521188) // [XX]
                {
                    ptr += 8;
                    cmdcount--;
                    if (existOptions != ExistOptions.None)
                    {
                        errorMessage = CmdStrings.RESP_SYNTAX_ERROR;
                        error = true;
                        continue;
                    }
                    existOptions = ExistOptions.XX;
                }
                else if (*(long*)ptr == 960468791950390052 && *(ptr + 8) == 10) // [GET]
                {
                    ptr += 9;
                    cmdcount--;
                    getValue = true;
                }
                else if (!MakeUpperCase(ptr))
                {
                    errorMessage = CmdStrings.RESP_ERR;
                    error = true;
                    continue;
                }
            }

            readHead = (int)(ptr - recvBufferPtr);

            if (error)
            {
                while (!RespWriteUtils.WriteResponse(errorMessage, ref dcurr, dend))
                    SendAndReset();
                return true;
            }

            // Make space for key header
            keyPtr -= sizeof(int);

            // Set key length
            *(int*)keyPtr = ksize;

            // Make space for value header
            valPtr -= sizeof(int);

            switch (expOption)
            {
                case ExpirationOption.None:
                case ExpirationOption.EX:
                    switch (existOptions)
                    {
                        case ExistOptions.None:
                            return getValue ?
                                NetworkSET_Conditional(RespCommand.SET, ptr, expiry, keyPtr, valPtr, vsize, getValue, false, ref storageApi) :
                                NetworkSET_EX(RespCommand.SET, ptr, expiry, keyPtr, valPtr, vsize, false, ref storageApi); // Can perform a blind update
                        case ExistOptions.XX:
                            return NetworkSET_Conditional(RespCommand.SETEXXX, ptr, expiry, keyPtr, valPtr, vsize, getValue, false, ref storageApi);
                        case ExistOptions.NX:
                            return NetworkSET_Conditional(RespCommand.SETEXNX, ptr, expiry, keyPtr, valPtr, vsize, getValue, false, ref storageApi);
                    }
                    break;
                case ExpirationOption.PX:
                    switch (existOptions)
                    {
                        case ExistOptions.None:
                            return getValue ?
                                NetworkSET_Conditional(RespCommand.SET, ptr, expiry, keyPtr, valPtr, vsize, getValue, true, ref storageApi) :
                                NetworkSET_EX(RespCommand.SET, ptr, expiry, keyPtr, valPtr, vsize, true, ref storageApi); // Can perform a blind update
                        case ExistOptions.XX:
                            return NetworkSET_Conditional(RespCommand.SETEXXX, ptr, expiry, keyPtr, valPtr, vsize, getValue, true, ref storageApi);
                        case ExistOptions.NX:
                            return NetworkSET_Conditional(RespCommand.SETEXNX, ptr, expiry, keyPtr, valPtr, vsize, getValue, true, ref storageApi);
                    }
                    break;

                case ExpirationOption.KEEPTTL:
                    Debug.Assert(expiry == 0); // no expiration if KEEPTTL
                    switch (existOptions)
                    {
                        case ExistOptions.None:
                            // We can never perform a blind update due to KEEPTTL
                            return NetworkSET_Conditional(RespCommand.SETKEEPTTL, ptr, expiry, keyPtr, valPtr, vsize, getValue, false, ref storageApi);
                        case ExistOptions.XX:
                            return NetworkSET_Conditional(RespCommand.SETKEEPTTLXX, ptr, expiry, keyPtr, valPtr, vsize, getValue, false, ref storageApi);
                        case ExistOptions.NX:
                            return NetworkSET_Conditional(RespCommand.SETEXNX, ptr, expiry, keyPtr, valPtr, vsize, getValue, false, ref storageApi);
                    }
                    break;
            }

            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERR, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        private bool NetworkSET_EX<TGarnetApi>(RespCommand cmd, byte* ptr, int expiry, byte* keyPtr, byte* valPtr, int vsize, bool highPrecision, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(cmd == RespCommand.SET);

            if (expiry == 0) // no expiration provided - this code path will not be currently hit as TrySET is used
            {
                *(int*)valPtr = vsize;
            }
            else
            {
                // Move payload forward to make space for metadata
                Buffer.MemoryCopy(valPtr + sizeof(int), valPtr + sizeof(int) + sizeof(long), vsize, vsize);
                *(int*)valPtr = vsize + sizeof(long);
                SpanByte.Reinterpret(valPtr).ExtraMetadata = DateTimeOffset.UtcNow.Ticks + (highPrecision ? TimeSpan.FromMilliseconds(expiry).Ticks : TimeSpan.FromSeconds(expiry).Ticks);
            }

            storageApi.SET(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(valPtr));
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        private bool NetworkSET_Conditional<TGarnetApi>(RespCommand cmd, byte* ptr, int expiry, byte* keyPtr, byte* inputPtr, int isize, bool getValue, bool highPrecision, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // Make space for RespCommand in input
            inputPtr -= RespInputHeader.Size;

            if (expiry == 0) // no expiration provided
            {
                *(int*)inputPtr = RespInputHeader.Size + isize;
                ((RespInputHeader*)(inputPtr + sizeof(int)))->cmd = cmd;
                ((RespInputHeader*)(inputPtr + sizeof(int)))->flags = 0;
                if (getValue)
                    ((RespInputHeader*)(inputPtr + sizeof(int)))->SetSetGetFlag();
            }
            else
            {
                // Move payload forward to make space for metadata
                Buffer.MemoryCopy(inputPtr + sizeof(int) + RespInputHeader.Size, inputPtr + sizeof(int) + sizeof(long) + RespInputHeader.Size, isize, isize);
                *(int*)inputPtr = sizeof(long) + RespInputHeader.Size + isize;
                ((RespInputHeader*)(inputPtr + sizeof(int) + sizeof(long)))->cmd = cmd;
                ((RespInputHeader*)(inputPtr + sizeof(int) + sizeof(long)))->flags = 0;
                if (getValue)
                    ((RespInputHeader*)(inputPtr + sizeof(int) + sizeof(long)))->SetSetGetFlag();
                SpanByte.Reinterpret(inputPtr).ExtraMetadata = DateTimeOffset.UtcNow.Ticks + (highPrecision ? TimeSpan.FromMilliseconds(expiry).Ticks : TimeSpan.FromSeconds(expiry).Ticks);
            }

            if (getValue)
            {
                var o = new SpanByteAndMemory(dcurr, (int)(dend - dcurr));
                var status = storageApi.SET_Conditional(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(inputPtr), ref o);

                // Status tells us whether an old image was found during RMW or not
                if (status == GarnetStatus.NOTFOUND)
                {
                    Debug.Assert(o.IsSpanByte);
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    if (!o.IsSpanByte)
                        SendAndReset(o.Memory, o.Length);
                    else
                        dcurr += o.Length;
                }
            }
            else
            {
                var status = storageApi.SET_Conditional(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(inputPtr));

                bool ok = status != GarnetStatus.NOTFOUND;

                // Status tells us whether an old image was found during RMW or not
                // For a "set if not exists", NOTFOUND means the operation succeeded
                // So we invert the ok flag
                if (cmd == RespCommand.SETEXNX)
                    ok = !ok;
                if (!ok)
                {
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_ERRNOTFOUND, ref dcurr, dend))
                        SendAndReset();
                }
                else
                {
                    while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                        SendAndReset();
                }
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

        /// <summary> 
        /// Increment (INCRBY, DECRBY, INCR, DECR)
        /// </summary>
        private bool NetworkIncrement<TGarnetApi>(byte* ptr, RespCommand cmd, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            Debug.Assert(cmd == RespCommand.INCRBY || cmd == RespCommand.DECRBY || cmd == RespCommand.INCR || cmd == RespCommand.DECR);

            if (cmd == RespCommand.INCRBY || cmd == RespCommand.DECRBY)
                ptr += 16;
            else
                ptr += 14;

            byte* keyPtr = null;
            int ksize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            ArgSlice input = default;
            if (cmd == RespCommand.INCRBY || cmd == RespCommand.DECRBY)
            {
                byte* valPtr = null;
                int vsize = 0;
                if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, recvBufferPtr + bytesRead))
                    return false;
                valPtr -= RespInputHeader.Size;
                vsize += RespInputHeader.Size;
                ((RespInputHeader*)valPtr)->cmd = cmd;
                ((RespInputHeader*)valPtr)->flags = 0;
                input = new ArgSlice(valPtr, vsize);
            }
            else if (cmd == RespCommand.INCR)
            {
                int vsize = RespInputHeader.Size + 1;
                byte* valPtr = stackalloc byte[vsize];
                ((RespInputHeader*)valPtr)->cmd = cmd;
                ((RespInputHeader*)valPtr)->flags = 0;
                *(valPtr + RespInputHeader.Size) = (byte)'1';
                input = new ArgSlice(valPtr, vsize);
            }
            else if (cmd == RespCommand.DECR)
            {
                int vsize = RespInputHeader.Size + 2;
                byte* valPtr = stackalloc byte[vsize];
                ((RespInputHeader*)valPtr)->cmd = cmd;
                ((RespInputHeader*)valPtr)->flags = 0;
                *(valPtr + RespInputHeader.Size) = (byte)'-';
                *(valPtr + RespInputHeader.Size + 1) = (byte)'1';
                input = new ArgSlice(valPtr, vsize);
            }

            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, false))
                return true;

            var key = new ArgSlice(keyPtr, ksize);

            byte* pbOutput = stackalloc byte[20];
            var output = new ArgSlice(pbOutput, 20);

            var status = storageApi.Increment(key, input, ref output);

            while (!RespWriteUtils.WriteIntegerFromBytes(pbOutput, output.Length, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// APPEND command - appends value at the end of existing string
        /// </summary>
        private bool NetworkAppend<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
            where TGarnetApi : IGarnetApi
        {
            // Length of APPEND command (*3\r\n$6\r\nAPPEND\r\n)
            ptr += 16;

            byte* keyPtr = null, valPtr = null;
            int ksize = 0, vsize = 0;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (!RespReadUtils.ReadPtrWithLengthHeader(ref valPtr, ref vsize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            readHead = (int)(ptr - recvBufferPtr);

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, false))
                return true;

            const int maxOutputSize = 20; // max byte length to store length of appended string in ASCII byte values
            byte* pbOutput = stackalloc byte[maxOutputSize];

            keyPtr -= sizeof(int);
            valPtr -= sizeof(int);
            *(int*)keyPtr = ksize;
            *(int*)valPtr = vsize;

            var output = new SpanByteAndMemory(pbOutput, maxOutputSize);

            var status = storageApi.APPEND(ref Unsafe.AsRef<SpanByte>(keyPtr), ref Unsafe.AsRef<SpanByte>(valPtr), ref output);

            while (!RespWriteUtils.WriteIntegerFromBytes(pbOutput, output.Length, ref dcurr, dend))
                SendAndReset();

            return true;
        }

        /// <summary>
        /// PING
        /// </summary>
        private bool NetworkPING()
        {
            var ptr = recvBufferPtr + readHead;
            readHead += *ptr == '*' ? 14 : 6;
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_PONG, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// ASKING
        /// </summary>
        private bool NetworkASKING()
        {
            //*1\r\n$6\r\n ASKING\r\n = 16
            readHead += 16;
            if (storeWrapper.serverOptions.EnableCluster)
                SessionAsking = 2;
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// QUIT
        /// </summary>
        private bool NetworkQUIT()
        {
            readHead += 6;
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            toDispose = true;
            return true;
        }

        /// <summary>
        /// Mark this session as readonly session
        /// </summary>
        /// <returns></returns>
        private bool NetworkREADONLY()
        {
            //*1\r\n$8\r\nREADONLY\r\n
            readHead += 18;
            clusterSession?.SetReadOnlySession();
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// Mark this session as readwrite
        /// </summary>
        /// <returns></returns>
        private bool NetworkREADWRITE()
        {
            //*1\r\n$9\r\nREADWRITE\r\n
            readHead += 19;
            clusterSession?.SetReadWriteSession();
            while (!RespWriteUtils.WriteResponse(CmdStrings.RESP_OK, ref dcurr, dend))
                SendAndReset();
            return true;
        }

        /// <summary>
        /// Returns the length of the string value stored at key. An -1 is returned when key is not found
        /// </summary>
        /// <typeparam name="TGarnetApi"></typeparam>
        /// <param name="ptr"></param>
        /// <param name="storageApi"></param>
        /// <returns></returns>
        private bool NetworkSTRLEN<TGarnetApi>(byte* ptr, ref TGarnetApi storageApi)
             where TGarnetApi : IGarnetApi
        {
            ptr += 12;

            byte* keyPtr = null;
            int ksize = 0;

            //STRLEN key
            if (!RespReadUtils.ReadPtrWithLengthHeader(ref keyPtr, ref ksize, ref ptr, recvBufferPtr + bytesRead))
                return false;

            if (NetworkSingleKeySlotVerify(keyPtr, ksize, true))
                return true;

            var keyArgSlice = new ArgSlice(keyPtr, ksize);

            var status = storageApi.GET(keyArgSlice, out ArgSlice value);

            switch (status)
            {
                case GarnetStatus.OK:
                    while (!RespWriteUtils.WriteInteger(value.Length, ref dcurr, dend))
                        SendAndReset();
                    break;
                case GarnetStatus.NOTFOUND:
                    while (!RespWriteUtils.WriteInteger(0, ref dcurr, dend))
                        SendAndReset();
                    break;
            }

            readHead = (int)(ptr - recvBufferPtr);
            return true;
        }

    }
}