// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SendAndReset()
        {
            byte* d = networkSender.GetResponseObjectHead();
            if ((int)(dcurr - d) > 0)
            {
                Send(d);
                networkSender.GetResponseObject();
                dcurr = networkSender.GetResponseObjectHead();
                dend = networkSender.GetResponseObjectTail();
            }
            else
            {
                // Reaching here means that we retried SendAndReset without the RespWriteUtils.Write*
                // method making any progress. This should only happen when the message being written is
                // too large to fit in the response buffer.
                GarnetException.Throw("Failed to write to response buffer", LogLevel.Critical);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SendAndReset(IMemoryOwner<byte> memory, int length)
        {
            // Copy allocated memory to main buffer and send
            fixed (byte* _src = memory.Memory.Span)
            {
                byte* src = _src;
                int bytesLeft = length;

                // Repeat while we have bytes left to write from input Memory to output buffer
                while (bytesLeft > 0)
                {
                    // Compute space left on output buffer
                    int destSpace = (int)(dend - dcurr);

                    // Adjust number of bytes to copy, to MIN(space left on output buffer, bytes left to copy)
                    int toCopy = bytesLeft;
                    if (toCopy > destSpace)
                        toCopy = destSpace;

                    // Copy bytes to output buffer
                    Buffer.MemoryCopy(src, dcurr, destSpace, toCopy);

                    // Move cursor on output buffer and input memory, update bytes left
                    dcurr += toCopy;
                    src += toCopy;
                    bytesLeft -= toCopy;

                    // If output buffer is full, send and reset output buffer. It is okay to leave the
                    // buffer partially full, as ProcessMessage will do a final Send before returning.
                    if (toCopy == destSpace)
                    {
                        Send(networkSender.GetResponseObjectHead());
                        networkSender.GetResponseObject();
                        dcurr = networkSender.GetResponseObjectHead();
                        dend = networkSender.GetResponseObjectTail();
                    }
                }
            }
            memory.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Send(byte* d)
        {
            // Note: This SEND method may be called for responding to multiple commands in a single message (pipelining),
            // or multiple times in a single command for sending data larger than fitting in buffer at once.

            // #if DEBUG
            // logger?.LogTrace("SEND: [{send}]", Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", ""));
            // Debug.WriteLine($"SEND: [{Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", "")}]");
            // #endif

            if ((int)(dcurr - d) > 0)
            {
                // Debug.WriteLine("SEND: [" + Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr - d))).Replace("\n", "|").Replace("\r", "!") + "]");
                if (waitForAofBlocking)
                {
                    var task = storeWrapper.WaitForCommitAsync();
                    if (!task.IsCompleted) task.AsTask().GetAwaiter().GetResult();
                }
                int sendBytes = (int)(dcurr - d);
                networkSender.SendResponse((int)(d - networkSender.GetResponseObjectHead()), sendBytes);
                sessionMetrics?.incr_total_net_output_bytes((ulong)sendBytes);
            }
        }

        /// <summary>
        /// Debug version - send one byte at a time
        /// </summary>
        private void DebugSend(byte* d)
        {
            // Debug.WriteLine("SEND: [" + Encoding.UTF8.GetString(new Span<byte>(d, (int)(dcurr-d))).Replace("\n", "|").Replace("\r", "") + "]");

            if ((int)(dcurr - d) > 0)
            {
                if (storeWrapper.serverOptions.EnableAOF && storeWrapper.serverOptions.WaitForCommit)
                {
                    var task = storeWrapper.WaitForCommitAsync();
                    if (!task.IsCompleted) task.AsTask().GetAwaiter().GetResult();
                }
                int sendBytes = (int)(dcurr - d);
                byte[] buffer = new byte[sendBytes];
                fixed (byte* dest = buffer)
                    Buffer.MemoryCopy(d, dest, sendBytes, sendBytes);


                for (int i = 0; i < sendBytes; i++)
                {
                    *d = buffer[i];
                    networkSender.SendResponse((int)(d - networkSender.GetResponseObjectHead()), 1);
                    networkSender.GetResponseObject();
                    d = dcurr = networkSender.GetResponseObjectHead();
                    dend = networkSender.GetResponseObjectTail();
                }

                sessionMetrics?.incr_total_net_output_bytes((ulong)sendBytes);
            }
        }

        /// <summary>
        /// Writes current output object
        /// </summary>
        /// <param name="output"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe void ProcessOutput(SpanByteAndMemory output)
        {
            if (!output.IsSpanByte)
                SendAndReset(output.Memory, output.Length);
            else
                dcurr += output.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool Write(ref Status s, ref byte* dst, int length)
        {
            if (length < 1) return false;
            *dst++ = s.Value;
            return true;
        }

        private static unsafe bool Write(ref SpanByteAndMemory k, ref byte* dst, int length)
        {
            if (k.Length > length) return false;

            var dest = new SpanByte(length, (IntPtr)dst);
            if (k.IsSpanByte)
                k.SpanByte.CopyTo(ref dest);
            else
                k.AsMemoryReadOnlySpan().CopyTo(dest.AsSpan());
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool Write(int seqNo, ref byte* dst, int length)
        {
            if (length < sizeof(int)) return false;
            *(int*)dst = seqNo;
            dst += sizeof(int);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteAsciiBulkString(ReadOnlySpan<char> message)
        {
            while (!RespWriteUtils.TryWriteAsciiBulkString(message, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteAsciiDirect(ReadOnlySpan<char> message)
        {
            while (!RespWriteUtils.TryWriteAsciiDirect(message, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteArrayLength(int len)
        {
            while (!RespWriteUtils.TryWriteArrayLength(len, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteBulkString(scoped ReadOnlySpan<byte> message)
        {
            while (!RespWriteUtils.TryWriteBulkString(message, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteDirectLargeRespString(ReadOnlySpan<byte> message)
        {
            while (!RespWriteUtils.TryWriteBulkStringLength(message, ref dcurr, dend))
                SendAndReset();

            WriteDirectLarge(message);

            while (!RespWriteUtils.TryWriteNewLine(ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteDirectLarge(ReadOnlySpan<byte> src)
        {
            // Repeat while we have bytes left to write
            while (src.Length > 0)
            {
                // Compute space left on output buffer
                int destSpace = (int)(dend - dcurr);

                // Fast path if there is enough space
                if (src.Length <= destSpace)
                {
                    src.CopyTo(new Span<byte>(dcurr, src.Length));
                    dcurr += src.Length;
                    break;
                }

                // Adjust number of bytes to copy, to space left on output buffer, then copy
                src.Slice(0, destSpace).CopyTo(new Span<byte>(dcurr, destSpace));
                dcurr += destSpace;
                src = src.Slice(destSpace);

                // Send and reset output buffer
                Send(networkSender.GetResponseObjectHead());
                networkSender.GetResponseObject();
                dcurr = networkSender.GetResponseObjectHead();
                dend = networkSender.GetResponseObjectTail();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteDirect(scoped ReadOnlySpan<byte> span)
        {
            while (!RespWriteUtils.TryWriteDirect(span, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteDoubleNumeric(double value)
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteDoubleNumeric(value, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteDoubleBulkString(value, ref dcurr, dend))
                    SendAndReset();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteEmptyArray()
        {
            while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteEmptySet()
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteEmptySet(ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                    SendAndReset();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteError(scoped ReadOnlySpan<byte> errorString)
        {
            while (!RespWriteUtils.TryWriteError(errorString, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteError(ReadOnlySpan<char> errorString)
        {
            while (!RespWriteUtils.TryWriteError(errorString, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteInt32(int value)
        {
            while (!RespWriteUtils.TryWriteInt32(value, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteInt32AsBulkString(int value)
        {
            while (!RespWriteUtils.TryWriteInt32AsBulkString(value, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteInt64(long value)
        {
            while (!RespWriteUtils.TryWriteInt64(value, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteInt64AsBulkString(long value)
        {
            while (!RespWriteUtils.TryWriteInt64AsBulkString(value, ref dcurr, dend, out _))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteIntegerFromBytes(ReadOnlySpan<byte> integerBytes)
        {
            while (!RespWriteUtils.TryWriteIntegerFromBytes(integerBytes, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteMapLength(int count)
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteMapLength(count, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteArrayLength(count * 2, ref dcurr, dend))
                    SendAndReset();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteOK()
        {
            WriteDirect(CmdStrings.RESP_OK);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteNull()
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteResp3Null(ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteNull(ref dcurr, dend))
                    SendAndReset();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteNullArray()
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteResp3Null(ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteNullArray(ref dcurr, dend))
                    SendAndReset();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WritePushLength(int count)
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWritePushLength(count, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteArrayLength(count, ref dcurr, dend))
                    SendAndReset();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteSetLength(int count)
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteSetLength(count, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteArrayLength(count, ref dcurr, dend))
                    SendAndReset();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteSimpleString(scoped ReadOnlySpan<byte> simpleString)
        {
            while (!RespWriteUtils.TryWriteSimpleString(simpleString, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteSimpleString(ReadOnlySpan<char> simpleString)
        {
            while (!RespWriteUtils.TryWriteSimpleString(simpleString, ref dcurr, dend))
                SendAndReset();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteUtf8BulkString(ReadOnlySpan<char> chars)
        {
            while (!RespWriteUtils.TryWriteUtf8BulkString(chars, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteVerbatimASCIITxtString(ReadOnlySpan<char> item)
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteVerbatimASCIIString(item, "txt"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteAsciiBulkString(item, ref dcurr, dend))
                    SendAndReset();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteVerbatimTxtString(scoped ReadOnlySpan<byte> item)
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteVerbatimString(item, "txt"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteBulkString(item, ref dcurr, dend))
                    SendAndReset();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteVerbatimUtf8TxtString(ReadOnlySpan<char> item)
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteVerbatimUtf8String(item, "txt"u8, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteUtf8BulkString(item, ref dcurr, dend))
                    SendAndReset();
            }
        }
    }
}