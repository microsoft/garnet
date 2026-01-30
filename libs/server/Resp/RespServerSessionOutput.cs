// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// RESP server session
    /// </summary>
    internal sealed unsafe partial class RespServerSession : ServerSessionBase
    {
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

        private void WriteError(scoped ReadOnlySpan<byte> errorString)
        {
            while (!RespWriteUtils.TryWriteError(errorString, ref dcurr, dend))
                SendAndReset();
        }

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
        private void WriteZero()
        {
            while (!RespWriteUtils.TryWriteZero(ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteOne()
        {
            while (!RespWriteUtils.TryWriteOne(ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteBooleanTrue()
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteTrue(ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteOne(ref dcurr, dend))
                    SendAndReset();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteBooleanFalse()
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteFalse(ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteZero(ref dcurr, dend))
                    SendAndReset();
            }
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
        private void WriteLargeSimpleString(scoped ReadOnlySpan<byte> simpleString)
        {
            // Simple strings are of the form "+OK\r\n"
            while (dcurr == dend)
                SendAndReset();

            *dcurr++ = (byte)'+';
            while (simpleString.Length > 0)
            {
                if (dcurr == dend)
                {
                    SendAndReset();
                }

                int toCopy = Math.Min((int)(dend - dcurr), simpleString.Length);
                simpleString.Slice(0, toCopy).CopyTo(new Span<byte>(dcurr, toCopy));
                dcurr += toCopy;
                simpleString = simpleString.Slice(toCopy);
            }

            while (!RespWriteUtils.TryWriteNewLine(ref dcurr, dend))
                SendAndReset();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteUtf8BulkString(ReadOnlySpan<char> chars)
        {
            while (!RespWriteUtils.TryWriteUtf8BulkString(chars, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteVerbatimString(scoped ReadOnlySpan<byte> item, scoped ReadOnlySpan<byte> ext = default)
        {
            if (respProtocolVersion >= 3)
            {
                while (!RespWriteUtils.TryWriteVerbatimString(item, ext.IsEmpty ? RespStrings.VerbatimTxt : ext, ref dcurr, dend))
                    SendAndReset();
            }
            else
            {
                while (!RespWriteUtils.TryWriteBulkString(item, ref dcurr, dend))
                    SendAndReset();
            }
        }
    }
}