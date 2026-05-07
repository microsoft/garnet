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
        internal void ProcessOutput(SpanByteAndMemory output)
        {
            if (!output.IsSpanByte)
                SendAndReset(output.Memory, output.Length);
            else
                dcurr += output.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteAsciiBulkString(ReadOnlySpan<char> message)
        {
            while (!RespWriteUtils.TryWriteAsciiBulkString(message, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteAsciiDirect(ReadOnlySpan<char> message)
        {
            while (!RespWriteUtils.TryWriteAsciiDirect(message, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteArrayLength(int len)
        {
            while (!RespWriteUtils.TryWriteArrayLength(len, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteArrayItem(long recordsExpired)
        {
            while (!RespWriteUtils.TryWriteArrayItem(recordsExpired, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteBulkString(scoped ReadOnlySpan<byte> message)
        {
            while (!RespWriteUtils.TryWriteBulkString(message, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteDirectLargeRespString(ReadOnlySpan<byte> message)
        {
            while (!RespWriteUtils.TryWriteBulkStringLength(message, ref dcurr, dend))
                SendAndReset();

            WriteDirectLarge(message);

            while (!RespWriteUtils.TryWriteNewLine(ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteDirect(scoped ReadOnlySpan<byte> span)
        {
            while (!RespWriteUtils.TryWriteDirect(span, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteDoubleNumeric(double value)
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
        internal void WriteEmptyArray()
        {
            while (!RespWriteUtils.TryWriteEmptyArray(ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteEmptySet()
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

        internal void WriteError(scoped ReadOnlySpan<byte> errorString)
        {
            commandErrorWritten = true;
            while (!RespWriteUtils.TryWriteError(errorString, ref dcurr, dend))
                SendAndReset();
        }

        internal void WriteError(ReadOnlySpan<char> errorString)
        {
            commandErrorWritten = true;
            while (!RespWriteUtils.TryWriteError(errorString, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteInt32(int value)
        {
            while (!RespWriteUtils.TryWriteInt32(value, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteInt32AsBulkString(int value)
        {
            while (!RespWriteUtils.TryWriteInt32AsBulkString(value, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteInt64(long value)
        {
            while (!RespWriteUtils.TryWriteInt64(value, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteInt64AsBulkString(long value)
        {
            while (!RespWriteUtils.TryWriteInt64AsBulkString(value, ref dcurr, dend, out _))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteIntegerFromBytes(ReadOnlySpan<byte> integerBytes)
        {
            while (!RespWriteUtils.TryWriteIntegerFromBytes(integerBytes, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteMapLength(int count)
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
        internal void WriteZero()
        {
            while (!RespWriteUtils.TryWriteZero(ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteOne()
        {
            while (!RespWriteUtils.TryWriteOne(ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteNull()
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
        internal void WriteNullArray()
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
        internal void WritePushLength(int count)
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
        internal void WriteSetLength(int count)
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
        internal void WriteSimpleString(scoped ReadOnlySpan<byte> simpleString)
        {
            while (!RespWriteUtils.TryWriteSimpleString(simpleString, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteSimpleString(ReadOnlySpan<char> simpleString)
        {
            while (!RespWriteUtils.TryWriteSimpleString(simpleString, ref dcurr, dend))
                SendAndReset();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteUtf8BulkString(ReadOnlySpan<char> chars)
        {
            while (!RespWriteUtils.TryWriteUtf8BulkString(chars, ref dcurr, dend))
                SendAndReset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WriteVerbatimString(scoped ReadOnlySpan<byte> item, scoped ReadOnlySpan<byte> ext = default)
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