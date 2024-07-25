﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.common.Parsing;

namespace Garnet.server
{
    /// <summary>
    /// Wrapper to hold parse state for a RESP session.
    /// </summary>
    public unsafe struct SessionParseState
    {
        /// <summary>
        /// Initial number of arguments parsed for a command
        /// </summary>
        const int MinParams = 5; // 5 * 20 = 60; around one cache line of 64 bytes

        /// <summary>
        /// Count of arguments for the command
        /// </summary>
        public int count;

        /// <summary>
        /// Pinned buffer of arguments
        /// </summary>
        ArgSlice[] buffer;

        /// <summary>
        /// Pointer to buffer
        /// </summary>
        ArgSlice* bufferPtr;

        /// <summary>
        /// Get a Span of the parsed parameters in the form an ArgSlice
        /// </summary>
        public readonly Span<ArgSlice> Parameters => buffer.AsSpan().Slice(0, count);

        /// <summary>
        /// Initialize the parse state at the start of a session
        /// </summary>
        public void Initialize()
        {
            count = 0;
            buffer = GC.AllocateArray<ArgSlice>(MinParams, true);
            bufferPtr = (ArgSlice*)Unsafe.AsPointer(ref buffer[0]);
        }

        /// <summary>
        /// Initialize the parse state with a given count of arguments
        /// </summary>
        /// <param name="count"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize(int count)
        {
            this.count = count;

            if (count <= MinParams || count <= buffer.Length)
                return;

            buffer = GC.AllocateArray<ArgSlice>(count, true);
            bufferPtr = (ArgSlice*)Unsafe.AsPointer(ref buffer[0]);
        }

        /// <summary>
        /// Read the next argument from the input buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read(int i, ref byte* ptr, byte* end)
        {
            Debug.Assert(i < count);
            ref var slice = ref Unsafe.AsRef<ArgSlice>(bufferPtr + i);

            // Parse RESP string header
            if (!RespReadUtils.ReadUnsignedLengthHeader(out slice.length, ref ptr, end))
            {
                return false;
            }

            slice.ptr = ptr;

            // Parse content: ensure that input contains key + '\r\n'
            ptr += slice.length + 2;
            if (ptr > end)
            {
                return false;
            }

            if (*(ushort*)(ptr - 2) != MemoryMarshal.Read<ushort>("\r\n"u8))
            {
                RespParsingException.ThrowUnexpectedToken(*(ptr - 2));
            }

            return true;
        }

        /// <summary>
        /// Get the argument at the given index
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref ArgSlice GetArgSliceByRef(int i)
        {
            Debug.Assert(i < count);
            return ref Unsafe.AsRef<ArgSlice>(bufferPtr + i);
        }

        /// <summary>
        /// Get int argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetInt(int i)
        {
            Debug.Assert(i < count);
            return ParseUtils.ReadInt(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i));
        }

        /// <summary>
        /// Try to get int argument at the given index
        /// </summary>
        /// <returns>True if integer parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetInt(int i, out int value)
        {
            Debug.Assert(i < count);
            return ParseUtils.TryReadInt(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i), out value);
        }

        /// <summary>
        /// Get long argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetLong(int i)
        {
            Debug.Assert(i < count);
            return ParseUtils.ReadLong(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i));
        }

        /// <summary>
        /// Try to get long argument at the given index
        /// </summary>
        /// <returns>True if long parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetLong(int i, out long value)
        {
            Debug.Assert(i < count);
            return ParseUtils.TryReadLong(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i), out value);
        }

        /// <summary>
        /// Get ASCII string argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string GetString(int i)
        {
            Debug.Assert(i < count);
            return ParseUtils.ReadString(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i));
        }
    }
}