﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Reflection.PortableExecutable;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Garnet.common;
using Garnet.common.Parsing;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Wrapper to hold parse state for a RESP session.
    /// </summary>
    public unsafe struct SessionParseState
    {
        /// <summary>
        /// Size of parse state
        /// </summary>
        public const int Size = 12;

        /// <summary>
        /// Initial number of arguments parsed for a command
        /// </summary>
        const int MinParams = 5; // 5 * 20 = 60; around one cache line of 64 bytes

        /// <summary>
        /// Count of arguments for the command
        /// </summary>
        public int Count;

        /// <summary>
        /// Pointer to buffer
        /// </summary>
        ArgSlice* bufferPtr;

        /// <summary>
        /// Arguments buffer
        /// </summary>
        ArgSlice[] buffer;

        /// <summary>
        /// Get a Span of the parsed parameters in the form an ArgSlice
        /// </summary>
        public readonly Span<ArgSlice> Parameters => new(bufferPtr, Count);

        /// <summary>
        /// Initialize the parse state at the start of a session
        /// </summary>
        public void Initialize()
        {
            Count = 0;
            buffer = GC.AllocateArray<ArgSlice>(MinParams, true);
            bufferPtr = (ArgSlice*)Unsafe.AsPointer(ref buffer[0]);
        }

        /// <summary>
        /// Initialize the parse state with a given count of arguments
        /// </summary>
        /// <param name="count">Size of argument array to allocate</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize(int count)
        {
            this.Count = count;

            if (buffer != null && (count <= MinParams || count <= buffer.Length))
                return;

            buffer = GC.AllocateArray<ArgSlice>(count <= MinParams ? MinParams : count, true);
            bufferPtr = (ArgSlice*)Unsafe.AsPointer(ref buffer[0]);
        }

        /// <summary>
        /// Initialize the parse state with a given set of arguments
        /// </summary>
        /// <param name="args">Set of arguments to initialize buffer with</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeWithArguments(params ArgSlice[] args)
        {
            Initialize(args.Length);

            for (var i = 0; i < args.Length; i++)
            {
                buffer[i] = args[i];
            }
        }

        /// <summary>
        /// Set argument at a specific index
        /// </summary>
        /// <param name="i">Index of buffer at which to set argument</param>
        /// <param name="arg">Argument to set</param>
        public void SetArgument(int i, ArgSlice arg)
        {
            Debug.Assert(i < Count);
            buffer[i] = arg;
        }

        /// <summary>
        /// Set arguments starting at a specific index
        /// </summary>
        /// <param name="i">Index of buffer at which to start setting arguments</param>
        /// <param name="args">Arguments to set</param>
        public void SetArguments(int i, params ArgSlice[] args)
        {
            Debug.Assert(i + args.Length - 1 < Count);
            for (var j = 0; j < args.Length; j++)
            {
                buffer[i + j] = args[j];
            }
        }

        /// <summary>
        /// Get serialized length of parse state when arguments are only
        /// serialized starting at a specified index
        /// </summary>
        /// <param name="startIdx">Index from which arguments are serialized</param>
        /// <returns>The serialized length</returns>
        public int GetSerializedLength(int startIdx)
        {
            var serializedLength = sizeof(int);
            var argCount = Count - startIdx;

            if (argCount > 0)
            {
                Debug.Assert(startIdx < Count);

                for (var i = 0; i < argCount; i++)
                {
                    serializedLength += buffer[startIdx + i].SpanByte.TotalSize;
                }
            }

            return serializedLength;
        }

        /// <summary>
        /// Serialize parse state to memory buffer
        /// when arguments are only serialized starting at a specified index
        /// </summary>
        /// <param name="dest">The memory buffer to serialize into (of size at least SerializedLength(startIdx) bytes)</param>
        /// <param name="startIdx">Index from which arguments are serialized</param>
        /// <returns>Total serialized bytes</returns>
        public int CopyTo(byte* dest, int startIdx)
        {
            var curr = dest;

            // Serialize argument count
            var argCount = Count - startIdx;
            *(int*)curr = argCount;
            curr += sizeof(int);

            // Serialize arguments
            if (argCount > 0)
            {
                Debug.Assert(startIdx < Count);

                for (var i = 0; i < argCount; i++)
                {
                    var sbParam = buffer[startIdx + i].SpanByte;
                    sbParam.CopyTo(curr);
                    curr += sbParam.TotalSize;
                }
            }

            return (int)(dest - curr);
        }

        /// <summary>
        /// Deserialize parse state from memory buffer into current struct
        /// </summary>
        /// <param name="src">Memory buffer to deserialize from</param>
        public unsafe void DeserializeFrom(byte* src)
        {
            var curr = src;

            var argCount = *(int*)curr;
            curr += sizeof(int);

            if (argCount > 0)
            {
                Initialize(argCount);

                for (var i = 0; i < argCount; i++)
                {
                    ref var sbArgument = ref Unsafe.AsRef<SpanByte>(curr);
                    buffer[i] = new ArgSlice(ref sbArgument);
                    curr += sbArgument.TotalSize;
                }
            }
        }

        /// <summary>
        /// Read the next argument from the input buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read(int i, ref byte* ptr, byte* end)
        {
            Debug.Assert(i < Count);
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
            Debug.Assert(i < Count);
            return ref Unsafe.AsRef<ArgSlice>(bufferPtr + i);
        }

        /// <summary>
        /// Get int argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetInt(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadInt(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i));
        }

        /// <summary>
        /// Try to get int argument at the given index
        /// </summary>
        /// <returns>True if integer parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetInt(int i, out int value)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadInt(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i), out value);
        }

        /// <summary>
        /// Get long argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetLong(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadLong(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i));
        }

        /// <summary>
        /// Try to get long argument at the given index
        /// </summary>
        /// <returns>True if long parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetLong(int i, out long value)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadLong(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i), out value);
        }

        /// <summary>
        /// Get double argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double GetDouble(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadDouble(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i));
        }

        /// <summary>
        /// Try to get double argument at the given index
        /// </summary>
        /// <returns>True if double parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetDouble(int i, out double value)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadDouble(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i), out value);
        }

        /// <summary>
        /// Get ASCII string argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string GetString(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadString(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i));
        }

        /// <summary>
        /// Get enum argument at the given index
        /// Note: this method exists for compatibility with existing code.
        /// For best performance use: ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(""VALUE""u8) to figure out the current enum value
        /// </summary>
        /// <returns>True if enum parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T GetEnum<T>(int i, bool ignoreCase) where T : struct, Enum
        {
            Debug.Assert(i < Count);
            var strRep = GetString(i);
            var value = Enum.Parse<T>(strRep, ignoreCase);
            // Extra check is to avoid numerical values being successfully parsed as enum value
            return string.Equals(Enum.GetName(value), strRep,
                ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal) ? value : default;
        }

        /// <summary>
        /// Try to get enum argument at the given index
        /// Note: this method exists for compatibility with existing code.
        /// For best performance use: ReadOnlySpan.EqualsUpperCaseSpanIgnoringCase(""VALUE""u8) to figure out the current enum value
        /// </summary>
        /// <returns>True if integer parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetEnum<T>(int i, bool ignoreCase, out T value) where T : struct, Enum
        {
            Debug.Assert(i < Count);
            var strRep = GetString(i);
            var successful = Enum.TryParse(strRep, ignoreCase, out value) &&
                             // Extra check is to avoid numerical values being successfully parsed as enum value
                             string.Equals(Enum.GetName(value), strRep,
                                 ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
            if (!successful) value = default;
            return successful;
        }

        /// <summary>
        /// Get boolean argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetBool(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadBool(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i));
        }

        /// <summary>
        /// Try to get boolean argument at the given index
        /// </summary>
        /// <returns>True if boolean parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetBool(int i, out bool value)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadBool(ref Unsafe.AsRef<ArgSlice>(bufferPtr + i), out value);
        }
    }
}