// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
        /// Initial number of arguments parsed for a command
        /// </summary>
        const int MinParams = 5; // 5 * 20 = 60; around one cache line of 64 bytes

        /// <summary>
        /// Count of accessible arguments for the command
        /// </summary>
        public int Count;

        /// <summary>
        /// Pointer to the slice of <see cref="rootBuffer"/> (which is always pinned) that is accessible within the range of this instance's arguments.
        /// </summary>
        PinnedSpanByte* bufferPtr;

        /// <summary>
        /// Count of arguments in the original buffer
        /// </summary>
        int rootCount;

        /// <summary>
        /// Arguments original buffer (always pinned)
        /// </summary>
        PinnedSpanByte[] rootBuffer;

        /// <summary>
        /// Get a Span of the parsed parameters in the form an PinnedSpanByte
        /// </summary>
        public ReadOnlySpan<PinnedSpanByte> Parameters => new(bufferPtr, Count);

        private SessionParseState(ref PinnedSpanByte[] rootBuffer, int rootCount, ref PinnedSpanByte* bufferPtr, int count) : this()
        {
            this.rootBuffer = rootBuffer;
            this.rootCount = rootCount;
            this.bufferPtr = bufferPtr;
            this.Count = count;
        }

        /// <summary>
        /// Initialize the parse state at the start of a session
        /// </summary>
        public void Initialize()
        {
            Count = 0;
            rootCount = 0;
            rootBuffer = GC.AllocateArray<PinnedSpanByte>(MinParams, true);
            bufferPtr = (PinnedSpanByte*)Unsafe.AsPointer(ref rootBuffer[0]);
        }

        /// <summary>
        /// Initialize the parse state with a given count of arguments
        /// </summary>
        /// <param name="count">Size of argument array to allocate</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize(int count)
        {
            Count = count;
            rootCount = count;

            if (rootBuffer != null && (count <= MinParams || count <= rootBuffer.Length))
                return;

            rootBuffer = GC.AllocateArray<PinnedSpanByte>(count <= MinParams ? MinParams : count, true);
            bufferPtr = (PinnedSpanByte*)Unsafe.AsPointer(ref rootBuffer[0]);
        }

        /// <summary>
        /// Initialize the parse state with one argument
        /// </summary>
        /// <param name="arg">Argument to initialize buffer with</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeWithArgument(PinnedSpanByte arg)
        {
            Initialize(1);

            *bufferPtr = arg;
        }

        /// <summary>
        /// Initialize the parse state with two arguments
        /// </summary>
        /// <param name="arg1">First argument</param>
        /// <param name="arg2">Second argument</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeWithArguments(PinnedSpanByte arg1, PinnedSpanByte arg2)
        {
            Initialize(2);

            *bufferPtr = arg1;
            *(bufferPtr + 1) = arg2;
        }

        /// <summary>
        /// Initialize the parse state with three arguments
        /// </summary>
        /// <param name="arg1">First argument</param>
        /// <param name="arg2">Second argument</param>
        /// <param name="arg3">Third argument</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeWithArguments(PinnedSpanByte arg1, PinnedSpanByte arg2, PinnedSpanByte arg3)
        {
            Initialize(3);

            *bufferPtr = arg1;
            *(bufferPtr + 1) = arg2;
            *(bufferPtr + 2) = arg3;
        }

        /// <summary>
        /// Initialize the parse state with four arguments
        /// </summary>
        /// <param name="arg1">First argument</param>
        /// <param name="arg2">Second argument</param>
        /// <param name="arg3">Third argument</param>
        /// <param name="arg4">Fourth argument</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeWithArguments(PinnedSpanByte arg1, PinnedSpanByte arg2, PinnedSpanByte arg3, PinnedSpanByte arg4)
        {
            Initialize(4);

            *bufferPtr = arg1;
            *(bufferPtr + 1) = arg2;
            *(bufferPtr + 2) = arg3;
            *(bufferPtr + 3) = arg4;
        }

        /// <summary>
        /// Initialize the parse state with four arguments
        /// </summary>
        /// <param name="arg1">First argument</param>
        /// <param name="arg2">Second argument</param>
        /// <param name="arg3">Third argument</param>
        /// <param name="arg4">Fourth argument</param>
        /// <param name="arg5">Fifth argument</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeWithArguments(PinnedSpanByte arg1, PinnedSpanByte arg2, PinnedSpanByte arg3, PinnedSpanByte arg4, PinnedSpanByte arg5)
        {
            Initialize(5);

            *bufferPtr = arg1;
            *(bufferPtr + 1) = arg2;
            *(bufferPtr + 2) = arg3;
            *(bufferPtr + 3) = arg4;
            *(bufferPtr + 4) = arg5;
        }

        /// <summary>
        /// Initialize the parse state with a given set of arguments
        /// </summary>
        /// <param name="args">Set of arguments to initialize buffer with</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeWithArguments(PinnedSpanByte[] args)
        {
            Initialize(args.Length);

            for (var i = 0; i < args.Length; i++)
            {
                *(bufferPtr + i) = args[i];
            }
        }

        /// <summary>
        /// Limit access to the argument buffer to start at a specified index.
        /// </summary>
        /// <param name="idxOffset">Offset value to the underlying buffer</param>
        public SessionParseState Slice(int idxOffset)
        {
            Debug.Assert(idxOffset - 1 < rootCount);

            var count = rootCount - idxOffset;
            var offsetBuffer = bufferPtr + idxOffset;
            return new SessionParseState(ref rootBuffer, rootCount, ref offsetBuffer, count);
        }

        /// <summary>
        /// Limit access to the argument buffer to start at a specified index
        /// and end after a specified number of arguments.
        /// </summary>
        /// <param name="idxOffset">Offset value to the underlying buffer</param>
        /// <param name="count">Argument count</param>
        public SessionParseState Slice(int idxOffset, int count)
        {
            Debug.Assert(idxOffset + count - 1 < rootCount);

            var offsetBuffer = bufferPtr + idxOffset;
            return new SessionParseState(ref rootBuffer, rootCount, ref offsetBuffer, count);
        }

        /// <summary>
        /// Initialize the parse state with a given set of arguments
        /// </summary>
        /// <param name="args">Set of arguments to initialize buffer with</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeWithArguments(ReadOnlySpan<PinnedSpanByte> args)
        {
            Initialize(args.Length);

            for (var i = 0; i < args.Length; i++)
            {
                *(bufferPtr + i) = args[i];
            }
        }

        /// <summary>
        /// Set argument at a specific index
        /// </summary>
        /// <param name="i">Index of buffer at which to set argument</param>
        /// <param name="arg">Argument to set</param>
        public void SetArgument(int i, PinnedSpanByte arg)
        {
            Debug.Assert(i < Count);
            *(bufferPtr + i) = arg;
        }

        /// <summary>
        /// Set arguments starting at a specific index
        /// </summary>
        /// <param name="i">Index of buffer at which to start setting arguments</param>
        /// <param name="args">Arguments to set</param>
        public void SetArguments(int i, params PinnedSpanByte[] args)
        {
            Debug.Assert(i + args.Length - 1 < Count);
            for (var j = 0; j < args.Length; j++)
                *(bufferPtr + i + j) = args[j];
        }

        /// <summary>
        /// Set arguments starting at a specific index
        /// </summary>
        /// <param name="i">Index of buffer at which to start setting arguments</param>
        /// <param name="args">Arguments to set</param>
        public void SetArguments(int i, params ReadOnlySpan<PinnedSpanByte> args)
        {
            Debug.Assert(i + args.Length - 1 < Count);
            for (var j = 0; j < args.Length; j++)
                *(bufferPtr + i + j) = args[j];
        }

        /// <summary>
        /// Get serialized length of parse state
        /// </summary>
        /// <returns>The serialized length</returns>
        public int GetSerializedLength()
        {
            var serializedLength = sizeof(int);

            for (var i = 0; i < Count; i++)
                serializedLength += (*(bufferPtr + i)).TotalSize;
            return serializedLength;
        }

        /// <summary>
        /// Serialize parse state to memory buffer
        /// when arguments are only serialized starting at a specified index
        /// </summary>
        /// <param name="dest">The memory buffer to serialize into (of size at least SerializedLength(firstIdx) bytes)</param>
        /// <param name="length">Length of buffer to serialize into.</param>
        /// <returns>Total serialized bytes</returns>
        public int SerializeTo(byte* dest, int length)
        {
            var curr = dest;

            // Serialize argument count
            *(int*)curr = Count;
            curr += sizeof(int);

            // Serialize arguments
            for (var i = 0; i < Count; i++)
            {
                var argument = *(bufferPtr + i);
                argument.SerializeTo(curr);
                curr += argument.TotalSize;
            }

            return (int)(curr - dest);
        }

        /// <summary>
        /// Deserialize parse state from memory buffer into current struct
        /// </summary>
        /// <param name="src">Memory buffer to deserialize from</param>
        /// <returns>Number of deserialized bytes</returns>
        public unsafe int DeserializeFrom(byte* src)
        {
            var curr = src;

            var argCount = *(int*)curr;
            curr += sizeof(int);

            Initialize(argCount);

            for (var i = 0; i < argCount; i++)
            {
                var argument = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
                *(bufferPtr + i) = argument;
                curr += argument.TotalSize;
            }

            return (int)(curr - src);
        }

        /// <summary>
        /// Read the next argument from the input buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read(int i, ref byte* ptr, byte* end)
        {
            Debug.Assert(i < Count);
            ref var slice = ref Unsafe.AsRef<PinnedSpanByte>(bufferPtr + i);

            // Parse RESP string header
            if (!RespReadUtils.TryReadUnsignedLengthHeader(out var length, ref ptr, end))
                return false;
            slice.Set(ptr, length);

            // Parse content: ensure that input contains key + '\r\n'
            ptr += slice.Length + 2;
            if (ptr > end)
                return false;

            if (*(ushort*)(ptr - 2) != MemoryMarshal.Read<ushort>("\r\n"u8))
                RespParsingException.ThrowUnexpectedToken(*(ptr - 2));

            return true;
        }

        /// <summary>
        /// Get the argument at the given index
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref PinnedSpanByte GetArgSliceByRef(int i)
        {
            Debug.Assert(i < Count);
            return ref Unsafe.AsRef<PinnedSpanByte>(bufferPtr + i);
        }

        /// <summary>
        /// Get int argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetInt(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadInt(*(bufferPtr + i));
        }

        /// <summary>
        /// Try to get int argument at the given index
        /// </summary>
        /// <returns>True if integer parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetInt(int i, out int value)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadInt(*(bufferPtr + i), out value);
        }

        /// <summary>
        /// Get long argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetLong(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadLong(*(bufferPtr + i));
        }

        /// <summary>
        /// Try to get long argument at the given index
        /// </summary>
        /// <returns>True if long parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetLong(int i, out long value)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadLong(*(bufferPtr + i), out value);
        }

        /// <summary>
        /// Get double argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double GetDouble(int i, bool canBeInfinite = true)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadDouble(Unsafe.AsRef<PinnedSpanByte>(bufferPtr + i), canBeInfinite);
        }

        /// <summary>
        /// Try to get double argument at the given index
        /// </summary>
        /// <returns>True if double parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetDouble(int i, out double value, bool canBeInfinite = true)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadDouble(Unsafe.AsRef<PinnedSpanByte>(bufferPtr + i), out value, canBeInfinite);
        }

        /// <summary>
        /// Get ASCII string argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string GetString(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadString(*(bufferPtr + i));
        }

        /// <summary>
        /// Get boolean argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetBool(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadBool(*(bufferPtr + i));
        }

        /// <summary>
        /// Try to get boolean argument at the given index
        /// </summary>
        /// <returns>True if boolean parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetBool(int i, out bool value)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadBool(*(bufferPtr + i), out value);
        }
    }
}