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
        /// Get a Span of the parsed parameters in the form an PinnedSpanByte
        /// </summary>
        public readonly ReadOnlySpan<PinnedSpanByte> Parameters => new(bufferPtr, Count);

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private SessionParseState(ref PinnedSpanByte[] rootBuffer, int rootCount, PinnedSpanByte* bufferPtr, int count)
        {
            this.rootBuffer = rootBuffer;
            this.rootCount = rootCount;
            this.bufferPtr = bufferPtr;
            Count = count;
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
                *(bufferPtr + i) = args[i];
        }

        /// <summary>
        /// Ensure the argument buffer can hold at least <paramref name="capacity"/> entries
        /// from the current slice offset, preserving existing contents. No-op if already large enough.
        /// </summary>
        public void EnsureCapacity(int capacity)
        {
            var oldBuffer = rootBuffer;

            // Compute slice offset (bufferPtr may point into the middle of rootBuffer)
            var sliceOffset = oldBuffer != null
                ? (int)(bufferPtr - (PinnedSpanByte*)Unsafe.AsPointer(ref oldBuffer[0]))
                : 0;

            // Total buffer size needed = slice offset + requested capacity
            var requiredLength = sliceOffset + capacity;

            if (oldBuffer != null && requiredLength <= oldBuffer.Length)
                return;

            var oldCount = Count;
            Initialize(requiredLength);

            if (oldBuffer != null)
            {
                // Copy all data up to the end of the current slice
                var copyLength = sliceOffset + oldCount;
                if (copyLength > 0)
                    oldBuffer.AsSpan(0, copyLength).CopyTo(rootBuffer);
            }

            // Restore slice offset and count
            bufferPtr = (PinnedSpanByte*)Unsafe.AsPointer(ref rootBuffer[0]) + sliceOffset;
            Count = oldCount;
        }

        /// <summary>
        /// Limit access to the argument buffer to start at a specified index.
        /// </summary>
        /// <param name="idxOffset">Offset value to the underlying buffer</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SessionParseState Slice(int idxOffset)
        {
            Debug.Assert(idxOffset - 1 < rootCount);
            return new SessionParseState(ref rootBuffer, rootCount, bufferPtr: bufferPtr + idxOffset, count: rootCount - idxOffset);
        }

        /// <summary>
        /// Limit access to the argument buffer to start at a specified index
        /// and end after a specified number of arguments.
        /// </summary>
        /// <param name="idxOffset">Offset value to the underlying buffer</param>
        /// <param name="count">Argument count</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SessionParseState Slice(int idxOffset, int count)
        {
            Debug.Assert(idxOffset + count - 1 < rootCount);
            return new SessionParseState(ref rootBuffer, rootCount, bufferPtr: bufferPtr + idxOffset, count);
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
                *(bufferPtr + i) = args[i];
        }

        /// <summary>
        /// Set argument at a specific index
        /// </summary>
        /// <param name="i">Index of buffer at which to set argument</param>
        /// <param name="arg">Argument to set</param>
        public void SetArgument(int i, PinnedSpanByte arg)
        {
            Debug.Assert(i < rootBuffer.Length);
            *(bufferPtr + i) = arg;

            if (i >= Count)
                Count = i + 1;
        }

        /// <summary>
        /// Set arguments starting at a specific index
        /// </summary>
        /// <param name="i">Index of buffer at which to start setting arguments</param>
        /// <param name="args">Arguments to set</param>
        public readonly void SetArguments(int i, params ReadOnlySpan<PinnedSpanByte> args)
        {
            Debug.Assert(i + args.Length - 1 < Count);
            for (var j = 0; j < args.Length; j++)
                *(bufferPtr + i + j) = args[j];
        }

        /// <summary>
        /// Get serialized length of parse state
        /// </summary>
        /// <returns>The serialized length</returns>
        public readonly int GetSerializedLength()
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
        public readonly int SerializeTo(byte* dest, int length)
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
        public int DeserializeFrom(byte* src)
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
        public readonly bool Read(int i, ref byte* ptr, byte* end)
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
        public readonly ref PinnedSpanByte GetArgSliceByRef(int i)
        {
            Debug.Assert(i < Count);
            return ref Unsafe.AsRef<PinnedSpanByte>(bufferPtr + i);
        }

        /// <summary>
        /// Get int argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly int GetInt(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadInt(*(bufferPtr + i));
        }

        /// <summary>
        /// Try to get int argument at the given index
        /// </summary>
        /// <returns>True if integer parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryGetInt(int i, out int value)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadInt(*(bufferPtr + i), out value);
        }

        /// <summary>
        /// Get long argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetLong(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadLong(*(bufferPtr + i));
        }

        /// <summary>
        /// Try to get long argument at the given index
        /// </summary>
        /// <returns>True if long parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryGetLong(int i, out long value)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadLong(*(bufferPtr + i), out value);
        }

        /// <summary>
        /// Try to get long argument at the given index
        /// </summary>
        /// <returns>True if long parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryGetLong(int i, bool allowLeadingZeros, out long value)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadLong(*(bufferPtr + i), allowLeadingZeros, out value);
        }

        /// <summary>
        /// Get double argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly double GetDouble(int i, bool canBeInfinite = true)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadDouble(Unsafe.AsRef<PinnedSpanByte>(bufferPtr + i), canBeInfinite);
        }

        /// <summary>
        /// Try to get double argument at the given index
        /// </summary>
        /// <returns>True if double parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryGetDouble(int i, out double value, bool canBeInfinite = true)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadDouble(Unsafe.AsRef<PinnedSpanByte>(bufferPtr + i), out value, canBeInfinite);
        }

        /// <summary>
        /// Get float argument at the given index
        /// </summary>
        /// <returns>True if double parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly float GetFloat(int i, bool canBeInfinite = true)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadFloat(Unsafe.AsRef<PinnedSpanByte>(bufferPtr + i), canBeInfinite);
        }

        /// <summary>
        /// Try to get double argument at the given index
        /// </summary>
        /// <returns>True if double parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryGetFloat(int i, out float value, bool canBeInfinite = true)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadFloat(Unsafe.AsRef<PinnedSpanByte>(bufferPtr + i), out value, canBeInfinite);
        }

        /// <summary>
        /// Get ASCII string argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly string GetString(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadString(*(bufferPtr + i));
        }

        /// <summary>
        /// Get boolean argument at the given index
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool GetBool(int i)
        {
            Debug.Assert(i < Count);
            return ParseUtils.ReadBool(*(bufferPtr + i));
        }

        /// <summary>
        /// Try to get boolean argument at the given index
        /// </summary>
        /// <returns>True if boolean parsed successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryGetBool(int i, out bool value)
        {
            Debug.Assert(i < Count);
            return ParseUtils.TryReadBool(*(bufferPtr + i), out value);
        }
    }
}