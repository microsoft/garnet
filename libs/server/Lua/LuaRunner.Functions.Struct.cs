// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

/******************************************************************************
* Copyright (C) 2010-2018 Lua.org, PUC-Rio.  All rights reserved.
*
* Permission is hereby granted, free of charge, to any person obtaining
* a copy of this software and associated documentation files (the
* "Software"), to deal in the Software without restriction, including
* without limitation the rights to use, copy, modify, merge, publish,
* distribute, sublicense, and/or sell copies of the Software, and to
* permit persons to whom the Software is furnished to do so, subject to
* the following conditions:
*
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
* MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
* IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
* CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
* TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
* SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
******************************************************************************/

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.InteropServices;
using KeraLua;

namespace Garnet.server
{
    // All "called from Lua"-functions and details go here
    //
    // One import thing is NO exceptions can bubble out of these functions
    // or .NET is going to crash horribly.
    internal sealed partial class LuaRunner
    {
        [StructLayout(LayoutKind.Sequential)]
        internal struct Header
        {
            public int Endian;
            public int Align;
        }

        internal enum Endian
        {
            BIG = 0,
            LITTLE = 1,
        }

        internal static class Native
        {
            public static int Endian { get; } = BitConverter.IsLittleEndian ? 1 : 0; // Matches LITTLE = 1, BIG = 0
        }

        static bool TryGetFormat(LuaRunner self, int stackIndex, ref Span<byte> format, out byte[] rentedArr)
        {
            if (self.state.Type(stackIndex) != LuaType.String)
            {
                format = default;
                rentedArr = null;
                return false;
            }

            self.state.KnownStringToBuffer(stackIndex, out var data);
            if (data.Length > format.Length)
            {
                format = rentedArr = ArrayPool<byte>.Shared.Rent(data.Length);
                format = format[..data.Length];
                data.CopyTo(format);
            }
            else
            {
                rentedArr = null;
                data.CopyTo(format);
                format = format[..data.Length];
            }

            self.state.Remove(stackIndex);
            return true;
        }

        static Header DefaultOptions()
        {
            return new Header
            {
                // Check the system's native endianness
                Endian = BitConverter.IsLittleEndian ? 1 : 0,
                // No padding, pack data tightly
                Align = 1
            };
        }

        static bool TryGetOptSize(LuaRunner self, char opt, Span<byte> format, ref int optIx, out int size, out int constStrRegisteryIndex)
        {
            const int MAXINTSIZE = 32;

            switch (opt)
            {
                case 'B':
                case 'b':
                    size = sizeof(byte);
                    constStrRegisteryIndex = -1;
                    return true;
                case 'H':
                case 'h':
                    size = sizeof(short);
                    constStrRegisteryIndex = -1;
                    return true;
                case 'L':
                case 'l':
                    size = sizeof(long);
                    constStrRegisteryIndex = -1;
                    return true;
                case 'T':
                    size = IntPtr.Size;
                    constStrRegisteryIndex = -1;
                    return true;
                case 'f':
                    size = sizeof(float);
                    constStrRegisteryIndex = -1;
                    return true;
                case 'd':
                    size = sizeof(double);
                    constStrRegisteryIndex = -1;
                    return true;
                case 'x':
                    size = 1;
                    constStrRegisteryIndex = -1;
                    return true;
                case 'c':
                    if (!TryGetNum(self, format, ref optIx, 1, out size, out constStrRegisteryIndex))
                    {
                        return false;
                    }

                    constStrRegisteryIndex = -1;
                    return true;
                case 'i':
                case 'I':
                    if (!TryGetNum(self, format, ref optIx, sizeof(int), out size, out constStrRegisteryIndex))
                    {
                        return false;
                    }

                    if (size > MAXINTSIZE)
                    {
                        size = -1;
                        constStrRegisteryIndex = self.constStrs.BadArgFormat;
                        return false;
                    }

                    constStrRegisteryIndex = -1;
                    return true;
                default:
                    size = 0;
                    constStrRegisteryIndex = -1;
                    return true;
            }
        }

        static bool TryGetNum(LuaRunner self, Span<byte> format, ref int optIx, int defaultValue, out int size, out int constStrRegisteryIndex)
        {
            if (optIx >= format.Length || !char.IsDigit((char)format[optIx]))
            {
                size = defaultValue;
                constStrRegisteryIndex = -1;
                return true;
            }

            int result = 0;
            while (optIx < format.Length && char.IsDigit((char)format[optIx]))
            {
                int digit = format[optIx] - '0';
                if (result > (int.MaxValue / 10) || result * 10 > (int.MaxValue - digit))
                {
                    // Integral size overflow
                    size = -1;
                    constStrRegisteryIndex = self.constStrs.BadArgFormat;
                    return false;
                }

                result = result * 10 + digit;
                optIx++;
            }

            size = result;
            constStrRegisteryIndex = -1;
            return true;
        }

        static int GetToAlign(int len, int align, char opt, int size)
        {
            if (size == 0 || opt == 'c')
                return 0;

            if (size > align)
                size = align;

            return (size - (len & (size - 1))) & (size - 1);
        }

        [StructLayout(LayoutKind.Explicit)]
        struct cD
        {
            [FieldOffset(0)]
            public byte c;       // equivalent to char in C

            [FieldOffset(8)]     // manually aligned for 8-byte boundary
            public double d;
        }

        static bool TryParseControlOptions(LuaRunner self, char opt, Span<byte> format, ref Header h, ref int optIx, out int constStrRegisteryIndex)
        {
            int padding = Marshal.SizeOf(typeof(cD)) - sizeof(double);
            int MAXALIGN = padding > sizeof(int) ? padding : sizeof(int);

            switch (opt)
            {
                case ' ':
                    constStrRegisteryIndex = -1;
                    return true;

                case '>':
                    h.Endian = (int)Endian.BIG;

                    constStrRegisteryIndex = -1;
                    return true;

                case '<':
                    h.Endian = (int)Endian.LITTLE;

                    constStrRegisteryIndex = -1;
                    return true;

                case '!':
                    if (!TryGetNum(self, format, ref optIx, MAXALIGN, out int a, out constStrRegisteryIndex))
                    {
                        return false;
                    }

                    if (!BitOperations.IsPow2(a))
                    {
                        // Alignment {a} is not a power of 2
                        constStrRegisteryIndex = self.constStrs.BadArgFormat;
                        return false;
                    }
                    h.Align = a;

                    constStrRegisteryIndex = -1;
                    return true;

                default:
                    constStrRegisteryIndex = self.constStrs.BadArgFormat;
                    return false;
            }
        }

        /// <summary>
        /// Entry point for struct.pack from a Lua script.
        /// </summary>
        internal int StructPack(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var numLuaArgs = state.StackTop;
            Span<byte> format = stackalloc byte[64];

            if (numLuaArgs == 0 || !TryGetFormat(this, 1, ref format, out var rentedArray))
            {
                return LuaWrappedError(1, constStrs.BadArgPack);
            }

            try
            {
                scratchBufferBuilder.Reset();

                // Parse format
                int totalSize = 0;
                int optIx = 0;
                Header h = DefaultOptions();

                while (optIx < format.Length)
                {
                    char opt = (char)format[optIx++];

                    if (!TryGetOptSize(this, opt, format, ref optIx, out int size, out var errOptSizeIndex))
                    {
                        return LuaWrappedError(1, errOptSizeIndex);
                    }

                    int toAlign = GetToAlign(totalSize, h.Align, opt, size);
                    totalSize += toAlign;

                    while (toAlign-- > 0)
                    {
                        AddAlignmentPadding(this);
                    }

                    switch (opt)
                    {
                        case 'b':
                        case 'B':
                        case 'h':
                        case 'H':
                        case 'l':
                        case 'L':
                        case 'T':
                        case 'i':
                        case 'I':
                            {
                                if (!TryEncodeInteger(this, opt, size, 1, h, out var errIndex))
                                {
                                    return LuaWrappedError(1, errIndex);
                                }
                                break;
                            }
                        case 'x':
                            AddAlignmentPadding(this);
                            break;

                        case 'f':
                            {
                                if (!TryEncodeSingle(this, size, 1, h, out var errIndex))
                                {
                                    return LuaWrappedError(1, errIndex);
                                }
                                break;
                            }
                        case 'd':
                            {
                                if (!TryEncodeDouble(this, 1, h, out var errIndex))
                                {
                                    return LuaWrappedError(1, errIndex);
                                }
                                break;
                            }
                        case 'c':
                        case 's':
                            {
                                if (!TryEncodeBytes(this, 1, opt, ref size, out var errIndex))
                                {
                                    return LuaWrappedError(1, errIndex);
                                }
                                break;
                            }
                        default:
                            {
                                if (!TryParseControlOptions(this, opt, format, ref h, ref optIx, out var errIndex))
                                {
                                    return LuaWrappedError(1, errIndex);
                                }
                                break;
                            }
                    }

                    totalSize += size;
                }

                // After all encoding, stack should be empty
                state.ExpectLuaStackEmpty();

                var ret = scratchBufferBuilder.ViewFullArgSlice().ReadOnlySpan;
                if (!state.TryPushBuffer(ret))
                {
                    return LuaWrappedError(1, constStrs.OutOfMemory);
                }

                return 1;
            }
            finally
            {
                if (rentedArray != null)
                {
                    ArrayPool<byte>.Shared.Return(rentedArray);
                }
            }

            // Helper functions
            static void AddAlignmentPadding(LuaRunner self)
            {
                var into = self.scratchBufferBuilder.ViewRemainingArgSlice(1).Span;
                into[0] = 0x00;
                self.scratchBufferBuilder.MoveOffset(1);
            }

            static bool TryEncodeInteger(LuaRunner self, char opt, int size, int stackIndex, Header h, out int constStrRegisteryIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Number, "Expected number");

                var numRaw = self.state.CheckNumber(stackIndex);
                var value = (long)numRaw;

                switch (opt)
                {
                    case 'b':
                    case 'B':
                        self.scratchBufferBuilder.ViewRemainingArgSlice(size).Span[0] = (byte)value;
                        self.scratchBufferBuilder.MoveOffset(size);
                        break;
                    case 'h':
                        {
                            var into = self.scratchBufferBuilder.ViewRemainingArgSlice(size).Span;

                            if (h.Endian == (int)Endian.BIG)
                            {
                                BinaryPrimitives.WriteInt16BigEndian(into[0..], (short)value);
                            }
                            else
                            {
                                BinaryPrimitives.WriteInt16LittleEndian(into[0..], (short)value);
                            }

                            self.scratchBufferBuilder.MoveOffset(size);
                            break;
                        }
                    case 'H':
                        {
                            var into = self.scratchBufferBuilder.ViewRemainingArgSlice(size).Span;

                            if (h.Endian == (int)Endian.BIG)
                            {
                                BinaryPrimitives.WriteUInt16BigEndian(into[0..], (ushort)value);
                            }
                            else
                            {
                                BinaryPrimitives.WriteUInt16LittleEndian(into[0..], (ushort)value);
                            }

                            self.scratchBufferBuilder.MoveOffset(size);
                            break;
                        }
                    case 'l':
                        {
                            var into = self.scratchBufferBuilder.ViewRemainingArgSlice(size).Span;

                            if (h.Endian == (int)Endian.BIG)
                            {
                                BinaryPrimitives.WriteInt64BigEndian(into[0..], (long)value);
                            }
                            else
                            {
                                BinaryPrimitives.WriteInt64LittleEndian(into[0..], (long)value);
                            }

                            self.scratchBufferBuilder.MoveOffset(size);
                            break;
                        }
                    case 'L':
                        {
                            var into = self.scratchBufferBuilder.ViewRemainingArgSlice(size).Span;

                            if (h.Endian == (int)Endian.BIG)
                            {
                                BinaryPrimitives.WriteUInt64BigEndian(into[0..], (ulong)value);
                            }
                            else
                            {
                                BinaryPrimitives.WriteUInt64LittleEndian(into[0..], (ulong)value);
                            }

                            self.scratchBufferBuilder.MoveOffset(size);
                            break;
                        }
                    case 'T':
                        {
                            var into = self.scratchBufferBuilder.ViewRemainingArgSlice(size).Span;

                            if (h.Endian == (int)Endian.BIG)
                            {
                                BinaryPrimitives.WriteUInt64BigEndian(into[0..], (ulong)value);
                            }
                            else
                            {
                                BinaryPrimitives.WriteUInt64LittleEndian(into[0..], (ulong)value);
                            }

                            self.scratchBufferBuilder.MoveOffset(size);
                            break;
                        }
                    case 'i':
                        {
                            var into = self.scratchBufferBuilder.ViewRemainingArgSlice(size).Span;

                            if (h.Endian == (int)Endian.BIG)
                            {
                                BinaryPrimitives.WriteInt32BigEndian(into[0..], (int)value);
                            }
                            else
                            {
                                BinaryPrimitives.WriteInt32LittleEndian(into[0..], (int)value);
                            }

                            self.scratchBufferBuilder.MoveOffset(size);
                            break;
                        }
                    case 'I':
                        {
                            var into = self.scratchBufferBuilder.ViewRemainingArgSlice(size).Span;

                            if (h.Endian == (int)Endian.BIG)
                            {
                                BinaryPrimitives.WriteUInt32BigEndian(into[0..], (uint)value);
                            }
                            else
                            {
                                BinaryPrimitives.WriteUInt32LittleEndian(into[0..], (uint)value);
                            }

                            self.scratchBufferBuilder.MoveOffset(size);
                            break;
                        }
                    default:
                        break;
                }

                self.state.Remove(stackIndex);

                constStrRegisteryIndex = -1;
                return true;
            }

            static bool TryEncodeSingle(LuaRunner self, int size, int stackIndex, Header h, out int constStrRegisteryIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Number, "Expected number");

                var value = self.state.CheckNumber(stackIndex);

                var into = self.scratchBufferBuilder.ViewRemainingArgSlice(size).Span;

                if (h.Endian == Native.Endian) // header endian == native endian
                {
                    // If header endian matches native endian, pick the method that matches native order
                    if (Native.Endian == (int)Endian.LITTLE)
                        BinaryPrimitives.WriteSingleLittleEndian(into[0..], (float)value);
                    else
                        BinaryPrimitives.WriteSingleBigEndian(into[0..], (float)value);
                }
                else
                {
                    // Endian mismatch: pick the opposite method to get the bytes in header's endian format
                    if (Native.Endian == (int)Endian.LITTLE)
                        BinaryPrimitives.WriteSingleBigEndian(into[0..], (float)value);
                    else
                        BinaryPrimitives.WriteSingleLittleEndian(into[0..], (float)value);
                }

                self.scratchBufferBuilder.MoveOffset(size);
                self.state.Remove(stackIndex);

                constStrRegisteryIndex = -1;
                return true;
            }

            static bool TryEncodeDouble(LuaRunner self, int stackIndex, Header h, out int constStrRegisteryIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.Number, "Expected number");

                var value = self.state.CheckNumber(stackIndex);

                var into = self.scratchBufferBuilder.ViewRemainingArgSlice(8).Span;

                if (h.Endian == Native.Endian) // header endian == native endian
                {
                    // If header endian matches native endian, pick the method that matches native order
                    if (Native.Endian == (int)Endian.LITTLE)
                        BinaryPrimitives.WriteDoubleLittleEndian(into[0..], value);
                    else
                        BinaryPrimitives.WriteDoubleBigEndian(into[0..], value);
                }
                else
                {
                    // Endian mismatch: pick the opposite method to get the bytes in header's endian format
                    if (Native.Endian == (int)Endian.LITTLE)
                        BinaryPrimitives.WriteDoubleBigEndian(into[0..], value);
                    else
                        BinaryPrimitives.WriteDoubleLittleEndian(into[0..], value);
                }

                self.scratchBufferBuilder.MoveOffset(8);
                self.state.Remove(stackIndex);

                constStrRegisteryIndex = -1;
                return true;
            }

            static bool TryEncodeBytes(LuaRunner self, int stackIndex, char opt, ref int size, out int constStrRegisteryIndex)
            {
                Debug.Assert(self.state.Type(stackIndex) == LuaType.String, "Expected string");

                self.state.KnownStringToBuffer(stackIndex, out var data);

                int l = data.Length;

                if (size == 0)
                {
                    size = l;
                }

                if (l < size)
                {
                    // string too short, expected at least {size} bytes but got {l}
                    constStrRegisteryIndex = self.constStrs.BadArgPack;
                    return false;
                }

                // When l > size, that is, the Lua string is longer than the requested size.Only the first size bytes of the string are copied into the buffer. The extra bytes beyond size are ignored.
                var into = self.scratchBufferBuilder.ViewRemainingArgSlice(size).Span;
                data[..size].CopyTo(into[0..]);
                self.scratchBufferBuilder.MoveOffset(size);

                if (opt is 's')
                {
                    AddAlignmentPadding(self);
                    size++;
                }

                self.state.Remove(stackIndex);

                constStrRegisteryIndex = -1;
                return true;
            }
        }

        /// <summary>
        /// Entry point for struct.unpack from a Lua script.
        /// </summary>
        internal int StructUnpack(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var numLuaArgs = state.StackTop;
            Span<byte> format = stackalloc byte[64];

            if (numLuaArgs < 2 || !TryGetFormat(this, 1, ref format, out var rentedArray))
            {
                return LuaWrappedError(0, constStrs.BadArgUnpack);
            }

            // Format is removed from args as reading from the above, here is to check the data is string type
            if (state.Type(1) != LuaType.String)
            {
                return LuaWrappedError(0, constStrs.BadArgUnpack);
            }

            try
            {
                // Remove input arg from lua stack as reading, so stack index is still 1
                state.KnownStringToBuffer(1, out var data);
                state.Remove(1);

                int pos = 0;
                int dataLen = data.Length;
                if (numLuaArgs >= 3) // Only check if the 3rd argument is passed
                {
                    if (state.Type(1) != LuaType.Number || state.CheckNumber(1) <= 0)
                    {
                        return LuaWrappedError(0, constStrs.BadArgUnpack);
                    }
                    pos = (int)state.CheckNumber(1);
                    state.Remove(1);

                    if (pos < 1)
                    {
                        // Offset must be 1 or greater
                        return LuaWrappedError(0, constStrs.BadArgUnpack);
                    }
                    pos--; // Lua indexes are 1-based
                }

                int decodedCount = 0;
                int optIx = 0;
                Header h = DefaultOptions();

                while (optIx < format.Length)
                {
                    char opt = (char)format[optIx++];

                    if (!TryGetOptSize(this, opt, format, ref optIx, out int size, out var errOptSizeIndex))
                    {
                        return LuaWrappedError(0, errOptSizeIndex);
                    }

                    pos += GetToAlign(pos, h.Align, opt, size);

                    if (size > dataLen || pos > (dataLen - size))
                    {
                        // Data string too short
                        return LuaWrappedError(0, constStrs.BadArgUnpack);
                    }

                    // Makes sure there’s enough stack space in Lua for result + pos.
                    if (!state.TryEnsureMinimumStackCapacity(2))
                    {
                        return LuaWrappedError(0, constStrs.InsufficientLuaStackSpace);
                    }

                    switch (opt)
                    {
                        case 'b':
                        case 'B':
                        case 'h':
                        case 'H':
                        case 'l':
                        case 'L':
                        case 'T':
                        case 'i':
                        case 'I':
                            {
                                if (!TryDecodeInteger(this, opt, h, size, pos, data, ref decodedCount, out int errIndex))
                                {
                                    return LuaWrappedError(0, errIndex);
                                }
                                break;
                            }
                        case 'x':
                            break;
                        case 'f':
                            {
                                if (!TryDecodeSingle(this, h, size, pos, data, ref decodedCount, out int errIndex))
                                {
                                    return LuaWrappedError(0, errIndex);
                                }
                                break;
                            }
                        case 'd':
                            {
                                if (!TryDecodeDouble(this, h, size, pos, data, ref decodedCount, out int errIndex))
                                {
                                    return LuaWrappedError(0, errIndex);
                                }
                                break;
                            }
                        case 'c':
                            {
                                if (!TryDecodeCharacter(this, pos, ref size, data, ref decodedCount, out int errIndex))
                                {
                                    return LuaWrappedError(0, errIndex);
                                }
                                break;
                            }
                        case 's':
                            {
                                if (!TryDecodeString(this, pos, ref size, data, ref decodedCount, out int errIndex))
                                {
                                    return LuaWrappedError(0, errIndex);
                                }
                                break;
                            }
                        default:
                            {
                                if (!TryParseControlOptions(this, opt, format, ref h, ref optIx, out var errIndex))
                                {
                                    return LuaWrappedError(0, errIndex);
                                }
                                break;
                            }
                    }
                    pos += size;
                }

                // Next position
                state.PushInteger(pos + 1);

                // Error and count for error_wrapper_rvar
                state.PushNil();
                state.PushInteger(decodedCount + 1);
                state.Rotate(1, 2);

                // Decode data according to format, including pos
                return decodedCount + 3;
            }
            finally
            {
                if (rentedArray != null)
                {
                    ArrayPool<byte>.Shared.Return(rentedArray);
                }
            }

            // Helper functions
            static bool TryDecodeInteger(LuaRunner self, char opt, Header h, int size, int pos, ReadOnlySpan<byte> data, ref int decodedCount, out int constStrErrId)
            {
                ulong l = 0;
                double result = 0;
                // data = data[pos..];
                var slice = data.Slice(pos);

                if (h.Endian == 0) // Big-endian
                {
                    for (int i = 0; i < size; i++)
                    {
                        l <<= 8;
                        l |= slice[i];
                    }
                }
                else // Little-endian
                {
                    for (int i = size - 1; i >= 0; i--)
                    {
                        l <<= 8;
                        l |= slice[i];
                    }
                }

                // Lower case is signed
                if (char.IsLower(opt))
                {
                    // Sign extension for negative values
                    ulong mask = ~0UL << (size * 8 - 1);
                    if ((l & mask) != 0)
                    {
                        l |= mask; // Extend the sign bit
                        long signedValue = unchecked((long)l);
                        result = (double)signedValue;
                    }
                    else
                    {
                        result = (double)(long)l;
                    }
                }
                else
                {
                    result = (double)l;
                }

                self.state.PushNumber(result);
                decodedCount++;

                constStrErrId = -1;
                return true;
            }

            static bool TryDecodeSingle(LuaRunner self, Header h, int size, int pos, ReadOnlySpan<byte> data, ref int decodedCount, out int constStrErrId)
            {
                var slice = data.Slice(pos);
                float value;

                // Reverses the bytes only when the target endianness is different from the system's native one
                if (h.Endian == Native.Endian)
                {
                    if (Native.Endian == (int)Endian.LITTLE)
                        value = BinaryPrimitives.ReadSingleLittleEndian(slice);
                    else
                        value = BinaryPrimitives.ReadSingleBigEndian(slice);
                }
                else
                {
                    if (Native.Endian == (int)Endian.LITTLE)
                        value = BinaryPrimitives.ReadSingleBigEndian(slice);
                    else
                        value = BinaryPrimitives.ReadSingleLittleEndian(slice);
                }


                self.state.PushNumber((double)value);
                decodedCount++;

                constStrErrId = -1;
                return true;
            }

            static bool TryDecodeDouble(LuaRunner self, Header h, int size, int pos, ReadOnlySpan<byte> data, ref int decodedCount, out int constStrErrId)
            {
                var slice = data.Slice(pos);
                double value;

                // Reverses the bytes only when the target endianness is different from the system's native one
                if (h.Endian == Native.Endian)
                {
                    if (Native.Endian == (int)Endian.LITTLE)
                        value = BinaryPrimitives.ReadDoubleLittleEndian(slice);
                    else
                        value = BinaryPrimitives.ReadDoubleBigEndian(slice);
                }
                else
                {
                    if (Native.Endian == (int)Endian.LITTLE)
                        value = BinaryPrimitives.ReadDoubleBigEndian(slice);
                    else
                        value = BinaryPrimitives.ReadDoubleLittleEndian(slice);
                }


                self.state.PushNumber(value);
                decodedCount++;

                constStrErrId = -1;
                return true;
            }

            static bool TryDecodeCharacter(LuaRunner self, int pos, ref int size, ReadOnlySpan<byte> data, ref int decodedCount, out int constStrErrId)
            {
                var slice = data.Slice(pos);

                if (size == 0)
                {
                    int previousArgIndex = self.state.StackTop;

                    // if c0, then get the size from the previous decoded number
                    if (decodedCount == 0 || self.state.Type(previousArgIndex) != LuaType.Number)
                    {
                        constStrErrId = self.constStrs.BadArgUnpack;
                        return false;
                    }

                    size = (int)self.state.CheckNumber(previousArgIndex);
                    self.state.Remove(previousArgIndex);
                    decodedCount--;
                }

                if (slice.Length < size)
                {
                    // Data string too short
                    constStrErrId = self.constStrs.BadArgUnpack;
                    return false;
                }

                var str = slice[..size];

                if (!self.state.TryPushBuffer(str))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }

                decodedCount++;

                constStrErrId = -1;
                return true;
            }

            static bool TryDecodeString(LuaRunner self, int pos, ref int size, ReadOnlySpan<byte> data, ref int decodedCount, out int constStrErrId)
            {
                var slice = data.Slice(pos);

                int nullIndex = slice.IndexOf((byte)0);
                if (nullIndex == -1)
                {
                    // Unfinished string in data
                    constStrErrId = self.constStrs.BadArgUnpack;
                    return false;
                }

                size = nullIndex + 1; // Include null terminator counting size
                var str = slice[..(size - 1)]; // Exclude null for pushing result

                if (!self.state.TryPushBuffer(str))
                {
                    constStrErrId = self.constStrs.OutOfMemory;
                    return false;
                }

                decodedCount++;
                constStrErrId = -1;
                return true;
            }
        }

        /// <summary>
        /// Entry point for struct.size from a Lua script.
        /// </summary>
        internal int StructSize(nint luaStatePtr)
        {
            state.CallFromLuaEntered(luaStatePtr);

            var numLuaArgs = state.StackTop;
            Span<byte> format = stackalloc byte[64];

            if (numLuaArgs == 0 || !TryGetFormat(this, 1, ref format, out var rentedArray))
            {
                return LuaWrappedError(1, constStrs.BadArgFormat);
            }

            try
            {
                scratchBufferBuilder.Reset();

                // Parse format
                int totalSize = 0;
                int optIx = 0;
                Header h = DefaultOptions();

                while (optIx < format.Length)
                {
                    char opt = (char)format[optIx++];

                    if (!TryGetOptSize(this, opt, format, ref optIx, out int size, out var errOptSizeIndex))
                    {
                        return LuaWrappedError(1, errOptSizeIndex);
                    }

                    int toAlign = GetToAlign(totalSize, h.Align, opt, size);
                    totalSize += toAlign;

                    if (opt == 's')
                    {
                        return LuaWrappedError(1, constStrs.BadArgFormat);
                    }

                    if (opt == 'c' && size == 0)
                    {
                        return LuaWrappedError(1, constStrs.BadArgFormat);
                    }

                    if (!char.IsLetterOrDigit(opt))
                    {
                        if (!TryParseControlOptions(this, opt, format, ref h, ref optIx, out var errIndex))
                        {
                            return LuaWrappedError(1, errIndex);
                        }
                    }

                    totalSize += size;
                }

                state.PushNumber(totalSize);
                return 1;
            }
            finally
            {
                if (rentedArray != null)
                {
                    ArrayPool<byte>.Shared.Return(rentedArray);
                }
            }
        }
    }
}