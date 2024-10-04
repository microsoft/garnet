// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Flags used by append-only file (AOF/WAL)
    /// </summary>
    [Flags]
    enum RespInputFlags : byte
    {
        /// <summary>
        /// Flag indicating a SET operation that returns the previous value
        /// </summary>
        SetGet = 32,
        /// <summary>
        /// Deterministic
        /// </summary>
        Deterministic = 64,
        /// <summary>
        /// Expired
        /// </summary>
        Expired = 128,
    }

    /// <summary>
    /// Common input header for Garnet
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct RespInputHeader
    {
        /// <summary>
        /// Size of header
        /// </summary>
        public const int Size = 2;
        internal const byte FlagMask = (byte)RespInputFlags.SetGet - 1;

        [FieldOffset(0)]
        internal RespCommand cmd;

        [FieldOffset(0)]
        internal GarnetObjectType type;

        [FieldOffset(1)]
        internal RespInputFlags flags;

        /// <summary>
        /// Set RESP input header
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        public void SetHeader(byte cmd, byte flags)
        {
            this.cmd = (RespCommand)cmd;
            this.flags = (RespInputFlags)flags;
        }

        internal byte SubId
        {
            get => (byte)((byte)flags & FlagMask);
            set => flags = (RespInputFlags)(((byte)flags & ~FlagMask) | (byte)value);
        }

        internal SortedSetOperation SortedSetOp
        {
            get => (SortedSetOperation)((byte)flags & FlagMask);
            set => flags = (RespInputFlags)(((byte)flags & ~FlagMask) | (byte)value);
        }

        internal HashOperation HashOp
        {
            get => (HashOperation)((byte)flags & FlagMask);
            set => flags = (RespInputFlags)(((byte)flags & ~FlagMask) | (byte)value);
        }

        internal SetOperation SetOp
        {
            get => (SetOperation)((byte)flags & FlagMask);
            set => flags = (RespInputFlags)(((byte)flags & ~FlagMask) | (byte)value);
        }

        internal ListOperation ListOp
        {
            get => (ListOperation)((byte)flags & FlagMask);
            set => flags = (RespInputFlags)(((byte)flags & ~FlagMask) | (byte)value);
        }

        /// <summary>
        /// Set expiration flag, used for log replay
        /// </summary>
        internal unsafe void SetExpiredFlag() => flags |= RespInputFlags.Expired;

        /// <summary>
        /// Set "SetGet" flag, used to get the old value of a key after conditionally setting it
        /// </summary>
        internal unsafe void SetSetGetFlag() => flags |= RespInputFlags.SetGet;

        /// <summary>
        /// Check if record is expired, either deterministically during log replay,
        /// or based on current time in normal operation.
        /// </summary>
        /// <param name="expireTime">Expiration time</param>
        /// <returns></returns>
        internal unsafe bool CheckExpiry(long expireTime)
        {
            if ((flags & RespInputFlags.Deterministic) != 0)
            {
                if ((flags & RespInputFlags.Expired) != 0)
                    return true;
            }
            else
            {
                if (expireTime < DateTimeOffset.Now.UtcTicks)
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Check the SetGet flag
        /// </summary>
        internal unsafe bool CheckSetGetFlag()
            => (flags & RespInputFlags.SetGet) != 0;

        /// <summary>
        /// Gets a pointer to the top of the header
        /// </summary>
        /// <returns>Pointer</returns>
        public unsafe byte* ToPointer()
            => (byte*)Unsafe.AsPointer(ref cmd);

        /// <summary>
        /// Get header as Span
        /// </summary>
        /// <returns>Span</returns>
        public unsafe Span<byte> AsSpan() => new(ToPointer(), Size);

        /// <summary>
        /// Get header as SpanByte
        /// </summary>
        public unsafe SpanByte SpanByte => new(Length, (nint)ToPointer());

        /// <summary>
        /// Get header length
        /// </summary>
        public int Length => AsSpan().Length;
    }

    /// <summary>
    /// Header for Garnet Object Store inputs
    /// </summary>
    public struct ObjectInput : IStoreInput
    {
        /// <summary>
        /// Common input header for Garnet
        /// </summary>
        public RespInputHeader header;

        /// <summary>
        /// Argument for generic usage by command implementation
        /// </summary>
        public int arg1;

        /// <summary>
        /// Argument for generic usage by command implementation
        /// </summary>
        public int arg2;

        /// <summary>
        /// First index to start reading the parse state for command execution
        /// </summary>
        public int parseStateStartIdx;

        /// <summary>
        /// Session parse state
        /// </summary>
        public SessionParseState parseState;

        /// <inheritdoc />
        public int SerializedLength
        {
            get
            {
                var serializedLength = header.SpanByte.TotalSize
                                       + (3 * sizeof(int)) // Length + arg1 + arg2
                                       + parseState.GetSerializedLength(parseStateStartIdx);

                return serializedLength;
            }
        }

        /// <inheritdoc />
        public unsafe void CopyTo(byte* dest)
        {
            // Leave space for length
            var curr = dest + sizeof(int);

            // Serialize header
            header.SpanByte.CopyTo(curr);
            curr += header.SpanByte.TotalSize;

            // Serialize arg1
            *(int*)curr = arg1;
            curr += sizeof(int);

            // Serialize arg2
            *(int*)curr = arg2;
            curr += sizeof(int);

            // Serialize parse state
            // Only serialize arguments starting from parseStateStartIdx
            var len = parseState.CopyTo(curr, parseStateStartIdx);
            curr += len;

            // Serialize length
            *(int*)dest = (int)(curr - dest - sizeof(int));
        }

        /// <inheritdoc />
        public unsafe void DeserializeFrom(byte* src)
        {
            var curr = src;

            // Deserialize header
            ref var sbHeader = ref Unsafe.AsRef<SpanByte>(curr);
            ref var h = ref Unsafe.AsRef<RespInputHeader>(sbHeader.ToPointer());
            curr += sbHeader.TotalSize;
            header = h;

            // Deserialize arg1
            arg1 = *(int*)curr;
            curr += sizeof(int);

            // Deserialize arg2
            arg2 = *(int*)curr;
            curr += sizeof(int);

            // Deserialize parse state
            parseState.DeserializeFrom(curr);
        }
    }

    /// <summary>
    /// Header for Garnet Main Store inputs
    /// </summary>
    public struct RawStringInput : IStoreInput
    {
        /// <summary>
        /// Common input header for Garnet
        /// </summary>
        public RespInputHeader header;

        /// <summary>
        /// Argument for generic usage by command implementation
        /// </summary>
        public long arg1;

        /// <summary>
        /// First index to start reading the parse state for command execution
        /// </summary>
        public int parseStateStartIdx;

        /// <summary>
        /// Session parse state
        /// </summary>
        public SessionParseState parseState;

        /// <inheritdoc />
        public int SerializedLength
        {
            get
            {
                var serializedLength = sizeof(int) // Length
                                       + header.SpanByte.TotalSize
                                       + sizeof(long) // arg1
                                       + parseState.GetSerializedLength(parseStateStartIdx);

                return serializedLength;
            }
        }

        /// <inheritdoc />
        public unsafe void CopyTo(byte* dest)
        {
            // Leave space for length
            var curr = dest + sizeof(int);

            // Serialize header
            header.SpanByte.CopyTo(curr);
            curr += header.SpanByte.TotalSize;

            // Serialize arg1
            *(long*)curr = arg1;
            curr += sizeof(long);

            // Serialize parse state
            // Only serialize arguments starting from parseStateStartIdx
            var len = parseState.CopyTo(curr, parseStateStartIdx);
            curr += len;

            // Serialize length
            *(int*)dest = (int)(curr - dest - sizeof(int));
        }

        /// <inheritdoc />
        public unsafe void DeserializeFrom(byte* src)
        {
            var curr = src;

            // Deserialize header
            ref var sbHeader = ref Unsafe.AsRef<SpanByte>(curr);
            ref var h = ref Unsafe.AsRef<RespInputHeader>(sbHeader.ToPointer());
            curr += sbHeader.TotalSize;
            header = h;

            // Deserialize arg1
            arg1 = *(long*)curr;
            curr += sizeof(long);

            // Deserialize parse state
            parseState.DeserializeFrom(curr);
        }
    }

    /// <summary>
    /// Object output header (sometimes used as footer)
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct ObjectOutputHeader
    {
        /// <summary>
        /// Expected size of this object
        /// </summary>
        public const int Size = 4;

        /// <summary>
        /// Some result of operation (e.g., number of items added successfully)
        /// </summary>
        [FieldOffset(0)]
        public int result1;
    }
}