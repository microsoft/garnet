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

        /// <summary>
        /// Flag indicating if a SET operation should retain the etag of the previous value if it exists.
        /// This is used for conditional setting.
        /// </summary>
        RetainEtag = 129,

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
        /// Set "RetainEtag" flag, used to update the old etag of a key after conditionally setting it
        /// </summary>
        internal unsafe void SetRetainEtagFlag() => flags |= RespInputFlags.RetainEtag;

        /// <summary>
        /// Check if the RetainEtagFlag is set
        /// </summary>
        /// <returns></returns>
        internal unsafe bool CheckRetainEtagFlag() => (flags & RespInputFlags.RetainEtag) != 0;

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
    }

    /// <summary>
    /// Header for Garnet Object Store inputs
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct ObjectInput
    {
        /// <summary>
        /// Size of header
        /// </summary>
        public const int Size = RespInputHeader.Size + (3 * sizeof(int)) + SessionParseState.Size;

        /// <summary>
        /// Common input header for Garnet
        /// </summary>
        [FieldOffset(0)]
        public RespInputHeader header;

        /// <summary>
        /// Argument for generic usage by command implementation
        /// </summary>
        [FieldOffset(RespInputHeader.Size)]
        public int arg1;

        /// <summary>
        /// Argument for generic usage by command implementation
        /// </summary>
        [FieldOffset(RespInputHeader.Size + sizeof(int))]
        public int arg2;

        /// <summary>
        /// First index to start reading the parse state for command execution
        /// </summary>
        [FieldOffset(RespInputHeader.Size + (2 * sizeof(int)))]
        public int parseStateStartIdx;

        /// <summary>
        /// Session parse state
        /// </summary>
        [FieldOffset(RespInputHeader.Size + (3 * sizeof(int)))]
        public SessionParseState parseState;

        /// <summary>
        /// Gets a pointer to the top of the header
        /// </summary>
        /// <returns>Pointer</returns>
        public unsafe byte* ToPointer()
            => (byte*)Unsafe.AsPointer(ref header);

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