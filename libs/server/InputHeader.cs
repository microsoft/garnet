// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

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
        internal const byte FlagMask = (byte)RespInputFlags.Deterministic - 1;

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
    }

    /// <summary>
    /// Object input header, building on the basic RESP input header
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    struct ObjectInputHeader
    {
        public const int Size = RespInputHeader.Size + sizeof(int) + sizeof(int);

        [FieldOffset(0)]
        public RespInputHeader header;
        [FieldOffset(RespInputHeader.Size)]
        public int count;
        [FieldOffset(RespInputHeader.Size + sizeof(int))]
        public int done;
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
        public const int Size = 12;

        /// <summary>
        /// Amount of items processed
        /// </summary>
        [FieldOffset(0)]
        public int countDone;

        /// <summary>
        /// Bytes that were read
        /// </summary>
        [FieldOffset(4)]
        public int bytesDone;

        /// <summary>
        /// Amount of ops completed
        /// </summary>
        [FieldOffset(8)]
        public int opsDone;
    }
}