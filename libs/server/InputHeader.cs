// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Flags used by append-only file (AOF/WAL)
    /// The byte representation only use the last 3 bits of the byte since the lower 5 bits of the field used to store the flag stores other data in the case of Object types.
    /// In the case of a Rawstring, the last 4 bits are used for flags, and the other 4 bits are unused of the byte.
    /// NOTE: This will soon be expanded as a part of a breaking change to make WithEtag bit compatible with object store as well.
    /// </summary>
    [Flags]
    public enum RespInputFlags : byte
    {
        /// <summary>
        /// Flag indicating an operation intending to add an etag for a RAWSTRING command.
        /// </summary>
        WithEtag = 16,

        /// <summary>
        /// Flag indicating a SET operation that returns the previous value (for strings).
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
        public const int Size = 3;

        // Since we know WithEtag is not used with any Object types, we keep the flag mask to work with the last 3 bits as flags,
        // and the other 5 bits for storing object associated flags. However, in the case of Rawstring we use the last 4 bits for flags, and let the others remain unused.
        internal const byte FlagMask = (byte)RespInputFlags.SetGet - 1;

        [FieldOffset(0)]
        internal RespCommand cmd;

        [FieldOffset(0)]
        internal GarnetObjectType type;

        [FieldOffset(2)]
        internal RespInputFlags flags;

        /// <summary>
        /// Create a new instance of RespInputHeader
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        public RespInputHeader(RespCommand cmd, RespInputFlags flags = 0)
        {
            this.cmd = cmd;
            this.flags = flags;
        }

        /// <summary>
        /// Create a new instance of RespInputHeader
        /// </summary>
        /// <param name="type">Object type</param>
        /// <param name="flags">Flags</param>
        public RespInputHeader(GarnetObjectType type, RespInputFlags flags = 0)
        {
            this.type = type;
            this.flags = flags;
        }

        /// <summary>
        /// Set RESP input header
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        public void SetHeader(ushort cmd, byte flags)
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
        /// Set "WithEtag" flag for the input header
        /// </summary>
        internal void SetWithETagFlag() => flags |= RespInputFlags.WithEtag;

        /// <summary>
        /// Check if the WithEtag flag is set
        /// </summary>
        /// <returns></returns>
        internal bool CheckWithETagFlag() => (flags & RespInputFlags.WithEtag) != 0;

        /// <summary>
        /// Check if record is expired, either deterministically during log replay,
        /// or based on current time in normal operation.
        /// </summary>
        /// <param name="expireTime">Expiration time</param>
        /// <returns></returns>
        internal readonly unsafe bool CheckExpiry(long expireTime)
            => (flags & RespInputFlags.Deterministic) != 0
                ? (flags & RespInputFlags.Expired) != 0
                : expireTime < DateTimeOffset.Now.UtcTicks;

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
        /// Get header as PinnedSpanByte
        /// </summary>
        public unsafe PinnedSpanByte SpanByte => PinnedSpanByte.FromPinnedPointer(ToPointer(), Length);

        /// <summary>
        /// Get header length
        /// </summary>
        public int Length => Size;
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
        /// Session parse state
        /// </summary>
        public SessionParseState parseState;

        /// <summary>
        /// Create a new instance of ObjectInput
        /// </summary>
        /// <param name="header">Input header</param>
        /// <param name="arg1">First general-purpose argument</param>
        /// <param name="arg2">Second general-purpose argument</param>
        public ObjectInput(RespInputHeader header, int arg1 = 0, int arg2 = 0)
        {
            this.header = header;
            this.arg1 = arg1;
            this.arg2 = arg2;
        }

        /// <summary>
        /// Create a new instance of ObjectInput
        /// </summary>
        /// <param name="header">Input header</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="arg1">First general-purpose argument</param>
        /// <param name="arg2">Second general-purpose argument</param>
        public ObjectInput(RespInputHeader header, ref SessionParseState parseState, int arg1 = 0, int arg2 = 0)
            : this(header, arg1, arg2)
        {
            this.parseState = parseState;
        }

        /// <summary>
        /// Create a new instance of ObjectInput
        /// </summary>
        /// <param name="header">Input header</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="startIdx">First command argument index in parse state</param>
        /// <param name="arg1">First general-purpose argument</param>
        /// <param name="arg2">Second general-purpose argument</param>
        public ObjectInput(RespInputHeader header, ref SessionParseState parseState, int startIdx, int arg1 = 0, int arg2 = 0)
            : this(header, arg1, arg2)
        {
            this.parseState = parseState.Slice(startIdx);
        }

        /// <inheritdoc />
        public int SerializedLength => header.SpanByte.TotalSize
                                       + (2 * sizeof(int)) // arg1 + arg2
                                       + parseState.GetSerializedLength();

        /// <inheritdoc />
        public unsafe int CopyTo(byte* dest, int length)
        {
            Debug.Assert(length >= this.SerializedLength);

            var curr = dest;

            // Serialize header
            header.SpanByte.SerializeTo(curr);
            curr += header.SpanByte.TotalSize;

            // Serialize arg1
            *(int*)curr = arg1;
            curr += sizeof(int);

            // Serialize arg2
            *(int*)curr = arg2;
            curr += sizeof(int);

            // Serialize parse state
            var remainingLength = length - (int)(curr - dest);
            var len = parseState.SerializeTo(curr, remainingLength);
            curr += len;

            // Number of serialized bytes
            return (int)(curr - dest);
        }

        /// <inheritdoc />
        public unsafe int DeserializeFrom(byte* src)
        {
            var curr = src;

            // Deserialize header
            var header = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            ref var h = ref Unsafe.AsRef<RespInputHeader>(header.ToPointer());
            curr += header.TotalSize;
            this.header = h;

            // Deserialize arg1
            arg1 = *(int*)curr;
            curr += sizeof(int);

            // Deserialize arg2
            arg2 = *(int*)curr;
            curr += sizeof(int);

            // Deserialize parse state
            var len = parseState.DeserializeFrom(curr);
            curr += len;

            return (int)(src - curr);
        }
    }

    /// <summary>
    /// Header for Garnet Main Store inputs
    /// </summary>
    public struct StringInput : IStoreInput
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
        /// Session parse state
        /// </summary>
        public SessionParseState parseState;

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        /// <param name="arg1">General-purpose argument</param>
        public StringInput(RespCommand cmd, RespInputFlags flags = 0, long arg1 = 0)
        {
            this.header = new RespInputHeader(cmd, flags);
            this.arg1 = arg1;
        }

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        /// <param name="arg1">General-purpose argument</param>
        public StringInput(ushort cmd, byte flags = 0, long arg1 = 0) :
            this((RespCommand)cmd, (RespInputFlags)flags, arg1)

        {
        }

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="arg1">General-purpose argument</param>
        /// <param name="flags">Flags</param>
        public StringInput(RespCommand cmd, ref SessionParseState parseState, long arg1 = 0, RespInputFlags flags = 0) : this(cmd, flags, arg1)
        {
            this.parseState = parseState;
        }

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="startIdx">First command argument index in parse state</param>
        /// <param name="arg1">General-purpose argument</param>
        /// <param name="flags">Flags</param>
        public StringInput(RespCommand cmd, ref SessionParseState parseState, int startIdx, long arg1 = 0, RespInputFlags flags = 0) : this(cmd, flags, arg1)
        {
            this.parseState = parseState.Slice(startIdx);
        }

        /// <inheritdoc />
        public int SerializedLength => header.SpanByte.TotalSize
                                       + sizeof(long) // arg1
                                       + parseState.GetSerializedLength();

        /// <inheritdoc />
        public unsafe int CopyTo(byte* dest, int length)
        {
            Debug.Assert(length >= this.SerializedLength);

            var curr = dest;

            // Serialize header
            header.SpanByte.SerializeTo(curr);
            curr += header.SpanByte.TotalSize;

            // Serialize arg1
            *(long*)curr = arg1;
            curr += sizeof(long);

            // Serialize parse state
            var remainingLength = length - (int)(curr - dest);
            var len = parseState.SerializeTo(curr, remainingLength);
            curr += len;

            // Serialize length
            return (int)(curr - dest);
        }

        /// <inheritdoc />
        public unsafe int DeserializeFrom(byte* src)
        {
            var curr = src;

            // Deserialize header
            var header = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            ref var h = ref Unsafe.AsRef<RespInputHeader>(header.ToPointer());
            curr += header.TotalSize;
            this.header = h;

            // Deserialize arg1
            arg1 = *(long*)curr;
            curr += sizeof(long);

            // Deserialize parse state
            var len = parseState.DeserializeFrom(curr);
            curr += len;

            return (int)(curr - src);
        }
    }

    /// <summary>
    /// Header for Garnet Unified Store inputs
    /// </summary>
    public struct UnifiedInput : IStoreInput
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
        /// Session parse state
        /// </summary>
        public SessionParseState parseState;

        /// <summary>
        /// Create a new instance of UnifiedInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        /// <param name="arg1">General-purpose argument</param>
        public UnifiedInput(RespCommand cmd, RespInputFlags flags = 0, long arg1 = 0)
        {
            this.header = new RespInputHeader(cmd, flags);
            this.arg1 = arg1;
        }

        /// <summary>
        /// Create a new instance of UnifiedInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        /// <param name="arg1">General-purpose argument</param>
        public UnifiedInput(ushort cmd, byte flags = 0, long arg1 = 0) :
            this((RespCommand)cmd, (RespInputFlags)flags, arg1)

        {
        }

        /// <summary>
        /// Create a new instance of UnifiedInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="arg1">General-purpose argument</param>
        /// <param name="flags">Flags</param>
        public UnifiedInput(RespCommand cmd, ref SessionParseState parseState, long arg1 = 0, RespInputFlags flags = 0) : this(cmd, flags, arg1)
        {
            this.parseState = parseState;
        }

        /// <summary>
        /// Create a new instance of UnifiedInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="startIdx">First command argument index in parse state</param>
        /// <param name="arg1">General-purpose argument</param>
        /// <param name="flags">Flags</param>
        public UnifiedInput(RespCommand cmd, ref SessionParseState parseState, int startIdx, long arg1 = 0, RespInputFlags flags = 0) : this(cmd, flags, arg1)
        {
            this.parseState = parseState.Slice(startIdx);
        }

        /// <inheritdoc />
        public int SerializedLength => header.SpanByte.TotalSize
                                       + sizeof(long) // arg1
                                       + parseState.GetSerializedLength();

        /// <inheritdoc />
        public unsafe int CopyTo(byte* dest, int length)
        {
            Debug.Assert(length >= this.SerializedLength);

            var curr = dest;

            // Serialize header
            header.SpanByte.SerializeTo(curr);
            curr += header.SpanByte.TotalSize;

            // Serialize arg1
            *(long*)curr = arg1;
            curr += sizeof(long);

            // Serialize parse state
            var remainingLength = length - (int)(curr - dest);
            var len = parseState.SerializeTo(curr, remainingLength);
            curr += len;

            // Serialize length
            return (int)(curr - dest);
        }

        /// <inheritdoc />
        public unsafe int DeserializeFrom(byte* src)
        {
            var curr = src;

            // Deserialize header
            var header = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            ref var h = ref Unsafe.AsRef<RespInputHeader>(header.ToPointer());
            curr += header.TotalSize;
            this.header = h;

            // Deserialize arg1
            arg1 = *(long*)curr;
            curr += sizeof(long);

            // Deserialize parse state
            var len = parseState.DeserializeFrom(curr);
            curr += len;

            return (int)(curr - src);
        }
    }

    /// <summary>
    /// Header for Garnet Main Store inputs
    /// </summary>
    public struct UnifiedStoreInput : IStoreInput
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
        /// Session parse state
        /// </summary>
        public SessionParseState parseState;

        /// <summary>
        /// Create a new instance of UnifiedStoreInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        /// <param name="arg1">General-purpose argument</param>
        public UnifiedStoreInput(RespCommand cmd, RespInputFlags flags = 0, long arg1 = 0)
        {
            this.header = new RespInputHeader(cmd, flags);
            this.arg1 = arg1;
        }

        /// <summary>
        /// Create a new instance of UnifiedStoreInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        /// <param name="arg1">General-purpose argument</param>
        public UnifiedStoreInput(ushort cmd, byte flags = 0, long arg1 = 0) :
            this((RespCommand)cmd, (RespInputFlags)flags, arg1)

        {
        }

        /// <summary>
        /// Create a new instance of UnifiedStoreInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="arg1">General-purpose argument</param>
        /// <param name="flags">Flags</param>
        public UnifiedStoreInput(RespCommand cmd, ref SessionParseState parseState, long arg1 = 0, RespInputFlags flags = 0) : this(cmd, flags, arg1)
        {
            this.parseState = parseState;
        }

        /// <summary>
        /// Create a new instance of UnifiedStoreInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="startIdx">First command argument index in parse state</param>
        /// <param name="arg1">General-purpose argument</param>
        /// <param name="flags">Flags</param>
        public UnifiedStoreInput(RespCommand cmd, ref SessionParseState parseState, int startIdx, long arg1 = 0, RespInputFlags flags = 0) : this(cmd, flags, arg1)
        {
            this.parseState = parseState.Slice(startIdx);
        }

        /// <inheritdoc />
        public int SerializedLength => header.SpanByte.TotalSize
                                       + sizeof(long) // arg1
                                       + parseState.GetSerializedLength();

        /// <inheritdoc />
        public unsafe int CopyTo(byte* dest, int length)
        {
            Debug.Assert(length >= this.SerializedLength);

            var curr = dest;

            // Serialize header
            header.SpanByte.SerializeTo(curr);
            curr += header.SpanByte.TotalSize;

            // Serialize arg1
            *(long*)curr = arg1;
            curr += sizeof(long);

            // Serialize parse state
            var remainingLength = length - (int)(curr - dest);
            var len = parseState.SerializeTo(curr, remainingLength);
            curr += len;

            // Serialize length
            return (int)(curr - dest);
        }

        /// <inheritdoc />
        public unsafe int DeserializeFrom(byte* src)
        {
            var curr = src;

            // Deserialize header
            var header = PinnedSpanByte.FromLengthPrefixedPinnedPointer(curr);
            ref var h = ref Unsafe.AsRef<RespInputHeader>(header.ToPointer());
            curr += header.TotalSize;
            this.header = h;

            // Deserialize arg1
            arg1 = *(long*)curr;
            curr += sizeof(long);

            // Deserialize parse state
            var len = parseState.DeserializeFrom(curr);
            curr += len;

            return (int)(curr - src);
        }
    }

    /// <summary>
    /// Header for Garnet CustomProcedure inputs
    /// </summary>
    public struct CustomProcedureInput : IStoreInput
    {
        /// <summary>
        /// Session parse state
        /// </summary>
        public SessionParseState parseState;

        /// <summary>
        /// RESP version of the session currently executing.
        /// 
        /// Will be 2 or 3.
        /// </summary>
        public byte RespVersion { get; }

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="parseState">Parse state</param>
        /// <param name="respVersion">RESP version for the session</param>
        public CustomProcedureInput(ref SessionParseState parseState, byte respVersion)
        {
            this.parseState = parseState;
            RespVersion = respVersion;
        }

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="parseState">Parse state</param>
        /// <param name="startIdx">First command argument index in parse state</param>
        /// /// <param name="respVersion">RESP version for the session</param>
        public CustomProcedureInput(ref SessionParseState parseState, int startIdx, byte respVersion)
        {
            this.parseState = parseState.Slice(startIdx);
            RespVersion = respVersion;
        }

        /// <inheritdoc />
        public int SerializedLength => parseState.GetSerializedLength();

        /// <inheritdoc />
        public unsafe int CopyTo(byte* dest, int length)
        {
            Debug.Assert(length >= this.SerializedLength);

            var curr = dest;

            // Serialize parse state
            var remainingLength = (int)(curr - dest);
            var len = parseState.SerializeTo(curr, remainingLength);
            curr += len;

            return (int)(curr - dest);
        }

        /// <inheritdoc />
        public unsafe int DeserializeFrom(byte* src)
        {
            // Deserialize parse state
            var len = parseState.DeserializeFrom(src);

            return len;
        }
    }
}