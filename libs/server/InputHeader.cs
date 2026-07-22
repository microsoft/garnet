// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Flags used by append-only file (AOF/WAL).
    /// These occupy the top bits (32/64/128) of the header flags byte (RespInputHeader byte 2).
    /// The object sub-operation id now lives in its own header byte (RespInputHeader.subId), so the
    /// flags byte no longer shares space with it.
    /// </summary>
    [Flags]
    public enum RespInputFlags : byte
    {
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
    /// Common input header for Garnet.
    ///
    /// Layout (3 bytes, explicit) is a discriminated union keyed by command category:
    ///   byte 0-1 : <see cref="cmd"/> (ushort) — used by string/unified inputs.
    ///   byte 0   : <see cref="type"/> (GarnetObjectType) — used by object inputs; overlaps cmd's low byte.
    ///   byte 1   : <see cref="subId"/> (object sub-operation id) — used by object inputs; overlaps cmd's high byte.
    ///   byte 2   : <see cref="flags"/> (<see cref="RespInputFlags"/>).
    ///
    /// Object inputs use { type, subId } and never read/write <see cref="cmd"/>; string/unified inputs use
    /// <see cref="cmd"/> and never set <see cref="subId"/>. The two views are disjoint by command category
    /// (store / AofEntryType), so sharing byte 1 between cmd's high byte and subId is safe. This gives the
    /// object sub-operation a full byte (256 values/type) instead of the former 5 bits packed into flags.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct RespInputHeader
    {
        /// <summary>
        /// Size of header
        /// </summary>
        public const int Size = 3;

        [FieldOffset(0)]
        internal RespCommand cmd;

        [FieldOffset(0)]
        internal GarnetObjectType type;

        [FieldOffset(1)]
        internal byte subId;

        [FieldOffset(2)]
        internal RespInputFlags flags;

        /// <summary>
        /// Create a new instance of RespInputHeader
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
            get => subId;
            set => subId = value;
        }

        internal SortedSetOperation SortedSetOp
        {
            get => (SortedSetOperation)subId;
            set => subId = (byte)value;
        }

        internal HashOperation HashOp
        {
            get => (HashOperation)subId;
            set => subId = (byte)value;
        }

        internal SetOperation SetOp
        {
            get => (SetOperation)subId;
            set => subId = (byte)value;
        }

        internal ListOperation ListOp
        {
            get => (ListOperation)subId;
            set => subId = (byte)value;
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

            return (int)(curr - src);
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringInput(ushort cmd, byte flags = 0, long arg1 = 0)
            : this((RespCommand)cmd, (RespInputFlags)flags, arg1)
        {
        }

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="arg1">General-purpose argument</param>
        /// <param name="flags">Flags</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringInput(RespCommand cmd, ref SessionParseState parseState, long arg1 = 0, RespInputFlags flags = 0)
            : this(cmd, flags, arg1)
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StringInput(RespCommand cmd, ref SessionParseState parseState, int startIdx, long arg1 = 0, RespInputFlags flags = 0)
            : this(cmd, flags, arg1)
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

    /// <summary>
    /// Header for Garnet Main Store inputs but for Vector element r/w/d ops
    /// </summary>
    public struct VectorInput : IStoreInput
    {
        public int SerializedLength => throw new NotImplementedException();

        public int ReadDesiredSize { get; set; }

        public int WriteDesiredSize { get; set; }

        public int Index { get; set; }
        public nint CallbackContext { get; set; }
        public nint Callback { get; set; }

        public bool AlignmentExpected { get; set; }

        [MemberNotNullWhen(returnValue: true, member: nameof(MaxMigrationHeapAllocationSize))]
        public bool IsMigrationRead => MaxMigrationHeapAllocationSize != null;

        public int? MaxMigrationHeapAllocationSize { get; set; }

        public VectorInput()
        {
        }

        public unsafe int CopyTo(byte* dest, int length) => throw new NotImplementedException();
        public unsafe int DeserializeFrom(byte* src) => throw new NotImplementedException();
    }
}