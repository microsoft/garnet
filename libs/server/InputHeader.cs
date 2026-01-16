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
        /// No flags set
        /// </summary>
        None = 0,

        /// <summary>
        /// Flag indicating a SET operation that returns the previous value (for strings).
        /// </summary>
        SetGet = 1,

        /// <summary>
        /// Deterministic
        /// </summary>
        Deterministic = 1 << 1,

        /// <summary>
        /// Expired
        /// </summary>
        Expired = 1 << 2,

        /// <summary>
        /// Flag indicating that the command does not require a RESP output
        /// </summary>
        SkipRespOutput = 1 << 3,
    }

    /// <summary>
    /// Enum used to indicate a meta command
    /// i.e. a command that envelops a main nested command
    /// </summary>
    public enum RespMetaCommand : byte
    {
        /// <summary>
        /// No meta command specified
        /// </summary>
        None = 0,

        // Beginning of etag-related meta-commands (if adding new etag meta-commands before this, update IsEtagCommand)

        /// <summary>
        /// Execute the main command and add the current etag to the output
        /// </summary>
        ExecWithEtag,

        // Beginning of etag conditional-execution meta-commands (if adding new etag conditional-execution meta-commands before this, update IsEtagCondExecCommand)

        /// <summary>
        /// Execute the main command if the current etag matches a specified etag
        /// </summary>
        ExecIfMatch,
        /// <summary>
        /// Execute the main command if the current etag does not match a specified etag
        /// </summary>
        ExecIfNotMatch,
        /// <summary>
        /// Execute the main command if a specified etag is greater than the current etag
        /// </summary>
        ExecIfGreater,

        // End of etag conditional-execution meta-commands (if adding new etag conditional-execution meta-commands after this, update IsEtagCondExecCommand)

        // End of etag-related meta-commands (if adding new etag meta-commands after this, update IsEtagCommand)
    }

    static class RespMetaCommandExtensions
    {
        /// <summary>
        /// Check if meta command is an etag-related meta-command
        /// </summary>
        /// <param name="metaCmd">Meta command</param>
        /// <returns>True if etag meta-command</returns>
        public static bool IsEtagCommand(this RespMetaCommand metaCmd)
            => metaCmd is >= RespMetaCommand.ExecWithEtag and <= RespMetaCommand.ExecIfGreater;

        /// <summary>
        /// Check if meta command is an etag-related conditional execution meta-command
        /// </summary>
        /// <param name="metaCmd">Meta command</param>
        /// <returns>True if etag meta-command</returns>
        public static bool IsEtagCondExecCommand(this RespMetaCommand metaCmd)
            => metaCmd is >= RespMetaCommand.ExecIfMatch and <= RespMetaCommand.ExecIfGreater;

        /// <summary>
        /// Check conditional execution of command based on meta-command
        /// </summary>
        /// <param name="metaCmd">Meta command</param>
        /// <param name="currEtag">Current etag record</param>
        /// <param name="compEtag">Etag comparand</param>
        /// <returns>True if command should execute</returns>
        public static bool CheckConditionalExecution(this RespMetaCommand metaCmd, long currEtag, long compEtag)
        {
            var comparisonResult = compEtag.CompareTo(currEtag);
            return metaCmd switch
            {
                RespMetaCommand.ExecIfMatch => comparisonResult == 0,
                RespMetaCommand.ExecIfNotMatch => comparisonResult != 0,
                RespMetaCommand.ExecIfGreater => comparisonResult == 1,
                _ => throw new ArgumentException($"Unexpected meta command: {metaCmd}", nameof(metaCmd)),
            };
        }
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
        public const int Size = 4;

        [FieldOffset(0)]
        internal RespCommand cmd;

        [FieldOffset(0)]
        internal GarnetObjectType type;

        [FieldOffset(2)]
        internal RespInputFlags flags;

        [FieldOffset(3)]
        internal byte SubId;

        [FieldOffset(3)]
        internal SortedSetOperation SortedSetOp;

        [FieldOffset(3)]
        internal HashOperation HashOp;

        [FieldOffset(3)]
        internal SetOperation SetOp;

        [FieldOffset(3)]
        internal ListOperation ListOp;

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

        /// <summary>
        /// Set expiration flag, used for log replay
        /// </summary>
        internal void SetExpiredFlag() => flags |= RespInputFlags.Expired;

        /// <summary>
        /// Set "SetGet" flag, used to get the old value of a key after conditionally setting it
        /// </summary>
        internal void SetSetGetFlag() => flags |= RespInputFlags.SetGet;

        /// <summary>
        /// Set "SkipRespOutput" flag, used to indicate that no RESP output is required
        /// </summary>
        internal void SetSkipRespOutputFlag() => flags |= RespInputFlags.SkipRespOutput;

        /// <summary>
        /// Check if record is expired, either deterministically during log replay,
        /// or based on current time in normal operation.
        /// </summary>
        /// <param name="expireTime">Expiration time</param>
        /// <returns></returns>
        internal readonly bool CheckExpiry(long expireTime)
            => (flags & RespInputFlags.Deterministic) != 0
                ? (flags & RespInputFlags.Expired) != 0
                : expireTime < DateTimeOffset.Now.UtcTicks;

        /// <summary>
        /// Check the SetGet flag
        /// </summary>
        internal bool CheckSetGetFlag()
            => (flags & RespInputFlags.SetGet) != 0;

        /// <summary>
        /// Check the SkipRespOutput flag
        /// </summary>
        internal bool CheckSkipRespOutputFlag()
            => (flags & RespInputFlags.SkipRespOutput) != 0;

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
    /// Info related to the meta-command enveloping the RESP command (if exists)
    /// </summary>
    public struct MetaCommandInfo(RespMetaCommand metaCommand, SessionParseState metaCommandParseState)
    {
        /// <summary>
        /// Meta Command
        /// </summary>
        public RespMetaCommand MetaCommand = metaCommand;

        /// <summary>
        /// Meta command parse state
        /// </summary>
        public SessionParseState MetaCommandParseState = metaCommandParseState;
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
        /// Info related to the parsed meta-command (if exists)
        /// </summary>
        public MetaCommandInfo metaCommandInfo;

        /// <summary>
        /// Setter for <see cref="RespInputHeader.SortedSetOp"/> in <see cref="header"/>
        /// </summary>
        public SortedSetOperation SortedSetOp
        {
            set { header.SortedSetOp = value; }
        }

        /// <summary>
        /// Setter for <see cref="RespInputHeader.HashOp"/> in <see cref="header"/>
        /// </summary>
        public HashOperation HashOp
        {
            set { header.HashOp = value; }
        }

        /// <summary>
        /// Setter for <see cref="RespInputHeader.SetOp"/> in <see cref="header"/>
        /// </summary>
        public SetOperation SetOp
        {
            set { header.SetOp = value; }
        }

        /// <summary>
        /// Setter for <see cref="RespInputHeader.ListOp"/> in <see cref="header"/>
        /// </summary>
        public ListOperation ListOp
        {
            set { header.ListOp = value; }
        }

        /// <summary>
        /// Setter for <see cref="RespInputHeader.SubId"/> in <see cref="header"/>
        /// </summary>
        public byte SubId
        {
            set { header.SubId = value; }
        }

        /// <summary>
        /// Create a new instance of ObjectInput
        /// </summary>
        /// <param name="type">Object type</param>
        /// <param name="metaCommandInfo">Meta command info</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="arg1">First general-purpose argument</param>
        /// <param name="arg2">Second general-purpose argument</param>
        /// <param name="flags">Flags</param>
        public ObjectInput(GarnetObjectType type, ref MetaCommandInfo metaCommandInfo, ref SessionParseState parseState, int arg1 = 0, int arg2 = 0, RespInputFlags flags = RespInputFlags.None)
            : this(type, ref metaCommandInfo, arg1, arg2, flags)
        {
            this.parseState = parseState;
        }

        /// <summary>
        /// Create a new instance of ObjectInput
        /// </summary>
        /// <param name="type">Object type</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="metaCommandInfo">Meta command info</param>
        /// <param name="startIdx">First command argument index in parse state</param>
        /// <param name="arg1">First general-purpose argument</param>
        /// <param name="arg2">Second general-purpose argument</param>
        /// <param name="flags">Flags</param>
        public ObjectInput(GarnetObjectType type, ref SessionParseState parseState, int startIdx, ref MetaCommandInfo metaCommandInfo, int arg1 = 0, int arg2 = 0, RespInputFlags flags = RespInputFlags.None)
            : this(type, ref metaCommandInfo, arg1, arg2, flags)
        {
            this.parseState = parseState.Slice(startIdx);
        }

        /// <summary>
        /// Create a new instance of ObjectInput
        /// </summary>
        /// <param name="type">Object type</param>
        /// <param name="metaCommandInfo">Meta command info</param>
        /// <param name="arg1">First general-purpose argument</param>
        /// <param name="arg2">Second general-purpose argument</param>
        /// <param name="flags">Flags</param>
        public ObjectInput(GarnetObjectType type, ref MetaCommandInfo metaCommandInfo, int arg1 = 0, int arg2 = 0, RespInputFlags flags = RespInputFlags.None)
            : this(type, arg1, arg2, flags)
        {
            this.metaCommandInfo = metaCommandInfo;
        }

        private ObjectInput(GarnetObjectType type, int arg1 = 0, int arg2 = 0, RespInputFlags flags = RespInputFlags.None)
        {
            this.header = new RespInputHeader(type, flags);
            this.arg1 = arg1;
            this.arg2 = arg2;
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
        /// Info related to the parsed meta-command (if exists)
        /// </summary>
        public MetaCommandInfo metaCommandInfo;

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        /// <param name="arg1">General-purpose argument</param>
        public StringInput(RespCommand cmd, RespInputFlags flags = 0, long arg1 = 0)
        : this(cmd, RespMetaCommand.None, arg1, flags)
        {
        }

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        /// <param name="arg1">General-purpose argument</param>
        public StringInput(ushort cmd, byte flags = 0, long arg1 = 0) :
            this((RespCommand)cmd, RespMetaCommand.None, arg1, (RespInputFlags)flags)
        {
        }

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="metaCommandInfo"></param>
        /// <param name="flags">Flags</param>
        /// <param name="arg1">General-purpose argument</param>
        public StringInput(RespCommand cmd, ref MetaCommandInfo metaCommandInfo, RespInputFlags flags = 0, long arg1 = 0)
            : this(cmd, RespMetaCommand.None, arg1, flags)
        {
            this.metaCommandInfo = metaCommandInfo;
        }

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="metaCommandInfo">Meta command info</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="arg1">General-purpose argument</param>
        /// <param name="flags">Flags</param>
        public StringInput(RespCommand cmd, ref MetaCommandInfo metaCommandInfo, ref SessionParseState parseState, long arg1 = 0, RespInputFlags flags = 0)
            : this(cmd, ref metaCommandInfo, arg1, flags)
        {
            this.parseState = parseState;
        }

        /// <summary>
        /// Create a new instance of StringInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="metaCmd">Meta command</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="startIdx">First command argument index in parse state</param>
        /// <param name="arg1">General-purpose argument</param>
        /// <param name="flags">Flags</param>
        public StringInput(RespCommand cmd, RespMetaCommand metaCmd, ref SessionParseState parseState, int startIdx, long arg1 = 0, RespInputFlags flags = 0)
            : this(cmd, metaCmd, arg1, flags)
        {
            this.parseState = parseState.Slice(startIdx);
        }

        private StringInput(RespCommand cmd, RespMetaCommand metaCmd, long arg1 = 0, RespInputFlags flags = 0)
        {
            this.header = new RespInputHeader(cmd, flags);
            this.arg1 = arg1;
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
            : this(cmd, RespMetaCommand.None, flags, arg1)
        {
        }

        /// <summary>
        /// Create a new instance of UnifiedInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="flags">Flags</param>
        /// <param name="arg1">General-purpose argument</param>
        public UnifiedInput(ushort cmd, byte flags = 0, long arg1 = 0) :
            this((RespCommand)cmd, RespMetaCommand.None, (RespInputFlags)flags, arg1)
        {
        }

        /// <summary>
        /// Create a new instance of UnifiedInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="metaCmd">Meta command</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="arg1">General-purpose argument</param>
        /// <param name="flags">Flags</param>
        public UnifiedInput(RespCommand cmd, RespMetaCommand metaCmd, ref SessionParseState parseState, long arg1 = 0, RespInputFlags flags = 0)
            : this(cmd, metaCmd, flags, arg1)
        {
            this.parseState = parseState;
        }

        /// <summary>
        /// Create a new instance of UnifiedInput
        /// </summary>
        /// <param name="cmd">Command</param>
        /// <param name="metaCmd">Meta command</param>
        /// <param name="parseState">Parse state</param>
        /// <param name="startIdx">First command argument index in parse state</param>
        /// <param name="arg1">General-purpose argument</param>
        /// <param name="flags">Flags</param>
        public UnifiedInput(RespCommand cmd, RespMetaCommand metaCmd, ref SessionParseState parseState, int startIdx, long arg1 = 0, RespInputFlags flags = 0)
            : this(cmd, metaCmd, flags, arg1)
        {
            this.parseState = parseState.Slice(startIdx);
        }

        private UnifiedInput(RespCommand cmd, RespMetaCommand metaCmd, RespInputFlags flags = 0, long arg1 = 0)
        {
            this.header = new RespInputHeader(cmd, flags);
            this.arg1 = arg1;
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