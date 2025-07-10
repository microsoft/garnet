// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.server
{
    /// <summary>
    ///  Represents a GarnetStreamID, which is a 128-bit identifier for an entry in a stream.
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct StreamID
    {
        [FieldOffset(0)]
        public ulong ms;
        [FieldOffset(8)]
        public ulong seq;
        [FieldOffset(0)]
        public fixed byte idBytes[16];

        public StreamID(ulong ms, ulong seq)
        {
            BinaryPrimitives.WriteUInt64BigEndian(new Span<byte>(Unsafe.AsPointer(ref this.ms), 8), ms);
            BinaryPrimitives.WriteUInt64BigEndian(new Span<byte>(Unsafe.AsPointer(ref this.seq), 8), seq);
        }
        public void setMS(ulong ms)
        {
            BinaryPrimitives.WriteUInt64BigEndian(new Span<byte>(Unsafe.AsPointer(ref this.ms), 8), ms);
        }

        public void setSeq(ulong seq)
        {
            BinaryPrimitives.WriteUInt64BigEndian(new Span<byte>(Unsafe.AsPointer(ref this.seq), 8), seq);
        }

        public ulong getMS()
        {
            return BinaryPrimitives.ReadUInt64BigEndian(new Span<byte>(Unsafe.AsPointer(ref this.ms), 8));
        }

        public ulong getSeq()
        {
            return BinaryPrimitives.ReadUInt64BigEndian(new Span<byte>(Unsafe.AsPointer(ref this.seq), 8));
        }

        public unsafe StreamID(byte[] inputBytes)
        {
            if (inputBytes.Length != 16)
            {
                throw new ArgumentException("idBytes must be 16 bytes");
            }

            fixed (byte* idBytesPtr = idBytes)
            {
                var sourceSpan = new ReadOnlySpan<byte>(inputBytes);
                var destinationSpan = new Span<byte>(idBytesPtr, 16);
                sourceSpan.CopyTo(destinationSpan);
            }
        }
    }
}