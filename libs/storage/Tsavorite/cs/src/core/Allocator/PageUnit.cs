// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    struct PageUnit<TValuePage>
    {
        /// <summary>The byte array of this circular buffer page</summary>
        public byte[] array;

        /// <summary>The pinned pointer to this circular buffer page</summary>
        public long pointer;

        /// <summary>The specific allocator's associated value for this circular buffer page</summary>
        public TValuePage value;

        /// <inheritdoc/>
        public override readonly string ToString() => $"Value {value}, Pointer {pointer}, Array.Length {array.Length}";
    }

    [StructLayout(LayoutKind.Explicit)]
    internal struct FullPageStatus
    {
        [FieldOffset(0)]
        public long LastFlushedUntilAddress;
        /// <inheritdoc/>
        public override readonly string ToString() => $"LastFUA {LastFlushedUntilAddress}";
    }

    [StructLayout(LayoutKind.Explicit)]
    internal struct PageOffset
    {
        [FieldOffset(0)]
        public int Offset;
        [FieldOffset(4)]
        public int Page;
        [FieldOffset(0)]
        public long PageAndOffset;

        /// <inheritdoc/>
        public override readonly string ToString() => $"Page {Page}, Offset {Offset}";
    }
}