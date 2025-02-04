// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    struct PageUnit<TValuePage>
    {
        public TValuePage value;
        public long pointer;

        /// <inheritdoc/>
        public override string ToString() => $"Value {value}, Pointer {pointer}";
    }

    [StructLayout(LayoutKind.Explicit)]
    internal struct FullPageStatus
    {
        [FieldOffset(0)]
        public long LastFlushedUntilAddress;
        [FieldOffset(8)]
        public long Dirty;

        /// <inheritdoc/>
        public override string ToString() => $"LastFUA {LastFlushedUntilAddress}, Dirty {Dirty}";
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
        public override string ToString() => $"Page {Page}, Offset {Offset}";
    }
}