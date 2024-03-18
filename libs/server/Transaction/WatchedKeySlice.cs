// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    [StructLayout(LayoutKind.Explicit, Size = 29)]
    struct WatchedKeySlice
    {
        [FieldOffset(0)]
        public long version;

        [FieldOffset(8)]
        public ArgSlice slice;

        [FieldOffset(20)]
        public long hash;

        [FieldOffset(28)]
        public StoreType type;
    }
}