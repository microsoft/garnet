// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    internal enum ResizeOperationStatus : int { IN_PROGRESS, DONE };

    [StructLayout(LayoutKind.Explicit, Size = Size)]
    internal struct ResizeInfo
    {
        internal const int Size = 8;

        [FieldOffset(0)]
        public ResizeOperationStatus status;

        [FieldOffset(4)]
        public int version; // either 0 or 1

        [FieldOffset(0)]
        public long word;
    }
}
