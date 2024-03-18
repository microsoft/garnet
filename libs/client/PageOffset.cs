// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.client
{
    [StructLayout(LayoutKind.Explicit)]
    struct PageOffset
    {
        internal const int kPageBits = 12;
        const int kPageOffset = 32;
        internal const long kPageMask = (1L << kPageBits) - 1;
        const long kPageMaskInLong = kPageMask << kPageOffset;

        const int kOffsetBits = 32;
        const int kOffsetOffset = 0;
        internal const long kOffsetMask = (1L << kOffsetBits) - 1;
        const long kOffsetMaskInLong = kOffsetMask << kOffsetOffset;

        const int kTaskBits = 20;
        internal const int kTaskOffset = 44;
        internal const long kTaskMask = (1L << kTaskBits) - 1;
        const long kTaskMaskInLong = kTaskMask << kTaskOffset;

        [FieldOffset(0)]
        public long PageAndOffset;

        public int Page
        {
            get => (int)((PageAndOffset >> kPageOffset) & kPageMask);
            set => PageAndOffset = (((long)value) << kPageOffset) | (PageAndOffset & ~kPageMaskInLong);
        }

        public int Offset
        {
            get => (int)((PageAndOffset >> kOffsetOffset) & kOffsetMask);
            set => PageAndOffset = (((long)value) << kOffsetOffset) | (PageAndOffset & ~kOffsetMaskInLong);
        }

        public int TaskId
        {
            get => (int)((PageAndOffset >> kTaskOffset) & kTaskMask);
            set => PageAndOffset = (((long)value) << kTaskOffset) | (PageAndOffset & ~kTaskMaskInLong);
        }

        public int PrevTaskId => (int)(((PageAndOffset >> kTaskOffset) - 1) & kTaskMask);
    }
}