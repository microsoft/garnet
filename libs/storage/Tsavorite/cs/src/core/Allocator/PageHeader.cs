// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct PageHeader
    {
        /// <summary>Current version of <see cref="PageHeader"/></summary>
        const ushort CurrentHeaderVersion = 0;
        /// <summary>Current version of <see cref="RecordInfo"/>, <see cref="RecordDataHeader"/>, and layout of objects on the page</summary>
        const ushort CurrentRecordVersion = 0;

        /// <summary>The number of bits in the size of the struct. Currently set to make <see cref="Size"/> the size that the 0'th page offset was in earlier versions; 64 bytes</summary>
        internal const int SizeBits = 6;

        /// <summary>The size of the struct. Must be a power of 2. Currently set to the size that the 0'th page offset was; 64 bytes</summary>
        public const int Size = 1 << SizeBits;

        /// <summary>Version of this page header.</summary>
        [FieldOffset(0)]
        internal ushort headerVersion;

        [FieldOffset(sizeof(ushort))]
        internal ushort recordVersion;

        [FieldOffset(sizeof(int))]
        internal int unusedInt1;

        /// <summary>The object-log floor of this main-log page, if ObjectAllocator: the object log's tail before the page's first flush wrote any
        /// of its records. Object-log bytes this page or any later page references are at or above it, because the main object log is written
        /// in page order, so truncation never goes past it. Unstamped (see <see cref="ObjectLogPositionInfo.WordIsUnstamped"/>) until a flush
        /// stamps it.</summary>
        [FieldOffset(sizeof(long))]
        internal ulong objectLogLowestPositionWord;

        // Unused; as they become used, start with higher #
        [FieldOffset(sizeof(long) * 2)]
        internal long unusedLong6;
        [FieldOffset(sizeof(long) * 3)]
        internal long unusedLong5;
        [FieldOffset(sizeof(long) * 4)]
        internal long unusedLong4;
        [FieldOffset(sizeof(long) * 5)]
        internal long unusedLong3;
        [FieldOffset(sizeof(long) * 6)]
        internal long unusedLong2;
        [FieldOffset(sizeof(long) * 7)]
        internal long unusedLong1;

        /// <summary>
        /// Initializes the struct.
        /// </summary>
        /// <returns></returns>
        internal void Initialize()
        {
            this = default;
            headerVersion = CurrentHeaderVersion;
            recordVersion = CurrentRecordVersion;
            objectLogLowestPositionWord = ObjectLogPositionInfo.NotSet;
        }

        internal static unsafe void Initialize(long physicalAddressOfStartOfPage) => (*(PageHeader*)physicalAddressOfStartOfPage).Initialize();

        /// <summary>
        /// Set the object-log floor of this main-log page, if ObjectAllocator.
        /// </summary>
        /// <param name="address">The object-log address.</param>
        /// <param name="force">Overwrite an already-stamped floor. Used when a flush writes the page's objects into a different object-log address
        /// space than the floor refers to.</param>
        internal void SetLowestObjectLogAddress(long address, bool force = false)
        {
            if (force || ObjectLogPositionInfo.WordIsUnstamped(objectLogLowestPositionWord))
                objectLogLowestPositionWord = (ulong)address & ObjectLogPositionInfo.AddressMask;
        }

        /// <summary>
        /// Get the object-log floor of this main-log page, if ObjectAllocator; -1 if it is unstamped.
        /// </summary>
        internal readonly long GetLowestObjectLogAddress()
            => ObjectLogPositionInfo.WordIsUnstamped(objectLogLowestPositionWord) ? -1 : (long)(objectLogLowestPositionWord & ObjectLogPositionInfo.AddressMask);

        public override readonly string ToString()
            => $"HeaderVer {headerVersion}, RecordVer {recordVersion}, lowObjLogPos {objectLogLowestPositionWord}, ui1 {unusedInt1}, ul1 {unusedLong1}, ul2 {unusedLong2}, ul3 {unusedLong3}, ul4 {unusedLong4}, ul5 {unusedLong5}, ul6 {unusedLong6}";
    }
}