// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public struct PageHeader
    {
        const ushort CurrentVersion = 1;

        /// <summary>The number of bits in the size of the struct. Currently set to make <see cref="Size"/> the size that the 0'th page offset was in earlier versions; 64 bytes</summary>
        internal const int SizeBits = 6;

        /// <summary>The size of the struct. Must be a power of 2. Currently set to the size that the 0'th page offset was; 64 bytes</summary>
        public const int Size = 1 << SizeBits;

        /// <summary>Version of this page header.</summary>
        [FieldOffset(0)]
        internal ushort version;

        [FieldOffset(sizeof(ushort))]
        internal ushort unusedUshort1;

        [FieldOffset(sizeof(int))]
        internal int unusedInt1;

        /// <summary>The lowest object-log position on this main-log page, if ObjectAllocator. Contains both segmentId and offset on segment</summary>
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
            version = CurrentVersion;
            objectLogLowestPositionWord = ObjectLogFilePositionInfo.NotSet;
        }

        internal static unsafe void Initialize(long physicalAddressOfStartOfPage) => (*(PageHeader*)physicalAddressOfStartOfPage).Initialize();

        /// <summary>
        /// Set the lowest object-log position on this main-log page, if ObjectAllocator.
        /// </summary>
        /// <param name="position">The position in the object log.</param>
        internal void SetLowestObjectLogPosition(in ObjectLogFilePositionInfo position)
        {
            if (objectLogLowestPositionWord == ObjectLogFilePositionInfo.NotSet)
                objectLogLowestPositionWord = position.word;
        }

        /// <summary>
        /// Set the lowest object-log position on this main-log page, if ObjectAllocator.
        /// </summary>
        /// <param name="segmentBits">The number of bits in the object log's segments.</param>
        internal ObjectLogFilePositionInfo GetLowestObjectLogPosition(int segmentBits) 
            => objectLogLowestPositionWord == ObjectLogFilePositionInfo.NotSet ? new() : new(objectLogLowestPositionWord, segmentBits);

        public override readonly string ToString()
            => $"ver {version}, lowObjLogPos {objectLogLowestPositionWord}, us1 {unusedUshort1}, ui1 {unusedInt1}, ul1 {unusedLong1}, ul2 {unusedLong2}, ul3 {unusedLong3}, ul4 {unusedLong4}, ul5 {unusedLong5}, ul6 {unusedLong6}";
    }
}