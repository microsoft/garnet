// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    using static LogAddress;

    /// <summary>
    /// Carries various addresses and accompanying values corresponding to source records for the current InternalXxx or InternalContinuePendingR*
    /// operations, where "source" is a copy source for RMW and/or a locked record. This is passed to functions that create records, such as 
    /// TsavoriteKV.CreateNewRecord*() or TsavoriteKV.InternalTryCopyToTail(), and to unlocking utilities.
    /// </summary>
    internal struct RecordSource<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// If valid, this is the logical address of a record. As "source", it may be copied from for RMW or pending Reads,
        /// or is locked. This address lives in one of the following places:
        /// <list type="bullet">
        ///     <item>In the in-memory portion of the main log (<see cref="HasMainLogSrc"/>). In this case, it may be a source for RMW CopyUpdater, or simply used for locking.</item>
        ///     <item>In the readcache (<see cref="HasReadCacheSrc"/>). In this case, it may be a source for RMW CopyUpdater, or simply used for locking.</item>
        ///     <item>In the on-disk portion of the main log. In this case, the current call comes from a completed I/O request</item>
        /// </list>
        /// </summary>
        internal long LogicalAddress;

        /// <summary>
        /// If <see cref="HasInMemorySrc"/> this is the physical address of <see cref="LogicalAddress"/>.
        /// </summary>
        internal long PhysicalAddress;

        /// <summary>
        /// The highest logical address in the main log (i.e. below readcache) for this key; if we have a readcache prefix chain, this is the splice point.
        /// </summary>
        internal long LatestLogicalAddress;

        /// <summary>
        /// If valid, the lowest readcache logical address for this key; used to splice records between readcache and main log.
        /// </summary>
        internal long LowestReadCacheLogicalAddress;

        /// <summary>
        /// The physical address of <see cref="LowestReadCacheLogicalAddress"/>.
        /// </summary>
        internal long LowestReadCachePhysicalAddress;

        /// <summary>
        /// If <see cref="HasInMemorySrc"/>, this is the allocator (hlog or readcache) that <see cref="LogicalAddress"/> is in.
        /// </summary>
        internal TAllocator Allocator { get; private set; }

        /// <summary>
        /// If <see cref="HasInMemorySrc"/>, this is the allocator base (hlog or readcache) that <see cref="LogicalAddress"/> is in.
        /// </summary>
        internal AllocatorBase<TStoreFunctions, TAllocator> AllocatorBase { get; private set; }

        struct InternalStates
        {
            internal const int None = 0;
            internal const int EphemeralSLock = 0x0001;    // LockTable
            internal const int EphemeralXLock = 0x0002;    // LockTable
            internal const int LockBits = EphemeralSLock | EphemeralXLock;

            // These are separate from the AddressType in LogicalAddress because we need to know if that LogicalAddress matched the key.
            internal const int MainLogSrc = 0x0100;
            internal const int ReadCacheSrc = 0x0200;
            internal const int InMemSrcBits = MainLogSrc | ReadCacheSrc;

            internal static string ToString(int state)
            {
                System.Text.StringBuilder sb = new();

                if (state == None)
                    return nameof(None);

                void append(int value, string name)
                {
                    if ((state & value) != 0)
                    {
                        if (sb.Length > 0)
                            _ = sb.Append(", ");
                        _ = sb.Append(name);
                    }
                }

                append(EphemeralSLock, nameof(EphemeralSLock));
                append(EphemeralXLock, nameof(EphemeralXLock));
                append(MainLogSrc, nameof(MainLogSrc));
                append(ReadCacheSrc, nameof(ReadCacheSrc));
                return sb.ToString();
            }
        }

        int internalState;

        /// <summary>
        /// Set (and cleared) by caller to indicate whether we have a LockTable-based Ephemeral Shared lock (does not include Manual locks; this is per-operation only).
        /// </summary>
        internal readonly bool HasEphemeralSLock => (internalState & InternalStates.EphemeralSLock) != 0;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetHasEphemeralSLock() => internalState |= InternalStates.EphemeralSLock;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearHasEphemeralSLock() => internalState &= ~InternalStates.EphemeralSLock;

        /// <summary>
        /// Set (and cleared) by caller to indicate whether we have a LockTable-based Ephemeral Exclusive lock (does not include Manual locks; this is per-operation only).
        /// </summary>
        internal readonly bool HasEphemeralXLock => (internalState & InternalStates.EphemeralXLock) != 0;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetHasEphemeralXLock() => internalState |= InternalStates.EphemeralXLock;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearHasEphemeralXLock() => internalState &= ~InternalStates.EphemeralXLock;

        /// <summary>
        /// Indicates whether we have any type of non-Manual lock.
        /// </summary>
        internal readonly bool HasLock => (internalState & InternalStates.LockBits) != 0;

        /// <summary>
        /// Set by caller to indicate whether the <see cref="LogicalAddress"/> is an in-memory record in the main log, being used as a copy source and/or a lock.
        /// </summary>
        internal readonly bool HasMainLogSrc => (internalState & InternalStates.MainLogSrc) != 0;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetHasMainLogSrc()
        {
            Debug.Assert(!IsReadCache(LogicalAddress), "LogicalAddress must be a main log address to set HasMainLogSrc");
            internalState |= InternalStates.MainLogSrc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearHasMainLogSrc() => internalState &= ~InternalStates.MainLogSrc;

        /// <summary>
        /// Set by caller to indicate whether the <see cref="LogicalAddress"/> is an in-memory record in the readcache, being used as a copy source and/or a lock.
        /// </summary>
        internal readonly bool HasReadCacheSrc => (internalState & InternalStates.ReadCacheSrc) != 0;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetHasReadCacheSrc()
        {
            Debug.Assert(IsReadCache(LogicalAddress), "LogicalAddress must be a readcache address to set HasReadCacheSrc");
            internalState |= InternalStates.ReadCacheSrc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearHasReadCacheSrc() => internalState &= ~InternalStates.ReadCacheSrc;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long SetPhysicalAddress() => PhysicalAddress = AllocatorBase.GetPhysicalAddress(LogicalAddress);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly ref RecordInfo GetInfoRef() => ref LogRecord.GetInfoRef(PhysicalAddress);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal RecordInfo GetInfo() => LogRecord.GetInfo(PhysicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord CreateLogRecord()
        {
            // If we have a physical address we must be in the in-memory log.
            Debug.Assert(PhysicalAddress != 0, "Cannot CreateLogRecord until PhysicalAddress is set");
            return Allocator.CreateLogRecord(LogicalAddress, PhysicalAddress);
        }

        internal readonly bool HasInMemorySrc => (internalState & (InternalStates.MainLogSrc | InternalStates.ReadCacheSrc)) != 0;

        /// <summary>
        /// Initialize to the latest logical address from the caller.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(long latestLogicalAddress, AllocatorBase<TStoreFunctions, TAllocator> srcAllocatorBase)
        {
            PhysicalAddress = default;
            LowestReadCacheLogicalAddress = default;
            LowestReadCachePhysicalAddress = default;
            ClearHasMainLogSrc();
            ClearHasReadCacheSrc();

            // HasEphemeralLock = ...;   Do not clear this; it is in the LockTable and must be preserved until unlocked

            LatestLogicalAddress = LogicalAddress = latestLogicalAddress;
            SetAllocator(srcAllocatorBase);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetAllocator(AllocatorBase<TStoreFunctions, TAllocator> srcAllocatorBase)
        {
            AllocatorBase = srcAllocatorBase;
            Allocator = AllocatorBase._wrapper;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly string LockStateString() => InternalStates.ToString(internalState & InternalStates.LockBits);

        public override readonly string ToString()
            => $"lla {AddressString(LatestLogicalAddress)}, la {AddressString(LogicalAddress)}, lrcla {AddressString(LowestReadCacheLogicalAddress)},"
             + $" inMemSrc {InternalStates.ToString(internalState & InternalStates.InMemSrcBits)}, locks {LockStateString()}";
    }
}