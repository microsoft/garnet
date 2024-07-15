// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    /// <summary>
    /// Carries various addresses and accompanying values corresponding to source records for the current InternalXxx or InternalContinuePendingR*
    /// operations, where "source" is a copy source for RMW and/or a locked record. This is passed to functions that create records, such as 
    /// TsavoriteKV.CreateNewRecord*() or TsavoriteKV.InternalTryCopyToTail(), and to unlocking utilities.
    /// </summary>
    internal struct RecordSource<Key, Value, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<Key, Value>
        where TAllocator : IAllocator<Key, Value, TStoreFunctions>
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
        internal AllocatorBase<Key, Value, TStoreFunctions, TAllocator> AllocatorBase { get; private set; }

        struct InternalStates
        {
            internal const int None = 0;
            internal const int TransientSLock = 0x0001;    // LockTable
            internal const int TransientXLock = 0x0002;    // LockTable
            internal const int LockBits = TransientSLock | TransientXLock;

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
                            sb.Append(", ");
                        sb.Append(name);
                    }
                }

                append(TransientSLock, nameof(TransientSLock));
                append(TransientXLock, nameof(TransientXLock));
                append(MainLogSrc, nameof(MainLogSrc));
                append(ReadCacheSrc, nameof(ReadCacheSrc));
                return sb.ToString();
            }
        }

        int internalState;

        /// <summary>
        /// Set (and cleared) by caller to indicate whether we have a LockTable-based Transient Shared lock (does not include Manual locks; this is per-operation only).
        /// </summary>
        internal readonly bool HasTransientSLock => (internalState & InternalStates.TransientSLock) != 0;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetHasTransientSLock() => internalState |= InternalStates.TransientSLock;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearHasTransientSLock() => internalState &= ~InternalStates.TransientSLock;

        /// <summary>
        /// Set (and cleared) by caller to indicate whether we have a LockTable-based Transient Exclusive lock (does not include Manual locks; this is per-operation only).
        /// </summary>
        internal readonly bool HasTransientXLock => (internalState & InternalStates.TransientXLock) != 0;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetHasTransientXLock() => internalState |= InternalStates.TransientXLock;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearHasTransientXLock() => internalState &= ~InternalStates.TransientXLock;

        /// <summary>
        /// Indicates whether we have any type of non-Manual lock.
        /// </summary>
        internal readonly bool HasLock => (internalState & InternalStates.LockBits) != 0;

        /// <summary>
        /// Set by caller to indicate whether the <see cref="LogicalAddress"/> is an in-memory record in the main log, being used as a copy source and/or a lock.
        /// </summary>
        internal readonly bool HasMainLogSrc => (internalState & InternalStates.MainLogSrc) != 0;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetHasMainLogSrc() => internalState |= InternalStates.MainLogSrc;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearHasMainLogSrc() => internalState &= ~InternalStates.MainLogSrc;

        /// <summary>
        /// Set by caller to indicate whether the <see cref="LogicalAddress"/> is an in-memory record in the readcache, being used as a copy source and/or a lock.
        /// </summary>
        internal readonly bool HasReadCacheSrc => (internalState & InternalStates.ReadCacheSrc) != 0;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetHasReadCacheSrc() => internalState |= InternalStates.ReadCacheSrc;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearHasReadCacheSrc() => internalState &= ~InternalStates.ReadCacheSrc;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long SetPhysicalAddress() => PhysicalAddress = Allocator.GetPhysicalAddress(LogicalAddress);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly ref RecordInfo GetInfo() => ref Allocator.GetInfo(PhysicalAddress);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly ref Key GetKey() => ref Allocator.GetKey(PhysicalAddress);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly ref Value GetValue() => ref Allocator.GetValue(PhysicalAddress);

        internal readonly bool HasInMemorySrc => (internalState & (InternalStates.MainLogSrc | InternalStates.ReadCacheSrc)) != 0;

        /// <summary>
        /// Initialize to the latest logical address from the caller.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(long latestLogicalAddress, AllocatorBase<Key, Value, TStoreFunctions, TAllocator> srcAllocatorBase)
        {
            PhysicalAddress = default;
            LowestReadCacheLogicalAddress = default;
            LowestReadCachePhysicalAddress = default;
            ClearHasMainLogSrc();
            ClearHasReadCacheSrc();

            // HasTransientLock = ...;   Do not clear this; it is in the LockTable and must be preserved until unlocked

            LatestLogicalAddress = LogicalAddress = AbsoluteAddress(latestLogicalAddress);
            SetAllocator(srcAllocatorBase);
        }

        internal void SetAllocator(AllocatorBase<Key, Value, TStoreFunctions, TAllocator> srcAllocatorBase)
        {
            this.AllocatorBase = srcAllocatorBase;
            this.Allocator = AllocatorBase._wrapper;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly string LockStateString() => InternalStates.ToString(internalState & InternalStates.LockBits);

        public override readonly string ToString()
        {
            var isRC = "(rc)";
            var llaRC = IsReadCache(LatestLogicalAddress) ? isRC : string.Empty;
            var laRC = IsReadCache(LogicalAddress) ? isRC : string.Empty;
            return $"lla {AbsoluteAddress(LatestLogicalAddress)}{llaRC}, la {AbsoluteAddress(LogicalAddress)}{laRC}, lrcla {AbsoluteAddress(LowestReadCacheLogicalAddress)},"
                 + $" hasInMemorySrc {InternalStates.ToString(internalState & InternalStates.InMemSrcBits)}, hasLocks {LockStateString()}";
        }
    }
}