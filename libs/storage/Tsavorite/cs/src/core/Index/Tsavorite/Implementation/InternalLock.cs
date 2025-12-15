// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InternalTryLockShared(long keyHash)
        {
            HashEntryInfo hei = new(keyHash);
            FindTag(ref hei);
            return InternalTryLockShared(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InternalTryLockShared(ref HashEntryInfo hei)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalLockShared must have protected epoch");
            return LockTable.TryLockShared(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InternalTryLockExclusive(long keyHash)
        {
            HashEntryInfo hei = new(keyHash);
            FindTag(ref hei);
            return InternalTryLockExclusive(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InternalTryLockExclusive(ref HashEntryInfo hei)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalLockExclusive must have protected epoch");
            return LockTable.TryLockExclusive(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalUnlockShared(long keyHash)
        {
            HashEntryInfo hei = new(keyHash);
            FindTag(ref hei);
            InternalUnlockShared(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalUnlockShared(ref HashEntryInfo hei)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalUnlockShared must have protected epoch");
            LockTable.UnlockShared(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalUnlockExclusive(long keyHash)
        {
            HashEntryInfo hei = new(keyHash);
            FindTag(ref hei);
            InternalUnlockExclusive(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalUnlockExclusive(ref HashEntryInfo hei)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalUnlockExclusive must have protected epoch");
            LockTable.UnlockExclusive(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InternalPromoteLock(long keyHash)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalLock must have protected epoch");
            HashEntryInfo hei = new(keyHash);
            FindTag(ref hei);
            return LockTable.TryPromoteLock(ref hei);
        }
    }
}