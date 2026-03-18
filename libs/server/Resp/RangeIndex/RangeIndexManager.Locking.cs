// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Locking methods for RangeIndex operations.
    ///
    /// Shared locks are acquired for data operations (RI.SET, RI.GET, RI.DEL field)
    /// to prevent concurrent deletion of the underlying BfTree.
    /// Exclusive locks are acquired for index lifecycle operations (DEL key, eviction).
    /// </summary>
    public sealed partial class RangeIndexManager
    {
        /// <summary>
        /// RAII holder for a shared lock on a RangeIndex key.
        /// Disposing releases the shared lock.
        /// </summary>
        internal readonly ref struct ReadRangeIndexLock : IDisposable
        {
            private readonly ref readonly ReadOptimizedLock lockRef;
            private readonly int lockToken;

            internal ReadRangeIndexLock(ref readonly ReadOptimizedLock lockRef, int lockToken)
            {
                this.lockToken = lockToken;
                this.lockRef = ref lockRef;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                if (Unsafe.IsNullRef(in lockRef))
                    return;

                lockRef.ReleaseSharedLock(lockToken);
            }
        }

        /// <summary>
        /// RAII holder for an exclusive lock on a RangeIndex key.
        /// Disposing releases the exclusive lock.
        /// </summary>
        internal readonly ref struct ExclusiveRangeIndexLock : IDisposable
        {
            private readonly ref readonly ReadOptimizedLock lockRef;
            private readonly int lockToken;

            internal ExclusiveRangeIndexLock(ref readonly ReadOptimizedLock lockRef, int lockToken)
            {
                this.lockToken = lockToken;
                this.lockRef = ref lockRef;
            }

            /// <inheritdoc/>
            public void Dispose()
            {
                if (Unsafe.IsNullRef(in lockRef))
                    return;

                lockRef.ReleaseExclusiveLock(lockToken);
            }
        }

        private readonly ReadOptimizedLock rangeIndexLocks;

        /// <summary>
        /// Read the RangeIndex stub under a shared lock.
        /// Used by RI.SET, RI.GET, RI.DEL (field-level operations).
        /// Returns an RAII lock holder; caller operates on the BfTree while the lock is held.
        /// </summary>
        internal ReadRangeIndexLock ReadRangeIndex(
            StorageSession session,
            PinnedSpanByte key,
            ref StringInput input,
            scoped Span<byte> indexSpan,
            out GarnetStatus status)
        {
            Debug.Assert(indexSpan.Length >= IndexSizeBytes, "Insufficient space for index");

            var keyHash = session.stringBasicContext.GetKeyHash((FixedSpanByteKey)key);
            var output = StringOutput.FromPinnedSpan(indexSpan);

            rangeIndexLocks.AcquireSharedLock(keyHash, out var sharedLockToken);

            GarnetStatus readRes;
            try
            {
                readRes = session.Read_MainStore(key.ReadOnlySpan, ref input, ref output, ref session.stringBasicContext);
            }
            catch
            {
                rangeIndexLocks.ReleaseSharedLock(sharedLockToken);
                throw;
            }

            if (readRes != GarnetStatus.OK)
            {
                status = readRes;
                rangeIndexLocks.ReleaseSharedLock(sharedLockToken);
                return default;
            }

            // Validate that the output is a valid range index stub.
            // The Reader copies the value into output; check the actual written length
            // and ProcessInstanceId to reject non-range-index keys.
            var outputSpan = output.SpanByteAndMemory.IsSpanByte
                ? output.SpanByteAndMemory.SpanByte.ReadOnlySpan
                : output.SpanByteAndMemory.MemorySpan;

            if (outputSpan.Length < IndexSizeBytes)
            {
                status = GarnetStatus.NOTFOUND;
                rangeIndexLocks.ReleaseSharedLock(sharedLockToken);
                return default;
            }

            status = GarnetStatus.OK;
            return new(in rangeIndexLocks, sharedLockToken);
        }

        /// <summary>
        /// Acquire an exclusive lock for the given key hash.
        /// Used by TryDeleteRangeIndex to prevent concurrent data operations
        /// while a BfTree is being freed.
        /// </summary>
        internal ExclusiveRangeIndexLock AcquireExclusiveForDelete(long keyHash)
        {
            rangeIndexLocks.AcquireExclusiveLock(keyHash, out var exclusiveLockToken);
            return new(in rangeIndexLocks, exclusiveLockToken);
        }
    }
}