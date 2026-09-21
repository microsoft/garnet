// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define CALLOC

namespace Tsavorite.core
{
    internal struct HashIndexPageAsyncFlushResult
    {
        public int chunkIndex;
        public SectorAlignedMemory mem;

        /// <summary>Number of bytes this chunk's write was issued for. A device may complete a write successfully but
        /// short (e.g. Linux truncates a single request to MAX_RW_COUNT), so the completion callback compares the
        /// transferred count against this to detect a chunk that is only partially on disk.</summary>
        public uint numBytesToWrite;

        /// <summary>Shared one-shot guard for releasing this chunk's native index-checkpoint IO unit. This result is
        /// boxed when passed as the device callback's context, so the issuer's copy and the callback's copy are
        /// distinct structs; the guard is a reference type so both copies claim against the same cell. Guards the
        /// case where a device invokes the completion callback synchronously (which releases) and the submit then
        /// throws, so the issuer's catch would otherwise release a second time and underflow the counter.</summary>
        public System.Runtime.CompilerServices.StrongBox<int> ioUnitReleaseGuard;

        /// <summary>Atomically claim the right to release this chunk's IO unit exactly once. Returns true to the
        /// single caller that should perform the release.</summary>
        public readonly bool TryClaimIoUnitRelease()
            => System.Threading.Interlocked.Exchange(ref ioUnitReleaseGuard.Value, 1) == 0;
    }

    internal struct HashIndexPageAsyncReadResult
    {
        public int chunkIndex;

        /// <summary>Number of bytes this chunk's read was issued for, compared against the transferred count in the
        /// completion callback so a successful but short read does not leave part of the hash table unrecovered.</summary>
        public uint numBytesToRead;

        /// <summary>Shared one-shot guard for retiring this chunk from the recovery countdown. See
        /// <see cref="HashIndexPageAsyncFlushResult.ioUnitReleaseGuard"/> for why this must be a reference type.</summary>
        public System.Runtime.CompilerServices.StrongBox<int> retirementGuard;

        /// <summary>Atomically claim the right to retire this chunk from the countdown exactly once. Returns true to
        /// the single caller that should decrement.</summary>
        public readonly bool TryClaimRetirement()
            => System.Threading.Interlocked.Exchange(ref retirementGuard.Value, 1) == 0;
    }

    internal struct OverflowPagesFlushAsyncResult
    {
        public SectorAlignedMemory mem;

        /// <summary>Index of the allocator level this write covers, so a failure names the level that failed.</summary>
        public int levelIndex;

        /// <summary>Number of bytes this level's write was issued for, compared against the transferred count in the
        /// completion callback to detect a successful but short write.</summary>
        public uint numBytesToWrite;

        /// <summary>Shared one-shot guard for retiring this level from the checkpoint's outstanding-write count. See
        /// <see cref="HashIndexPageAsyncFlushResult.ioUnitReleaseGuard"/> for why this must be a reference type.</summary>
        public System.Runtime.CompilerServices.StrongBox<int> retirementGuard;

        /// <summary>Atomically claim the right to retire this level exactly once. Returns true to the single caller
        /// that claims it.</summary>
        public readonly bool TryClaimRetirement() => System.Threading.Interlocked.Exchange(ref retirementGuard.Value, 1) == 0;
    }

    internal struct OverflowPagesReadAsyncResult
    {
        /// <summary>Index of the allocator level this read covers, so a failure names the level that failed.</summary>
        public int levelIndex;

        /// <summary>Number of bytes of this level that must be recovered. The completion callback fails the recovery
        /// if fewer bytes were transferred.</summary>
        public uint numBytesToRead;

        /// <summary>Shared one-shot guard for retiring this level from the recovery countdown. See
        /// <see cref="HashIndexPageAsyncFlushResult.ioUnitReleaseGuard"/> for why this must be a reference type.</summary>
        public System.Runtime.CompilerServices.StrongBox<int> retirementGuard;

        /// <summary>Atomically claim the right to retire this level from the countdown exactly once. Returns true to
        /// the single caller that should decrement.</summary>
        public readonly bool TryClaimRetirement()
            => System.Threading.Interlocked.Exchange(ref retirementGuard.Value, 1) == 0;
    }
}