// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#define CALLOC

namespace Tsavorite.core
{
    internal struct HashIndexPageAsyncFlushResult
    {
        public int chunkIndex;
        public SectorAlignedMemory mem;

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
    }

    internal struct OverflowPagesFlushAsyncResult
    {
        public SectorAlignedMemory mem;
    }

    internal struct OverflowPagesReadAsyncResult
    {
    }
}