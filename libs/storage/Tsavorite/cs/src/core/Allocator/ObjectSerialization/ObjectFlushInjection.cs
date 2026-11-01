// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace Tsavorite.core
{
    /// <summary>Test only: the points at which <see cref="ObjectFlushInjection"/> calls a test's hook. The calls are compiled out of Release
    /// builds, and nothing in the store depends on them.</summary>
    internal enum ObjectFlushPhase
    {
        /// <summary>The record's out-of-line components have been captured but the live <c>RecordInfo</c> has not been re-read.
        /// This is the window a concurrent elision must land in for the capture-then-recheck logic to be exercised.</summary>
        AfterCapture,

        /// <summary>The record's object bytes have been written and its object-log position stamped. The record is inside the
        /// flush window here, so <c>IsFrozenForFlush</c> holds for its address.</summary>
        AfterRecordWritten,

        /// <summary>Test only. A flush holds the claim on the record's value, an object or an overflow array, and has written the value
        /// to the object log, but has not stamped the record. A capture of the record waits for the claim here.</summary>
        ClaimHeld,

        /// <summary>Snapshot coordination has entered <c>CapturingCutoff</c> but has not yet sampled the ReadOnly cutoff. This is
        /// the window in which a ReadOnly worker can publish <c>LastIssuedFlushedUntilAddress</c> and must still be classified
        /// correctly, either inside the cutoff cohort or as post-cutoff. The address passed is the cutoff candidate at entry.</summary>
        SnapshotCutoffCapturing,

        /// <summary>Test only. A flush or a capture claiming an overflow array for publication has read the array's phase word and is
        /// about to take it by CAS; another publisher can claim and release the same word with a different owner in between. The
        /// hook runs on the claiming thread, which for a capture holds the store epoch and the key's bucket lock. The address passed
        /// is 0.</summary>
        PublicationClaimSampled
    }

    /// <summary>
    /// Test only: interleave points in the flush paths and in an overflow array's publication claim, letting a test run an operation at
    /// a precise moment rather than hoping a background race reproduces.
    /// </summary>
    /// <remarks>
    /// Non-generic so a test can reach it without naming the store-functions type, and every entry point is
    /// <see cref="ConditionalAttribute"/> on DEBUG so a Release build compiles out the calls and their arguments entirely -- the flush
    /// carries no production cost, not even a null check.
    /// <para>The flush points run ON THE FLUSH THREAD while it holds no epoch, so a handler may block, but it must not perform a store
    /// operation that would re-enter this allocator's flush path. <see cref="ObjectFlushPhase.PublicationClaimSampled"/> also runs on the
    /// thread of a CopyUpdate whose capture claims an overflow array; that thread holds the store epoch and the key's bucket lock while
    /// the handler runs. Handlers are expected to signal the test thread and optionally wait.</para>
    /// </remarks>
    internal static class ObjectFlushInjection
    {
        /// <summary>Invoked at each <see cref="ObjectFlushPhase"/> with the record's logical address. Null when no test is observing.</summary>
        internal static Action<ObjectFlushPhase, long> Hook;

        /// <summary>Whether the injection points are compiled in. False in Release, where <see cref="ConditionalAttribute"/> removes
        /// every call site, so a hook can be assigned but will never fire.</summary>
        /// <remarks>A test that depends on the hook firing must check this and skip itself, otherwise it waits for a callback that
        /// cannot arrive and fails on its timeout in Release builds only.</remarks>
        internal static bool IsAvailable =>
#if DEBUG
            true;
#else
            false;
#endif

        /// <summary>Call the hook, if any, for <paramref name="phase"/> at <paramref name="logicalAddress"/>.</summary>
        [Conditional("DEBUG")]
        internal static void At(ObjectFlushPhase phase, long logicalAddress) => Hook?.Invoke(phase, logicalAddress);

        /// <summary>Clear the hook. A test must call this in teardown so a leaked handler cannot affect a later test.</summary>
        [Conditional("DEBUG")]
        internal static void Reset() => Hook = null;
    }
}