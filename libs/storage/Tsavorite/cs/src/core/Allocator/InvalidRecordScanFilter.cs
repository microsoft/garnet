// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Invalid records that a scan iterator skips before copying their fields. Invalid records reach an iterator only if it
    /// includes closed records, as bounded lookup scans do so that they can return sealed, valid older versions.
    /// </summary>
    internal enum InvalidRecordScanFilter : byte
    {
        /// <summary>Skip no additional records; used by physical scans.</summary>
        None,

        /// <summary>
        /// Skip invalid records that are not sealed; used by lookup scans. Such a record never was, or no longer is, a version of its
        /// key: an allocation abandoned before its header was written, a record invalidated by exception handling after its insert's
        /// CAS, or a later-version record discarded by recovery, which clears Sealed on each page it loads. A discarded record can
        /// hold stale object ids, so copying its fields could fail or alias another record's objects.
        /// <para>Invalid sealed records are returned. They are elided records, and new records whose insert is in progress or never
        /// committed: a new record starts sealed and invalid until its CAS succeeds. In memory, such a record's object ids are released
        /// or still owned by it. A flush writes no objects for an invalid record, however, so the object iterator also skips an invalid
        /// record read from disk that still has heap fields, such as a tombstone elided by an in-place delete, which keeps its overflow
        /// key. ConditionalScanPush pushes an invalid sealed record only if its key has a record past the scan bound.</para>
        /// </summary>
        SkipUnsealed,

        /// <summary>Skip every invalid record, including elided ones; used by streaming checkpoint phase 2.</summary>
        SkipAll,
    }

    internal static class InvalidRecordScanFilterExtensions
    {
        /// <summary>Whether <paramref name="filter"/> skips a record with <paramref name="recordInfo"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool Skips(this InvalidRecordScanFilter filter, RecordInfo recordInfo)
            => recordInfo.Invalid && (filter == InvalidRecordScanFilter.SkipAll || (filter == InvalidRecordScanFilter.SkipUnsealed && !recordInfo.IsSealed));
    }
}