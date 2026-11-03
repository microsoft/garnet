---
id: tsavoritelog-commits
sidebar_label: TsavoriteLog Commits
title: TsavoriteLog Commits and Recovery
---

# TsavoriteLog Commits and Recovery

`TsavoriteLog` is Tsavorite's append-only log; Garnet's AOF is a `TsavoriteLog`. This page describes how a commit becomes
durable, what recovery reads, and how to prepare a recovered log for appending.

## Commit metadata

A commit is described by `TsavoriteLogRecoveryInfo`. Its serialized form is stored in two places:

- **Commit record**: in fast-commit mode, an entry in the log itself whose length field is negative and whose payload is
  the serialized `TsavoriteLogRecoveryInfo`.
- **Commit-metadata file**: written through `ILogCommitManager.Commit`, one per commit number.
  `DeviceLogCommitCheckpointManager` names it `log-commits/commit.<CommitNum>`.

| Field | Meaning |
| --- | --- |
| `BeginAddress` | Lowest valid address of the log as of this commit; the log below it may have been truncated |
| `UntilAddress` | Address up to which the log is committed. In fast-commit mode this is the end of the commit's commit record. Recovery restores the log's tail to it. |
| `CommitNum` | One more than the previous commit's number, or a larger number proposed by the caller. It names the commit-metadata file and selects the commit for `RecoverAsync(commitNum)`. |
| `Cookie` | Caller-supplied data, returned by recovery as `RecoveredCookie` |

## Committing

**Without fast commit** (the default):

1. A commit flushes the log through its tail.
2. It then writes the commit-metadata file. The commit completes only when that file is written.
3. Recovery restores the log to the `UntilAddress` of the newest readable commit-metadata file; it does not scan the log.

**Fast-commit mode** (`TsavoriteLogSettings.FastCommitMode`) needs one sequential write per commit:

1. A commit appends a commit record at the tail.
2. It flushes the log through that record. The commit completes when that flush completes.

The commit-metadata file is written afterwards, and only sometimes:

- **Throttling:** `DeviceLogCommitCheckpointManager` writes one every `fastCommitThrottleFreq` commits. In Garnet, this is
  `--fast-commit-throttle`, which defaults to 1000.
- **Forced writes:** a write is forced when the log's begin address moves, because recovery cannot start scanning in
  deleted segments.

In fast-commit mode, the files only give recovery a recent starting point. Garnet's AOF always uses fast-commit mode.

## Recovery

`RecoverAsync()` recovers to the latest commit:

1. **Load a starting commit:** read the newest readable commit-metadata file. A file that cannot be read is skipped in
   favor of the next older one.
2. **Scan forward (fast-commit mode only):**
   - The scan starts at that commit's `UntilAddress` and reads the device to the end of the written log. Without a readable
     commit-metadata file, it starts at the beginning of the log.
   - Entries are skipped. Each intact commit record is accepted.
   - A zero length field away from a page start is end-of-page padding, and the scan resumes at the next page. The rest of
     the page must be zero, since the allocator writes nothing after padding.
   - The scan ends at the end of the written log, at an invalid record (for example, after a torn write), or at data after
     end-of-page padding.
   - A read error fails recovery with `TsavoriteIOException`, since the end of the log is then unknown. With
     `TolerateDeviceFailure`, recovery instead keeps the last commit the scan found.
3. **Restore:** the log's tail is set to the last accepted commit's `UntilAddress`, `RecoveredCookie` to the cookie of the
   latest accepted commit that has one, and commit numbering continues from that commit's number.

`RecoverAsync(commitNum)` starts from the newest readable file for that commit or an earlier one, and scans the same way
until it reaches the requested commit.

## Appending after recovery

Recovery does not change the device. Everything above the recovered tail stays there: the rest of a write that was torn
by a crash, the commits after an earlier commit that `RecoverAsync(commitNum)` recovered, or, after `Initialize`,
`SafeInitialize` or `Reset`, a previous history. This lets a log be recovered to inspect it or to choose a commit.

Once entries are appended at the tail, though, a later fast-commit recovery can scan from them into that older data and
adopt its commit records. For example, new entries that end a page lead the scan to the next page, which still holds the
older data. Before appending to a recovered log, clear the device above the tail, in one of two ways:

- `RecoverAsync(commitNum, clearAboveTail: true)`, with -1 for the latest commit, clears as part of recovery.
- `PrepareToAppend()` clears a log that was recovered, initialized or reset without changes, once the caller decides to
  append to it.

Either way, every record above the tail is lost, including later commits that `RecoverAsync(commitNum)` left in place.
Appending without clearing keeps the earlier behavior; the padding check above then still stops a scan whose new tail
page is followed by older data on the same page.

### What clearing does

1. **Commit metadata:** removes each commit-metadata file numbered above the current commit. Its commit is above the
   tail, or it belongs to a history that `Initialize` or `Reset` replaced, since both restart numbering. A file numbered
   at or below the current commit is kept, so earlier commits can still be recovered, unless it ends above the tail,
   which can happen after `Initialize` with a `lastCommitNum` above 0. Files are removed before any data is cleared, so a
   crash in between recovers an earlier commit over unchanged data.
2. **Segments above the tail's:** the device removes every segment it holds above the tail's segment
   (`IDevice.TryRemoveSegmentsAbove`), however far above the tail they are. This covers the old segments a replica keeps
   when a full sync positions its log far below them, with nothing in between.
   - File devices delete the segment files; `LocalMemoryDevice` drops the segments; Azure deletes the page blobs it found
     when it started and those it created since.
   - `NativeStorageDevice` truncates the segment files to zero length instead. Its `RemoveSegment` removes every segment
     from its first through the one named, and it may hold the files open, so deleting them could lose later writes.
   - `IDevice.RemoveSegment` is not used, because on `NativeStorageDevice` it would remove the tail's segment too.
3. **Find the rest of the data:** the device cannot report how far its data extends, so clearing reads forward from the
   page after the tail's until it encounters a run of consecutive empty pages as long as the page buffer (the pages the
   log holds in memory), or until the end of the tail's segment if the segments above were removed. A page is empty if
   every byte reads as zero, including bytes past the end of a file or in a segment that does not exist.
   - The run's length bounds the gaps within the data, not the amount of data. The data above the tail is what the log
     held before, contiguous except where a crash stopped writes partway, and no more pages than the buffer holds are
     being written at once. So the first such run is past all the data, however much there is.
   - Each page's first sector is read first, and the rest of the page only if that sector is empty, since a torn write
     can lose a page's first sector and keep later ones.
4. **Zero it, highest address first:** from the end of the data down to the tail, including any empty pages among it.
   The sector holding the tail is rewritten last, keeping the bytes below the tail.

   Steps 2 to 4 change the device from the highest address down. If clearing is interrupted, what remains above the
   tail is the lower part of what was there, so the next attempt finds it, and recovery can adopt only a commit that was
   already there.
5. **Commit metadata for the tail:** writes a commit-metadata file for the tail, so recovery starts there.

Clearing costs a few reads when nothing is above the tail, which is usual after a clean shutdown: a directory listing,
and the rest of the tail's segment, which usually lies past the end of the file. After a crash it zeroes the data that
was being written, up to about the size of the page buffer. After `RecoverAsync(commitNum)` far back, `Initialize` or
`Reset`, it removes the segments above the tail's and zeroes the rest of the tail's segment, reading about one sector
per page of it. A device that cannot remove its segments above the tail's (the default for `IDevice` implementations
outside Tsavorite, and the tiered and sharded devices) has all the data above the tail zeroed instead, which can be most
of the log; reading past that data can leave empty segment files on such a device.

`PrepareToAppend` throws if the log is read-only or completed, if anything has been appended since the log was recovered,
initialized or reset, or if recovery could not determine what the log holds: it failed, could not read the tail's page,
or, without fast commit, found no commit while commit-metadata files could not be read. It does nothing if the log has
already been cleared. It must not run concurrently with appends.

## Serialized format

All values are little-endian. Bytes after a serialized record are ignored, such as the sector padding of a
commit-metadata file.

| Offset | Field |
| --- | --- |
| 0 | int version (1; version 0 is also read) |
| 4 | long checksum: XOR of `BeginAddress`, `UntilAddress`, `CommitNum`, cookie length, and the cookie's bytes |
| 12 | long `BeginAddress` |
| 20 | long `UntilAddress` |
| 28 | long `CommitNum` |
| 36 | int cookie length (-1 for none) |
| 40 | cookie bytes |

## Limitations

- **Torn entries without checksums:** with `LogChecksumType.None`, a torn entry whose length field survived reads as valid,
  and a later intact commit record is adopted. Per-entry checksums detect most torn entries.
- **Logs appended to without clearing:** clearing protects the next recovery only if it runs before the first append. A
  log that a caller, or an earlier build, appended to after recovery without clearing can still hold older data above
  its tail.
- **Data beyond a long gap:** within the tail's segment, and anywhere above it on a device that cannot remove its
  segments above the tail's, clearing stops at the first run of empty pages as long as the page buffer, so data beyond
  a longer gap stays on the device. Such a gap is left by a crash while the log had a larger page buffer, or by
  addresses that were never written or were truncated, when `Initialize` or `Reset` positions the log below them. A
  later recovery can adopt that data's commits once appends reach it.
- **Drive write caches:** clearing is only as durable as the device's acknowledged writes, as every commit is.
