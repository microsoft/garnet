---
id: compaction
sidebar_label: Compaction
title: Log Compaction
---

When Garnet is configured to run with storage using `EnableStorageTier` or `--storage-tier`, data that does not fit in memory will spill to disk storage.
Data on disk is split into segments, with one physical file per segment. The size of each segment is configured using `SegmentSize` or `--segment` for
the main store, and `ObjectStoreSegmentSize` or `--obj-segment` for the object store.

File segments continue to get created and added over time, so we need a way to delete older segments. This is where compaction comes in.

## Triggering Compaction

You can configure `CompactionFrequencySecs` or `--compaction-freq`, which creates a task that wakes up every so often to try compaction. If the number
of segments on disk exceeds `CompactionMaxSegments` or `--compaction-max-segments`, compaction runs using the specified strategy so that we end up with
at most `CompactionMaxSegments` active segments. The oldest segments are our chosen candidates for compaction. For the object store, the corresponding
switch is `ObjectStoreCompactionMaxSegments` or `--obj-compaction-max-segments`.


## Compaction Strategy

The candidate segments for compaction are processed using some strategy, specified using the 
`CompactionType` or `--compaction-type` switch. Available options are:
* None: No compaction is performed.
* Shift: The inactive segments are simply marked as ready for deletion.
* Scan: The entire log is scanned to check which records in the candidate segments to be compacted are "live", and these live records are copied to the tail of the log (in memory).
* Lookup: For every record in the candidate segments to be compacted, we perform a random lookup to check if it is live. As before, the live records are copied to the tail of the log (in memory).

## Segment Deletion

After the compaction strategy is applied on the candidate segments, they are inactive and eligible for deletion. However, the inactive segments are not 
immediately deleted from disk by default, since doing so can cause data loss in case the server crashes before taking the next checkpoint (and the AOF is disabled).
Instead, the next checkpoint will automatically cause the deletion of the inactive segments.

In case you are not taking checkpoints and want to force the physical deletion of inactive segments immediately after the compaction strategy is applied, you can specify
the override `CompactionForceDelete` or `--compaction-force-delete` switch. Note that this option can cause data loss when we recover to the previous 
checkpoint, in case the AOF is disabled.
