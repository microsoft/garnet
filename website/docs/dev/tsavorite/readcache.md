---
id: readcache
sidebar_label: Read Cache
title: Read Cache
---

# Read Cache

The **read cache** keeps hot, on-disk records in memory so that reads of those keys
can be served without a disk IO. It is an optional, in-memory, circular log
(`readcacheBase`) that sits "in front of" the main hybrid log. Read-cache records
are **redundant copies of committed data that also lives in the main log / on disk**
— this redundancy is the property that makes the design simple: *a read-cache record
can always be dropped, and the key will simply be re-promoted on its next read.*

This page describes how the read cache is woven into the hash chain, the concurrency
model that keeps it correct **without a lock**, and the rationale behind each design
choice. It also documents a future optimization ("fresh re-insertion") with an
implementation sketch.

## Chain structure

Every key hashes to a hash-table bucket entry, which points to the head of a chain
of records linked by `RecordInfo.PreviousAddress`. When the read cache is enabled, a
key's chain has the form:

```
                  |------- read-cache prefix -------|   |-------- main log --------|
   hashEntry  ->  rcN  ->  ...  ->  rc2  ->  rc1     ->  mM  ->  ...  ->  m1  ->  0
```

- Read-cache addresses carry a high bit (`RecordInfo.kIsReadCacheBitMask`, the top
  bit of the 48-bit address); `LogAddress.IsReadCache` / `AbsoluteAddress` test and
  strip it.
- **The read-cache records form a contiguous prefix at the head**, above all
  main-log records. Many components rely on this invariant (`FindInReadCache`, the
  read path, and checkpoint's `SkipReadCacheBucket`).
- Within the prefix, **addresses strictly decrease** walking down: `rcN` (highest /
  newest, at the head) … `rc1` (lowest / oldest, at the read-cache/main-log
  boundary). The read cache is a circular log, so **eviction reclaims the lowest
  (oldest) addresses** — i.e. the boundary records `rc1`, `rc2`, … first.

## The single linearization point: CAS only on the hash entry

The central design rule is:

> **Every *structural* compare-and-swap targets only the hash-table entry word.**
> No operation mutates an *existing* read-cache record's `PreviousAddress` to
> publish a new value.

Two consequences follow, and together they replace the bucket lock for read-cache
structure:

1. **Interior chain pointers are immutable after publication** (with one carefully
   reasoned exception in eviction, below). A reader or a chain walk can follow
   `PreviousAddress` links without coordinating with writers.
2. **The hash-entry CAS is the sole linearization point.** Concurrent inserters
   (read-promotions, updates) all contend on the *same* word, so the loser of a CAS
   retries and observes the winner.

### The Monotonic-Head invariant

> **No structural operation may ever republish a head value that was previously
> superseded.** Once the hash-entry head advances away from a value `X` due to a
> structural change, it must never return to `X`. Every operation that publishes a
> new head publishes a **fresh, never-before-used** address (a new main-log record,
> or a freshly-allocated read-cache record).

This invariant is what makes the hash-entry CAS a sound linearization point under
full concurrency: a stale CAS that still expects an old head value **fails iff any
structural change committed in between** — which is exactly what prevents the
lost-update A-B-A described later.

## Operations

### Read promotion (insert at head)

When a pending read brings a record in from disk (or copies from the immutable
region), `TryCopyToReadCache` allocates a fresh read-cache record, copies the value,
sets its `PreviousAddress` to the current head, and **CAS's it into the hash entry**
(`hei.TryCAS`). If the CAS loses to a concurrent insert it is abandoned (read-cache
population is best-effort). Read promotion is *value-preserving*: it caches the
current committed value.

### Value-changing update (Upsert / RMW / Delete): detach

A value-changing update must atomically (a) publish the new value and (b) ensure no
stale read-cache copy of the key shadows it. It does this with a **single hash-entry
CAS that detaches the whole read-cache prefix** (`CASRecordIntoChain`):

1. The new main-log record `newM` is written with `newM.PreviousAddress` already set
   to the first main-log address (`recSrc.LatestLogicalAddress`), i.e. the address
   *below* the prefix.
2. `hei.TryCAS(newM)` swings the hash entry from the prefix head to `newM`. This one
   word write **atomically commits the new value and orphans the entire read-cache
   prefix** (it is no longer reachable from the hash entry).

The orphaned prefix records are reclaimed by `ReadCacheEvict` when their pages close.
Bystander keys (other keys whose copies were in the prefix) are **dropped** and
re-promoted on their next read. The updated key's stale copy is gone simply because
the whole prefix is unreachable — no separate invalidation step is needed.

> **Why an interior splice would need a lock.** The pre-redesign code instead spliced
> `newM` at the read-cache/main-log boundary (CAS on `rc1.PreviousAddress`) and then
> invalidated the updated key's read-cache copy — two writes at two different chain
> locations. A concurrent read-promotion inserts at the **head**, *above* the splice
> point, so it can resurrect a stale copy that shadows the just-committed update:
>
> ```
> 1. H -> rc_K(V) -> rc1 -> mainlogHead -> M_K(V)      K currently reads V
> 2. U prepares newM(V'), Prev = mainlogHead
> 3. concurrent reader promotes K: inserts rc_K_new(V) at HEAD
> 4. U splices: rc1.Prev = newM(V')
>      H -> rc_K_new(V) -> ... -> rc1 -> newM(V') -> mainlogHead
> 5. later read of K hits rc_K_new(V) first -> returns V (STALE; U committed V')
> ```
>
> The splice writes at the boundary; the promotion writes at the head; nothing
> serializes them without a lock. The detach makes the update and all head-inserters
> contend on the **same** hash-entry word, giving the single linearization point that
> replaces the lock.

### Eviction: value-preserving, keep survivors via interior CAS

Eviction (`ReadCacheEvict` → `ReadCacheEvictChain`) runs from the allocator's
`OnPagesClosedWorker` **under epoch**, and **without** the bucket lock. It removes the
read-cache records in the page range being closed and **keeps the survivors above the
range in place**, by CAS'ing the lowest surviving record's `PreviousAddress` forward
to skip the evicted records (and CAS'ing the hash entry only when the evicted record
is the head). The evicted records are stamped `PreviousAddress = kTempInvalidAddress`
*after* they are unlinked.

This is the **one** place an existing record's `PreviousAddress` is mutated, and it is
safe lock-free for two reasons:

1. **Eviction is value-preserving.** It only removes *redundant* read-cache copies.
   A reader that observes the survivor's **old** `Prev` reaches the evicted record
   (value `V`); a reader that observes the **new** `Prev` skips to the main-log/disk
   record (same value `V`). Both yield the same result — there is no stale-vs-new
   mismatch a concurrent promotion could resurrect (a promotion during eviction would
   insert the **same** value `V`).
2. **It coordinates with concurrent walkers via `kTempInvalidAddress` + restart**, not
   the lock: a walker that has already advanced into an evicted record sees
   `PreviousAddress <= kTempInvalidAddress`, waits for eviction
   (`ReadCacheNeedToWaitForEviction`), and restarts the chain from the hash entry.

Eviction keeps the head untouched except to advance it monotonically toward the main
log when the head record itself is evicted — so it never restores a superseded head
and cannot cause the A-B-A.

### The value-changing vs value-preserving dividing line

The asymmetry above is the crux of the whole design:

| Path | Value semantics | Mechanism | Bystanders/survivors |
|---|---|---|---|
| Update (Upsert/RMW/Delete) | value-**changing** | **detach** (one hash-entry CAS) | dropped, re-promoted on read |
| Read promotion | value-preserving | head-insert (hash-entry CAS) | n/a |
| Eviction | value-**preserving** | interior `Prev` CAS (head untouched) | kept in place |

The dividing line is exactly: *can a concurrently-inserted head copy carry a value
that conflicts with the operation's result?* For an update it can (a promotion may
cache the old value), so the update needs the single-linearization-point detach. For
eviction it cannot (a promotion caches the same current value), so eviction's interior
CAS is safe without a lock.

## Coordination between updates and eviction

Updates (detach) and eviction (interior CAS) run concurrently and compose safely:

- **`newM.Prev` is always a *main-log* address**, and eviction only touches read-cache
  records — so eviction can never touch `newM.Prev`.
- **Eviction never moves the main-log boundary.** So the `mainlogHead` that an update
  sampled as `newM.Prev` stays valid regardless of concurrent eviction.
- **A detach pre-cleans the chain**: after a detach the live chain has no read-cache
  prefix, so any eviction work that lands afterward operates on now-orphaned records
  and is harmless; the pages being freed have no live referrer.
- The only shared CAS target is the hash entry when eviction evicts the *head*
  read-cache record — a plain CAS race resolved by retry.

## Memory reclamation and epoch protection

The read cache relies on Tsavorite's epoch protection for *memory reclamation*, not
for chain-traversal correctness. `OnPagesClosedWorker` runs `EvictCallback`
(= `ReadCacheEvict`) and frees the page buffers **after** it returns; an operation
that holds the epoch keeps the read-cache pages it is reading alive until it suspends.
Note that a single atomic `PreviousAddress` transition between two valid chains is
safe for a concurrent reader **without** epoch protection (the reader observes either
the old or the new successor, both valid); epoch is needed so eviction does not *free*
a page a reader or in-flight operation still references.

## Heap-size tracking parity

The read cache has its own `LogSizeTracker` (wired by Garnet's `CacheSizeTracker`)
that tracks the heap memory (value objects and overflow key/value) held by
read-cache records. This heap is adjusted at exactly two internal sites:

- **Create**: `TryCopyToReadCache` calls `logSizeTracker.UpdateSize(add: true)`
  *after* its head-CAS succeeds (a CAS failure abandons the allocation before any
  heap is added, so no decrement is owed).
- **Evict**: at page close, `EvictRecordsInRange` decrements each record's heap —
  but it **skips `Invalid`/`Null` records**, on the contract that an invalidated
  record already had its heap decremented at the invalidation site.

The load-bearing invariant is therefore: **no read-cache record that has had its
heap accounted may be marked `Invalid` before it is evicted.** If it were, eviction
would skip it and the `+UpdateSize` would never be matched — the tracker would drift
upward and the read cache would over-trim.

The detach design upholds this for free: a value-changing update detaches the whole
prefix with one hash-entry CAS, leaving every orphaned record **`Valid`**, so each is
decremented exactly once when its page closes. (The old splice design instead
invalidated the read-cache source in place after such an update — and during a
splice/promotion race — without decrementing, which leaked read-cache heap from the
tracker; removing those in-place invalidations fixed it.)

There is one in-place exception: an **RMW `CopyUpdate`** whose source is a read-cache
object record clears and disposes that source's value slot in place (the record stays
`Valid`; only the value is freed). It must decrement the **read-cache** tracker — the
allocator that *owns* the source record (`recSrc.AllocatorBase`), since that is where
the value heap was charged at promotion — not the main-log tracker. Eviction later
recomputes the now-zero value heap for the still-`Valid` record, so there is no
double-decrement. (Crediting the main-log tracker here was a latent bug: it leaked the
read-cache tracker and under-counted the main log.)

## Checkpointing

`SkipReadCacheBucket` runs during the index checkpoint over a copy of each bucket
page, walking the read-cache prefix and pointing the on-disk entry at the first
main-log record (read-cache addresses are never persisted). This depends on the
contiguous-prefix invariant; every *published* head state in this design is either a
main-log head or a read-cache record with a valid prefix below it, so the skip is
always well-defined.

## Tests

- `test/test.session/ReadCacheChainTests.cs` — deterministic chain/eviction scenarios
  with forced collisions (`LongKeyComparerModulo`); verifies that updates detach the
  prefix, eviction retains survivors, and values remain correct.
- `test/test.stress/ReadCacheStressTests.cs` — multi-threaded stress (1–8 read/write
  threads, forced collisions, concurrent eviction).
- `test/test.recordops/RecordLifecycleTests.cs` —
  `ReadCacheHeapSizeTrackerReturnsToZeroAfterUpdateAndEvict` asserts heap-tracker
  parity: after promoting heap-bearing records into the read cache, updating each
  cached key (value-changing), and evicting the read cache, the read-cache
  `LogSizeTracker` heap returns to zero (no leak).

---

## Future optimization: fresh re-insertion of bystanders (not implemented)

The current design **drops** bystander read-cache entries when an update detaches the
prefix; they are re-promoted lazily on the next read. This is fully correct and
minimal. The only cost is a read-cache hit-rate dip in the narrow case where an update
hits a key that **collides** (shares a hash bucket) with other cached keys: those
collided keys lose their cached copies until next read.

If profiling ever shows this matters, the optimization is to **re-insert the surviving
bystanders eagerly as fresh read-cache records** right after the detach. This section
is a complete sketch for a future implementer.

### Why "fresh", and not "re-splice the existing records"

Re-using the existing orphaned records (re-splicing them at the head) **reintroduces a
lost-update A-B-A** and must not be done:

```
1. H = rcN -> ... -> rc1 -> mainlogHead          (rcN is the original head)
2. stale updater U_s samples H = rcN, boundary mainlogHead
3. U1 detaches: CAS H: rcN -> newM1(V1)          (commit; head is now fresh)
4. U1 re-splices existing bystanders; the topmost ends up = rcN again
     H = rcN -> ... -> newM1 -> mainlogHead        (head restored to rcN!)
5. U_s: CAS H: rcN -> newM_s (Prev = mainlogHead) succeeds, orphaning newM1
     -> U1's committed update is LOST
```

Re-splicing puts an **old address back at the head**, and any old head address may
have been sampled by a still-in-flight updater whose boundary is now stale.
Distinguishing the restored `rcN` from the original would need a generation counter in
the hash-entry word, but the word is full (`[tentative][15-bit tag][48-bit address]`),
and the `tentative` bit is back to `false` exactly when the stale CAS strikes.
Keeping `newM` at the head and splicing bystanders *below* it keeps the head monotonic
but violates the contiguous-prefix invariant (a main-log record above read-cache
records), so `FindInReadCache` would stop at the main-log head and never reach the
bystanders.

**Fresh re-insertion** allocates *new, monotonically increasing* read-cache addresses,
so the head never returns to a sampled value — the A-B-A is impossible by
construction.

### Why the freshness check is simple

Two properties keep the freshness check to a single in-memory traceback:

1. **All structural inserts go through the hash-entry CAS.** A fresh bystander's
   head-insert CAS therefore fails if *any* record was inserted for *any* key in the
   bucket since the check — so the CAS itself validates "no concurrent insert."
2. **Self-cleaning via detach-on-update.** If a bystander key `K_b` is updated *after*
   it is re-inserted, that update detaches the prefix and drops the re-inserted record
   (it will even use it as its source). So a re-inserted record is either still
   current or atomically dropped by `K_b`'s next update — it can never linger as a
   stale shadow.

### Implementation sketch

Hook point: at the end of each create-record path that detaches a prefix
(`CreateNewRecordUpsert` / `…RMW` / `…Delete`, and `TryCopyToTail`), after the
post-CAS bookkeeping. Capture the **pre-detach hash-entry word** (the orphaned prefix
head) *before* calling `CASRecordIntoChain`, and pass it to a new helper. The
bystanders all share the operation's bucket, so the helper can reuse the operation's
`stackCtx.hei`.

```text
ReinsertDetachedReadCacheBystanders(detachedHead, opKey, sessionFunctions, ref pendingContext, ref stackCtx):
    if !UseReadCache: return
    reinserted = 0;  maxReinsert = <small bound>          // C12: bound epoch-held work
    walk = detachedHead
    while walk.IsReadCache and reinserted < maxReinsert:
        if AbsoluteAddress(walk) < readcacheBase.HeadAddress: break   // below eviction frontier; value no longer safely readable
        srcInfo = LogRecord.GetInfo(readcacheBase.GetPhysicalAddress(walk))
        next = srcInfo.PreviousAddress
        if next <= kTempInvalidAddress: break             // a concurrent eviction marked this orphaned record
        srcLogRecord = readcache.CreateLogRecord(walk)
        if !srcInfo.Invalid and !storeFunctions.KeysEqual(opKey, srcLogRecord):   // skip the updated key (newM is authoritative)
            TryReinsertOneBystander(srcLogRecord, ...)     // best-effort; ++reinserted on success
        walk = next

TryReinsertOneBystander(srcLogRecord, ...):
    key = srcLogRecord.Key
    stackCtx.hei.SetToCurrent(); stackCtx.SetRecordSourceToHashEntry(hlogBase)
    // (a) already cached?
    if stackCtx.hei.IsReadCache and FindInReadCache(key, ref stackCtx): return
    // (b) a newer in-memory main-log record? (would be shadowed -> drop)
    if TraceBackForKeyMatch(key, ref stackCtx.recSrc, minAddress: hlogBase.HeadAddress): return
    // else: key's most recent record is on disk == what srcLogRecord copied -> safe to re-promote
    sizeInfo = PopulateRecordSizeInfo(srcLogRecord.GetRecordFieldInfo())
    if !TryAllocateRecordReadCache(ref pendingContext, ref stackCtx, sizeInfo, out newLA, out newPA): return
    newRec = WriteNewRecordInfo(srcLogRecord, readcacheBase, newLA, newPA, sizeInfo,
                                previousAddress: stackCtx.hei.Address)   // hei.Address retains the read-cache bit
    newRec.TryCopyFrom(srcLogRecord)
    if stackCtx.hei.TryCAS(newLA | kIsReadCacheBitMask):                 // single head-insert CAS validates "no concurrent insert"
        readcacheBase.logSizeTracker?.UpdateSize(newRec, add: true); newRec.UnsealAndValidate(); ++reinserted
    else:
        // lost the CAS (a concurrent insert happened) -> our freshness check is stale; drop (best-effort)
        SetNewRecordInvalid(newRec); OnDispose(newRec, InitialWriterCASFailed); newRec.PreviousAddress = kTempInvalidAddress
```

### Correctness obligations for the implementer

- **No interior `PreviousAddress` writes** on this path (fresh records only); the only
  contended write is the head-insert hash-entry CAS, so Monotonic-Head holds.
- **Conservative drop:** if the freshness traceback would have to descend below
  `hlogBase.HeadAddress` (the key's authoritative record may have escaped to disk),
  **drop** rather than risk shadowing a newer record. (In practice, under epoch a
  record created during the operation cannot escape to disk, so an in-memory traceback
  to `HeadAddress` is sufficient; still, err toward dropping.)
- **Bounded epoch-held work** (`maxReinsert`): re-insertion runs under epoch and does
  `O(prefix)` allocations/copies; cap it so eviction's page-close is not starved.
- **Stop the walk on `kTempInvalidAddress` / below `HeadAddress`**: a concurrent
  eviction may be reclaiming the orphaned prefix; never read a record whose page may
  be closing.
- **Semantics change to expect in tests:** the updated key's read-cache copy is
  *dropped* (not retained-and-invalidated), and retained bystanders become *fresh
  copies* at new addresses. Update `ReadCacheChainTests.cs` accordingly, and add a
  no-latch lost-update regression that forces the A-B-A schedule above and asserts no
  committed record is lost.
