# Plan: Replace Upsert Value Parameter with Input in Tsavorite

## Problem Statement

Currently, Tsavorite's Upsert operation takes both a **TInput** and a **separate value** parameter (as `ReadOnlySpan<byte>`, `IHeapObject`, or `ISourceLogRecord`). The value is passed directly to the session function callbacks (`InitialWriter`, `InPlaceWriter`, etc.). In contrast, RMW only takes **TInput** and lets the session functions derive/compute values from it.

**Goal:** Align Upsert with RMW's pattern — remove the separate value parameter so the session functions receive only Input and are responsible for determining what value to write. Eliminate the `IUpsertValueSelector` dispatch infrastructure entirely.

**Scope:** Tsavorite only (the `libs/storage/Tsavorite/cs/` subtree). All Tsavorite unit tests must pass. Performance must not regress.

## Current Architecture

### Upsert Callback Signatures (ISessionFunctions)
Each callback has **3 overloads** for value type dispatch:
- `InitialWriter(..., ReadOnlySpan<byte> srcValue, ...)` — span values
- `InitialWriter(..., IHeapObject srcValue, ...)` — object values
- `InitialWriter<TSourceLogRecord>(..., in TSourceLogRecord inputLogRecord, ...)` — log record copy
- Same pattern for `InPlaceWriter`, `PostInitialWriter`, `PostUpsertOperation`

### RMW Callback Signatures (ISessionFunctions) — the target pattern
Each callback has **1 method** (no value parameter):
- `InitialUpdater(ref LogRecord, in RecordSizeInfo, ref TInput input, ref TOutput output, ref RMWInfo)`
- `InPlaceUpdater(ref LogRecord, ref TInput input, ref TOutput output, ref RMWInfo)`

### Value Dispatch Infrastructure
`IUpsertValueSelector` with 3 implementations (`SpanUpsertValueSelector`, `ObjectUpsertValueSelector`, `LogRecordUpsertValueSelector`) dispatches from `InternalUpsert` to the correct overload. **This entire infrastructure will be eliminated.**

### Size Calculation Chain
Value flows through: `IAllocator.GetUpsertRecordSize(value)` → `IVariableLengthInput.GetUpsertFieldInfo(value)`. Both have 3 overloads for 3 value types. **These collapse to a single overload taking only Input.**

### Internal Callers of LogRecord-based Upsert
- **Compaction/CompactScan** (`TsavoriteCompaction.cs:88-100`): uses `NoOpSessionFunctions`, calls `Upsert(in iterLogRecord)` and `Delete`
- **Iterator** (`TsavoriteIterator.cs:230`): uses **caller-provided TFunctions** (not NoOpSessionFunctions), calls `Upsert(in iterLogRecord)` and `Delete`
- **Compaction/CompactLookup** (`TsavoriteCompaction.cs:38`): uses `CompactionCopyToTail` (not Upsert) — unaffected
- **AllocatorScan** (`AllocatorScan.cs:221`): uses `ConditionalScanPush` (not Upsert) — unaffected

CompactScan uses `NoOpSessionFunctions` and the Iterator uses caller-provided functions for its temp KV. Both will switch to the new `LogRecordCopySessionFunctions<ITsavoriteScanIterator>`, which performs raw record copies. This is a semantic change for the iterator — caller-provided functions will no longer affect temp-KV maintenance. This is correct because the temp KV is an internal implementation detail for deduplication, and raw copy is the intended behavior.

## Design Decisions

### Decision 1: Eliminate UpsertValueSelector entirely
ALL Upsert goes through Input only. No overloads, no value dispatch.

**After the change, ISessionFunctions Upsert callbacks have exactly ONE form each:**
```csharp
bool InitialWriter(ref LogRecord, in RecordSizeInfo, ref TInput, ref TOutput, ref UpsertInfo)
void PostInitialWriter(ref LogRecord, in RecordSizeInfo, ref TInput, ref TOutput, ref UpsertInfo)
bool InPlaceWriter(ref LogRecord, ref TInput, ref TOutput, ref UpsertInfo)
void PostUpsertOperation<TKey, TEpochAccessor>(TKey key, ref TInput, ref UpsertInfo, TEpochAccessor)
```

This exactly mirrors RMW's pattern (single method per callback, value derived from Input).

### Decision 2: Create generic LogRecordInput + LogRecordCopySessionFunctions (zero boxing)
For internal record-copy operations (compaction, iteration), introduce generic types parameterized on `TSourceLogRecord`:

```csharp
/// Wraps a source log record as an Input for copy operations.
/// Generic to avoid boxing — all ISourceLogRecord implementations are structs
/// (LogRecord, DiskLogRecord). ITsavoriteScanIterator also extends ISourceLogRecord
/// and is implemented by classes (SpanByteScanIterator, ObjectScanIterator).
internal struct LogRecordInput<TSourceLogRecord> where TSourceLogRecord : ISourceLogRecord
{
    public TSourceLogRecord SourceRecord;
}

/// Session functions for log record copy operations (compaction, iteration).
internal struct LogRecordCopySessionFunctions<TSourceLogRecord>
    : ISessionFunctions<LogRecordInput<TSourceLogRecord>, Empty, Empty>
    where TSourceLogRecord : ISourceLogRecord
{
    // InitialWriter: dst.TryCopyFrom(in input.SourceRecord, ...) — no boxing
    // InPlaceWriter: same pattern
    // GetUpsertFieldInfo: derives size from input.SourceRecord
    // Delete methods: simple pass-through (return true)
    // All other methods: no-op or throw
}
```

Concrete instantiations:
- **Compaction/iteration**: `TSourceLogRecord = ITsavoriteScanIterator` (interface, backed by classes — no boxing)
- **Recovery/other**: `TSourceLogRecord = LogRecord` or `DiskLogRecord` (structs stored by value — no boxing)

### Decision 3: Update internal callers to use dedicated session
- **CompactScan**: `NewSession<..., LogRecordInput<ITsavoriteScanIterator>, Empty, Empty, LogRecordCopySessionFunctions<ITsavoriteScanIterator>>(...)`
- **TsavoriteKVIterator**: same pattern
- Call pattern changes from `Upsert(in iterLogRecord)` to `Upsert(key, ref logRecordInput)`
- The iterator class type parameters simplify (no longer needs TInput/TOutput/TContext/TFunctions)

### Decision 4: Simplify public API completely
**BasicContext.Upsert** has only Input-based overloads (like RMW):
```csharp
Upsert(TKey key, ref TInput input, ref TOutput output, ...)
Upsert(TKey key, ref TInput input, ...)  // convenience
```
All `ReadOnlySpan<byte>`, `IHeapObject`, and `ISourceLogRecord` overloads are removed.

### Decision 5: Single GetUpsertFieldInfo/GetUpsertRecordSize
```csharp
// IVariableLengthInput — single overload
RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ref TInput input)

// IAllocator — single overload
RecordSizeInfo GetUpsertRecordSize<TKey, TInput, TVariableLengthInput>(TKey key, ref TInput input, TVariableLengthInput varlenInput)
```

### Decision 6: NoOpSessionFunctions stays generic, simplified
`NoOpSessionFunctions<TInput, TOutput, TContext>` remains for non-Upsert internal use (CompactLookup, AllocatorScan). Its Upsert methods become single Input-only stubs that throw `NotImplementedException` (these are never called in its use cases).

## Implementation Plan

### TODO 1: Update ISessionFunctions interface
**File:** `libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctions.cs`

In the `#region Upserts` section, collapse all 3×4 overloads to 4 single methods:
- Remove ALL `InitialWriter` overloads (span, object, logrecord) — replace with single `InitialWriter(ref LogRecord, in RecordSizeInfo, ref TInput, ref TOutput, ref UpsertInfo)`
- Remove ALL `PostInitialWriter` overloads — replace with single version
- Remove ALL `InPlaceWriter` overloads — replace with single version  
- Remove ALL `PostUpsertOperation` overloads — replace with single version (no value parameter, like `PostRMWOperation`)

### TODO 2: Update IVariableLengthInput interface
**File:** `libs/storage/Tsavorite/cs/src/core/VarLen/IVariableLengthInput.cs`

- Remove all 3 `GetUpsertFieldInfo` overloads
- Add single `GetUpsertFieldInfo<TKey>(TKey key, ref TInput input)` — analogous to `GetRMWInitialFieldInfo`

### TODO 3: Update ISessionFunctionsWrapper interface
**File:** `libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionFunctionsWrapper.cs`

Mirror ISessionFunctions: collapse all Upsert-related methods to single versions.

### TODO 4: Update SessionFunctionsWrapper implementation
**File:** `libs/storage/Tsavorite/cs/src/core/ClientSession/SessionFunctionsWrapper.cs`

Remove all span/object/logrecord delegation methods. Add single Input-only versions that delegate to updated `ISessionFunctions`. Update `GetUpsertFieldInfo` forwarding.

### TODO 5: Update SessionFunctionsBase default implementations
**File:** `libs/storage/Tsavorite/cs/src/core/Index/Interfaces/SessionFunctionsBase.cs`

- Remove all span/object/logrecord Upsert defaults
- Add single `InitialWriter(ref LogRecord, in RecordSizeInfo, ref TInput, ref TOutput, ref UpsertInfo)` — **throws NotImplementedException** by default, forcing derived classes to opt in (the base class cannot know how to extract a value from generic TInput)
- Add single `InPlaceWriter(ref LogRecord, ref TInput, ref TOutput, ref UpsertInfo)` — **throws NotImplementedException**
- Single `PostInitialWriter`, `PostUpsertOperation` — no-op defaults (safe to not override)
- Single `GetUpsertFieldInfo(key, ref input)` — **throws NotImplementedException** (requires knowledge of TInput)
- **Rationale:** Default implementations must not silently return `true` without writing a value — this would cause silent data corruption. Throwing forces implementers to provide correct value-writing logic.

### TODO 6: Update NoOpSessionFunctions
**File:** `libs/storage/Tsavorite/cs/src/core/ClientSession/NoOpSessionFunctions.cs`

- Remove all span/object/logrecord Upsert overloads
- Add single Input-only stubs (throw `NotImplementedException` — NoOp is not used for Upsert after the change)
- Add single `GetUpsertFieldInfo(key, ref input)` that throws
- Keep Delete implementations as-is

### TODO 7: Create LogRecordInput type (generic, no boxing)
**New file:** `libs/storage/Tsavorite/cs/src/core/ClientSession/LogRecordInput.cs`

```csharp
/// <summary>
/// Wraps a source log record as an Input for copy operations (compaction, iteration).
/// Generic over TSourceLogRecord to avoid boxing — all ISourceLogRecord implementations
/// are value types (LogRecord, DiskLogRecord) except ITsavoriteScanIterator which is
/// implemented by classes.
/// </summary>
internal struct LogRecordInput<TSourceLogRecord> where TSourceLogRecord : ISourceLogRecord
{
    public TSourceLogRecord SourceRecord;
}
```

### TODO 8: Create LogRecordCopySessionFunctions (generic)
**New file:** `libs/storage/Tsavorite/cs/src/core/ClientSession/LogRecordCopySessionFunctions.cs`

```csharp
internal struct LogRecordCopySessionFunctions<TSourceLogRecord>
    : ISessionFunctions<LogRecordInput<TSourceLogRecord>, Empty, Empty>
    where TSourceLogRecord : ISourceLogRecord
```

- `InitialWriter`: `dstLogRecord.TryCopyFrom(in input.SourceRecord, in sizeInfo)` — zero boxing via generic `in TSourceLogRecord`
- `InPlaceWriter`: compute sizeInfo from source, then `TryCopyFrom`
- `GetUpsertFieldInfo`: derive size from `input.SourceRecord` — must handle:
  - Span value vs object value (`Info.ValueIsObject`)
  - Inline vs overflow value sizing
  - `ObjectIdMap.ObjectIdSize` for object values
  - ETag and expiration preservation (via `TryCopyFrom`)
- Delete methods: return true (simple pass-through)
- All other methods: no-op or throw
- **Lifetime constraint:** `LogRecordInput<T>` is valid only for the synchronous duration of the Upsert call. Callbacks must not store it.

### TODO 9: Delete UpsertValueSelector infrastructure
**Delete file:** `libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/UpsertValueSelector.cs`

Remove `IUpsertValueSelector`, `SpanUpsertValueSelector`, `ObjectUpsertValueSelector`, `LogRecordUpsertValueSelector` entirely.

### TODO 10: Update InternalUpsert
**File:** `libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs`

Major simplification:
- Remove `TValueSelector` generic parameter
- Remove `srcStringValue`, `srcObjectValue`, `inputLogRecord` parameters
- Signature becomes: `InternalUpsert<TKey, TInput, TOutput, TContext, TSessionFunctionsWrapper>(key, keyHash, ref input, ref output, ...)`
- Call `sessionFunctions.InitialWriter(ref logRecord, in sizeInfo, ref input, ref output, ref upsertInfo)` directly (no selector dispatch)
- Same for `InPlaceWriter`, `PostInitialWriter`, `PostUpsertOperation`
- Update `TryRevivifyInChain` and `CreateNewRecordUpsert` signatures to match

### TODO 11: Update ContextUpsert in TsavoriteKV
**File:** `libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs`

- Remove all 3 `ContextUpsert` overloads (span, object, logrecord)
- Add single `ContextUpsert(key, keyHash, ref input, ref output, out recordMetadata, context, sessionFunctions)` that calls simplified `InternalUpsert`

### TODO 12: Update Allocator GetUpsertRecordSize methods
**Files:**
- `libs/storage/Tsavorite/cs/src/core/Allocator/IAllocator.cs`
- `SpanByteAllocator.cs` / `SpanByteAllocatorImpl.cs`
- `ObjectAllocator.cs` / `ObjectAllocatorImpl.cs`
- `TsavoriteLogAllocator.cs`

- Remove all 3 `GetUpsertRecordSize` overloads (span, object, logrecord)
- Add single `GetUpsertRecordSize<TKey, TInput, TVariableLengthInput>(TKey key, ref TInput input, TVariableLengthInput varlenInput)` — calls `varlenInput.GetUpsertFieldInfo(key, ref input)`

### TODO 13: Update SpanByteFunctions
**File:** `libs/storage/Tsavorite/cs/src/core/VarLen/SpanByteFunctions.cs`

- Remove span/object `GetUpsertFieldInfo` overloads
- Add `GetUpsertFieldInfo(key, ref PinnedSpanByte input)` — derives value size from input

### TODO 14: Update ITsavoriteContext interface
**File:** `libs/storage/Tsavorite/cs/src/core/ClientSession/ITsavoriteContext.cs`

- Remove ALL span/object/logrecord Upsert method signatures
- Add Input-only Upsert signatures mirroring RMW pattern:
  - `Upsert(TKey key, ref TInput input, ref TOutput output, TContext userContext = default)`
  - `Upsert(TKey key, ref TInput input, ref TOutput output, ref UpsertOptions, TContext userContext = default)`
  - `Upsert(TKey key, ref TInput input, ref TOutput output, ref UpsertOptions, out RecordMetadata, TContext userContext = default)`
  - `Upsert(TKey key, ref TInput input, TContext userContext = default)` (convenience)
  - `Upsert(TKey key, ref TInput input, ref UpsertOptions, TContext userContext = default)` (convenience with options)

### TODO 15: Update all context implementations
**Files (6 context types, all must mirror ITsavoriteContext):**
- `BasicContext.cs` — primary implementation, delegates to `store.ContextUpsert`
- `UnsafeContext.cs` — delegates to BasicContext pattern without epoch resume/suspend
- `ConsistentReadContext.cs` — delegates to `BasicContext.Upsert`
- `TransactionalContext.cs` — delegates with transactional locking
- `TransactionalUnsafeContext.cs` — transactional + unsafe pattern
- `TransactionalConsistentReadContext.cs` — transactional + consistent read

For each: remove all span/object/logrecord Upsert overloads, add Input-only overloads matching ITsavoriteContext.

### TODO 16: Update Compaction (CompactScan)
**File:** `libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs`

- `CompactScan`: Change session from `NoOpSessionFunctions<TInput, TOutput, TContext>` to `LogRecordCopySessionFunctions<ITsavoriteScanIterator>`
- TInput becomes `LogRecordInput<ITsavoriteScanIterator>` — scan iterators are classes, zero boxing
- Change `Upsert(in iterLogRecord)` to `Upsert(iter1, ref logRecordInput)` where `logRecordInput.SourceRecord = iter1`
- `CompactScan` method signature can drop `TInput, TOutput, TContext` type params (uses concrete types)
- `CompactLookup` keeps `NoOpSessionFunctions` (it uses `CompactionCopyToTail`, not Upsert)

### TODO 17: Update TsavoriteKVIterator
**File:** `libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/TsavoriteIterator.cs`

- Change temp KV session to `NewSession<ITsavoriteScanIterator, LogRecordInput<ITsavoriteScanIterator>, Empty, Empty, LogRecordCopySessionFunctions<ITsavoriteScanIterator>>(new())`
- Simplify class type parameters: remove `TInput, TOutput, TContext, TFunctions` (no longer needed)
- Change `tempbContext.Upsert(in iterLogRecord)` to `tempbContext.Upsert(key, ref logRecordInput)`
- Update `Iterate` public API on `TsavoriteKV` to match simplified iterator
- **Note:** This is a semantic change — caller-provided functions no longer affect temp-KV. Verify with tests.

### TODO 18: Update Tsavorite test session functions
**Files:** Multiple test files in `libs/storage/Tsavorite/cs/test/`

**Migration strategy** (do this BEFORE updating call sites):
1. Inventory each test `ISessionFunctions` implementation
2. Define the new Input shape (add value field to TInput struct if needed)
3. Update `InitialWriter`, `InPlaceWriter`, `PostInitialWriter`, `PostUpsertOperation`, and `GetUpsertFieldInfo` together
4. Ensure each implementation correctly writes the value from Input to the log record

Key test session function implementations to update:
- `UpsertInputFunctions` (InputOutputParameterTests.cs)
- `ExpirationFunctions` (ExpirationTests.cs)  
- `SimpleLongSimpleFunctions` and related (TestUtils.cs)
- `TestInlineObjectFunctions` (ObjectInlineTests.cs)

**Important:** `PostUpsertOperation` implementations that currently access the value (span or object) must be migrated to read the same data from TInput instead.

### TODO 19: Update Tsavorite test Upsert call sites
**Files:** ~39 test files with 177+ Upsert invocations

All call sites change:
- `context.Upsert(key, spanValue)` → `context.Upsert(key, ref input)` where input carries the value
- `context.Upsert(key, ref input, spanValue, ref output)` → `context.Upsert(key, ref input, ref output)`
- Test TInput types may need to carry value data (e.g., add a value field to the test input struct)

### TODO 20: Build and run Tsavorite tests
```bash
dotnet build libs/storage/Tsavorite/cs/test/Tsavorite.test.csproj
dotnet test libs/storage/Tsavorite/cs/test/Tsavorite.test.csproj -f net10.0 -c Debug
```

## Risk Areas

1. **RecordSizeInfo computation**: The allocator's `GetUpsertRecordSize` currently uses the value to compute record size. After the change, `GetUpsertFieldInfo(key, ref input)` must return correct sizes from Input alone. All session functions (including test ones) must implement this correctly.

2. **InPlaceWriter sizeInfo**: Currently uses the value to determine if in-place update is feasible. After the change, must derive from Input. The `SessionFunctionsBase` default currently calls `GetUpsertFieldInfo(key, value, ref input)` — this must change to `GetUpsertFieldInfo(key, ref input)`.

3. **Test Input types**: Many tests use simple types (`int`, `long`, `Empty`) as TInput that don't carry value data. These need restructuring to carry the value within Input.

4. **Iterator public API change**: `TsavoriteKV.Iterate<TInput, TOutput, TContext, TFunctions>` loses its type parameters. Garnet callers will break (expected, fixed in follow-up PR).

5. **No boxing in LogRecordInput**: All `ISourceLogRecord` implementations are structs (`LogRecord`, `DiskLogRecord`). The generic `LogRecordInput<TSourceLogRecord>` avoids boxing by storing them by value. `ITsavoriteScanIterator` (also `ISourceLogRecord`) is implemented by classes, so no boxing there either.

6. **Performance**: Removing ValueSelector dispatch and collapsing overloads reduces generic specialization count and call indirection. Should be perf-neutral or slightly positive.

7. **PostUpsertOperation migration**: Existing implementations that access the value parameter (span or object) for ownership, cleanup, accounting, or conditional side effects must be migrated to read from TInput instead.

8. **Iterator semantic change**: Switching the iterator's temp KV from caller-provided TFunctions to LogRecordCopySessionFunctions means caller functions no longer affect temp-KV record copies. This is intentional (temp KV is an internal dedup mechanism), but should be verified with tests for object values, expiration/ETag, tombstones, and variable-length values.

9. **Retry safety**: Upsert retries (CAS failure, CPR shift, revivification) re-invoke callbacks with the same Input. Callbacks should not mutate TInput. This matches existing behavior where the value parameter was also re-used on retries.

10. **Scope**: Only Tsavorite project/tests must compile in this PR. Garnet integration (session functions, Upsert callers in `libs/server/`) will be fixed in a follow-up PR.

## Post-Implementation Validation

After all changes, run these searches to verify no old patterns remain:

```bash
# No remaining ValueSelector infrastructure
rg "IUpsertValueSelector|SpanUpsertValueSelector|ObjectUpsertValueSelector|LogRecordUpsertValueSelector"

# No remaining value-taking Upsert callbacks
rg "InitialWriter.*ReadOnlySpan<byte>" --glob "*.cs"
rg "InitialWriter.*IHeapObject" --glob "*.cs"
rg "InPlaceWriter.*ReadOnlySpan<byte>" --glob "*.cs"
rg "InPlaceWriter.*IHeapObject" --glob "*.cs"

# No remaining value-taking GetUpsertFieldInfo
rg "GetUpsertFieldInfo.*ReadOnlySpan<byte>" --glob "*.cs"
rg "GetUpsertFieldInfo.*IHeapObject" --glob "*.cs"

# No remaining value-taking ContextUpsert
rg "ContextUpsert.*srcStringValue|ContextUpsert.*srcObjectValue" --glob "*.cs"

# No remaining old-style Upsert(in logRecord) calls
rg "\.Upsert\(in " --glob "*.cs"

# All Tsavorite tests pass
dotnet build libs/storage/Tsavorite/cs/test/Tsavorite.test.csproj
dotnet test libs/storage/Tsavorite/cs/test/Tsavorite.test.csproj -f net10.0 -c Debug
```

## Implementation Order

The changes are tightly coupled. Recommended order:

1. TODOs 1-3 (interfaces) — define the new contract: ISessionFunctions, IVariableLengthInput, ISessionFunctionsWrapper
2. TODOs 4-6, 13 (existing session function implementations) — SessionFunctionsWrapper, SessionFunctionsBase, NoOpSessionFunctions, SpanByteFunctions
3. TODOs 7-8 (new types) — LogRecordInput, LogRecordCopySessionFunctions
4. TODOs 9-12 (core infrastructure) — delete UpsertValueSelector, simplify InternalUpsert, ContextUpsert, allocators
5. TODOs 14-15 (public API) — ITsavoriteContext + all 6 context implementations
6. TODOs 16-17 (internal callers) — compaction, iterator
7. TODOs 18-19 (tests) — migrate test session functions first, then call sites
8. TODO 20 (validation) — build, test, and run validation greps
