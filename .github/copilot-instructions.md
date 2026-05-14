# Garnet - Copilot Instructions

Garnet is a high-performance remote cache-store from Microsoft Research implementing the Redis RESP wire protocol in C#/.NET. It uses Tsavorite as its storage engine. Full developer docs: https://microsoft.github.io/garnet/docs/dev/onboarding

> **Note**: Some website docs may reference the older two-store architecture (separate main store and object store). This branch uses a **unified single-store** design — see the Architecture section below for the current model.

## Build, Test, and Lint

```bash
# Build the entire solution
dotnet build

# Run all Garnet tests
dotnet test test/Garnet.test -f net10.0 -c Debug -l "console;verbosity=detailed"

# Run all cluster tests
dotnet test test/Garnet.test.cluster -f net10.0 -c Debug -l "console;verbosity=detailed"

# Run a single test by fully qualified name
dotnet test test/Garnet.test -f net10.0 -c Debug --filter "FullyQualifiedName~RespTests.PingTest"

# Run all tests in a single test class
dotnet test test/Garnet.test -f net10.0 -c Debug --filter "FullyQualifiedName~RespTests"

# Build and test Tsavorite independently (has its own solution)
dotnet build libs/storage/Tsavorite/cs/test/Tsavorite.test.csproj
dotnet test libs/storage/Tsavorite/cs/test/Tsavorite.test.csproj -f net10.0 -c Debug -l "console;verbosity=detailed"

# Check formatting (CI enforces this)
dotnet format Garnet.slnx --verify-no-changes
dotnet format libs/storage/Tsavorite/cs/Tsavorite.slnx --verify-no-changes

# Run the server locally (from repo root)
cd main/GarnetServer && dotnet run -c Debug -f net10.0 -- --logger-level Trace -m 4g -i 64m
```

Target frameworks are `net8.0` and `net10.0`. CI runs tests on both, in Debug and Release, on Ubuntu and Windows.

## Architecture

### Unified Single-Store Design

Garnet uses a **single Tsavorite key-value store** instance (`TsavoriteKV<StoreFunctions, StoreAllocator>`) that holds both raw strings and complex objects. The store is accessed through three different **context types**, each with its own input/output types and session functions:

| Context | Input/Output Types | Session Functions | Used For |
|---------|-------------------|-------------------|----------|
| **String context** | `StringInput` / `StringOutput` | `MainSessionFunctions` | Raw string commands (GET, SET, APPEND, INCR, etc.) |
| **Object context** | `ObjectInput` / `ObjectOutput` | `ObjectSessionFunctions` | Collection commands (HSET, LPUSH, ZADD, SADD, etc.) |
| **Unified context** | `UnifiedInput` / `UnifiedOutput` | `UnifiedSessionFunctions` | Type-agnostic commands (EXISTS, DELETE, TYPE, TTL, EXPIRE, RENAME, etc.) |

All three contexts operate on the **same underlying store**. At the storage level, each record's `RecordInfo` has a `ValueIsObject` bit that indicates whether the value is a raw string (inline bytes) or a heap object reference, enabling the unified store to differentiate between the two value types. The `GarnetApi` struct is generic over all three context types:

```csharp
public partial struct GarnetApi<TStringContext, TObjectContext, TUnifiedContext>
```

Two concrete instantiations are used: `BasicGarnetApi` (normal operations) and `TransactionalGarnetApi` (within transactions). Type aliases for all context variants are defined in `libs/GlobalUsings.cs`.

The single store is held by `GarnetDatabase` (`libs/server/GarnetDatabase.cs`) and managed by `StoreWrapper` (`libs/server/StoreWrapper.cs`). Each record carries a `ValueIsObject` bit in its `RecordInfo` header to distinguish raw string values from serialized object values.

#### Storage layer organization

Each context type has parallel directory structures:

- **Functions** (Tsavorite callbacks for RMW, Read, Upsert, Delete):
  - `libs/server/Storage/Functions/MainStore/` — string operations
  - `libs/server/Storage/Functions/ObjectStore/` — collection operations
  - `libs/server/Storage/Functions/UnifiedStore/` — type-agnostic operations
- **Session ops** (StorageSession methods wrapping Tsavorite API):
  - `libs/server/Storage/Session/MainStore/` — string ops (MainStoreOps.cs, BitmapOps.cs, HyperLogLogOps.cs)
  - `libs/server/Storage/Session/ObjectStore/` — collection ops ([ObjectName]Ops.cs)
  - `libs/server/Storage/Session/UnifiedStore/` — unified ops (UnifiedStoreOps.cs)
- **Object implementations**: `libs/server/Objects/[ObjectName]/` — per-type logic (Hash, List, Set, SortedSet, SortedSetGeo)

### Key Layers

- **Network/Session** (`libs/common/Networking/`, `libs/server/Sessions/`) — Shared-memory network design where TLS and storage ops run on IO completion threads. `GarnetServerTcp` accepts connections, creates `ServerTcpNetworkHandler` per client. `GarnetProvider` creates `RespServerSession` instances to handle RESP messages.
- **RESP Command Processing** (`libs/server/Resp/`) — Commands are defined as `RespCommand` enum values and dispatched via switch expressions in `ProcessBasicCommands`/`ProcessArrayCommands`. The `RespServerSession` class is split across multiple partial `.cs` files organized by command category.
- **Storage API** (`libs/server/API/`) — Narrow-waist API (`IGarnetApi` inherits `IGarnetReadApi` + `IGarnetAdvancedApi`) with read, upsert, delete, and atomic read-modify-write operations. Command handlers are generic over `TGarnetApi` for testability. `StorageSession` wraps Tsavorite API calls. API methods for string, object, and unified commands are split across `GarnetApi.cs`, `GarnetApiObjectCommands.cs`, and `GarnetApiUnifiedCommands.cs`.
- **Tsavorite Engine** (`libs/storage/Tsavorite/cs/src/core/`) — Has its own solution (`Tsavorite.slnx`) and test project. Provides concurrent key-value storage with checkpointing, tiered storage, recovery, and epoch-based memory reclamation. Relies heavily on `Span<T>` and `SpanByte` for zero-copy memory management.
- **Cluster** (`libs/cluster/`) — Sharding, replication, gossip protocol, key migration. Interface defined in `libs/server/Cluster/IClusterProvider.cs`, implementation in `libs/cluster/Server/`.
- **Database Management** (`libs/server/Databases/`) — Factory pattern with `SingleDatabaseManager` and `MultiDatabaseManager` implementations behind `IDatabaseManager`. Multi-database only available when cluster mode is off. Each `RespServerSession` manages a map of `GarnetDatabaseSession` instances (one per database index).

### Type Aliases

The codebase uses `using` aliases extensively for complex generic store types. `libs/GlobalUsings.cs` defines the key aliases: `BasicGarnetApi`, `TransactionalGarnetApi`, `StringBasicContext`, `ObjectBasicContext`, `UnifiedBasicContext`, `StoreAllocator`, and their transactional variants. See also the top of `RespServerSession.cs` and `StoreWrapper.cs`.

## Adding a New RESP Command

Full guide: https://microsoft.github.io/garnet/docs/dev/garnet-api

### Steps for a new built-in command:

1. **Define the command**: Add enum value to `RespCommand` in `libs/server/Resp/Parser/RespCommand.cs`. For object commands (List, SortedSet, Hash, Set), also add a value to the `[ObjectName]Operation` enum in `libs/server/Objects/[ObjectName]/[ObjectName]Object.cs`.
2. **Add parsing logic**: In `libs/server/Resp/Parser/RespCommand.cs`, add to `FastParseCommand` (fixed arg count) or `FastParseArrayCommand` (variable args).
3. **Declare the API method**: Add method signature to `IGarnetReadApi` (read-only) or `IGarnetApi` (read-write) in `libs/server/API/IGarnetApi.cs`.
4. **Implement the network handler**: Add a method to `RespServerSession` (the class is split across ~22 partial `.cs` files — object commands go in `libs/server/Resp/Objects/[ObjectName]Commands.cs`, others in `libs/server/Resp/BasicCommands.cs`, `ArrayCommands.cs`, `AdminCommands.cs`, `KeyAdminCommands.cs`, etc.). The handler parses arguments from the network buffer via `parseState.GetArgSliceByRef(i)` (returns `ref PinnedSpanByte`), calls the storage API, and writes the RESP response using `RespWriteUtils` helper methods, then calls `SendAndReset()` to flush the response buffer.
5. **Add dispatch route**: In `libs/server/Resp/RespServerSession.cs`, add a case to `ProcessBasicCommands` or `ProcessArrayCommands` calling the handler from step 4.
6. **Implement storage logic**: Add method to `StorageSession`. Choose the appropriate context based on the command type:
   - **String commands**: Add to `libs/server/Storage/Session/MainStore/MainStoreOps.cs`. Call Tsavorite's `Read` or `RMW` via the string context. For RMW, implement init/update logic in `libs/server/Storage/Functions/MainStore/RMWMethods.cs`.
   - **Object (collection) commands**: Add to `libs/server/Storage/Session/ObjectStore/[ObjectName]Ops.cs`. Call `ReadObjectStoreOperation` or `RMWObjectStoreOperation` via the object context, then implement the case in `libs/server/Objects/[ObjectName]/[ObjectName]ObjectImpl.cs`.
   - **Type-agnostic commands** (EXISTS, DELETE, TTL, EXPIRE, TYPE, etc.): Add to `libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs`. Use the unified context. Implement callbacks in `libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs`.
7. **Transaction support**: For standard commands, define `KeySpecs` in the command's metadata — the framework automatically handles key locking via `TxnKeyManager.LockKeys()`. For custom multi-key operations, manually call `txnManager.SaveKeyEntryToLock(key, lockType)` in `libs/server/Transaction/TxnKeyManager.cs`; the key is a `PinnedSpanByte` in the unified key-space, so object-vs-string handling is managed internally by the transaction layer.
8. **Tests**: Add tests using both `StackExchange.Redis` and `LightClient` where applicable. Object command tests go in `test/Garnet.test/Resp[ObjectName]Tests.cs`, others in `test/Garnet.test/RespTests.cs` or similar.
9. **Documentation**: Update the appropriate markdown file under `website/docs/commands/` and mark the command as supported in `website/docs/commands/api-compatibility.md`.
10. **Command info metadata**: Add the command to `playground/CommandInfoUpdater/SupportedCommand.cs`, then run the updater tool:
    ```bash
    cd playground/CommandInfoUpdater
    dotnet run -- --output ../../libs/resources
    ```

> **Tip**: Write a basic test calling the new command first, then implement missing logic as you debug.

## Custom Extensions

Four extensibility points, all in C#. See https://microsoft.github.io/garnet/docs/dev/custom-commands

- **`CustomRawStringFunctions`** — Custom operations on raw strings (example: `main/GarnetServer/Extensions/DeleteIfMatch.cs`)
- **`CustomObjectBase` + `CustomObjectFactory`** — Custom data types for the object store (example: `main/GarnetServer/Extensions/MyDictObject.cs`)
- **`CustomTransactionProcedure`** — Server-side multi-key transactions (example: `main/GarnetServer/Extensions/ReadWriteTxn.cs`)
- **`CustomProcedure`** — Non-transactional server-side stored procedures (example: `main/GarnetServer/Extensions/Sum.cs`)

Register server-side via `server.Register` (a `RegisterApi` instance on `GarnetServer`) with `.NewCommand()`, `.NewTransactionProc()`, `.NewProcedure()`, or `.NewType()`. Client-side registration uses the `REGISTERCS` admin command with assemblies on the server.

## Key Conventions

### File Headers

All C# files require this header (enforced by .editorconfig `file_header_template`):

```csharp
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
```

This same header is used throughout the entire codebase, including Tsavorite files.

### Test Structure

- **Framework**: NUnit with `[TestFixture]`, `[Test]`, `[SetUp]`, `[TearDown]`
- **Test base class**: All test fixtures must inherit from `TestBase` (defined in `test/standalone/Garnet.test/TestBase.cs`). This tracks currently running tests for diagnostics.
- **Server lifecycle**: Create in `[SetUp]` via `TestUtils.CreateGarnetServer(TestUtils.MethodTestDir)`, call `.Start()`, then `.Dispose()` in `[TearDown]`. Common optional parameters include `enableAOF`, `lowMemory`, `enableTLS`, `enableCluster`, `tryRecover`, `disableObjects`, `useAcl`, and `defaultPassword`.
- **Teardown**: Always call `TestUtils.OnTearDown()` (checks for leaked `LightEpoch` instances)
- **Test directory cleanup**: `TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true)` at the start of `[SetUp]`
- **Namespace**: Test files use `Garnet.test` namespace (even files in subdirectories like `DiskANN/`)
- **Clients in tests**: Use `StackExchange.Redis` for high-level operations, `LightClient` for raw RESP protocol testing

```csharp
[TestFixture]
public class MyTests : TestBase
{
    GarnetServer server;

    [SetUp]
    public void Setup()
    {
        TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
        server.Start();
    }

    [TearDown]
    public void TearDown()
    {
        server.Dispose();
        TestUtils.OnTearDown();
    }

    [Test]
    public void MyTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        var db = redis.GetDatabase(0);
        // ... test using StackExchange.Redis
    }
}
```

### Adding Configuration Settings

To add a new Garnet server setting:
1. Add property to `Options` class in `libs/host/Configuration/Options.cs` with `[Option]` attribute
2. Add default value in `libs/host/defaults.conf`
3. If needed in core code, add matching property to `GarnetServerOptions` (`libs/server/Servers/GarnetServerOptions.cs`) and map it in `Options.GetServerOptions()`
4. Add tests in `test/Garnet.test/GarnetServerConfigTests.cs`

### Coding Style

- 4-space indentation, Allman braces (opening brace on new line)
- `var` preferred when type is apparent
- `unsafe` and `AllowUnsafeBlocks` enabled globally
- Private/internal fields: camelCase; constants and statics: PascalCase
- `TreatWarningsAsErrors` is enabled — all warnings must be resolved
- Central package version management via `Directory.Packages.props`
- XML doc comments (`/// <summary>`) are strongly recommended on public methods, with `<param>` tags for each parameter; analyzer rules for missing docs are currently configured as suggestions (see `.editorconfig`)
- Comment format: `// Comment starting with a capital letter` (one space after `//`)

### Performance Conventions

- Use `Span<T>` and `SpanByte` extensively for zero-copy memory management — avoid allocations on hot paths
- Use `[MethodImpl(MethodImplOptions.AggressiveInlining)]` for hot-path methods
- Use `[MethodImpl(MethodImplOptions.NoInlining)]` for cold-path and exception-throwing methods
- Tsavorite uses `LightEpoch` for epoch-based safe memory reclamation — acquire epoch protection before store operations, release after
- `LightEpoch` instances track ownership — only dispose if owned
- In parallel tests, share a `LightEpoch` instance across `GarnetClient` instances

### Epoch Protection and Log Address Invariants

Tsavorite uses **epoch-based memory reclamation** (`LightEpoch`) so writers can publish new values and reclaim old memory only after every reader has moved past it. Any change to the allocator, recovery, scan iterators, transient locking, or callbacks fired from the drain list must respect the rules below.

**Key files**: `libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs`, `libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs`, `libs/storage/Tsavorite/cs/src/core/ClientSession/ClientSession.cs`.

#### Epoch protection model (`LightEpoch`)

- Each thread acquires a per-instance entry via `epoch.Resume()` (calls `Acquire`) and releases it via `epoch.Suspend()` (calls `Release`). Inside the protected region, the thread's `localCurrentEpoch` is advanced on every `ProtectAndDrain()` call and on entry.
- `Resume()` is **non-reentrant** — `Acquire` asserts if the thread is already protected on this instance. Use `ResumeIfNotProtected()` (returns `true` if it acquired) when code may be entered under an existing hold; pair with a matching `Suspend()` only on the path that took it.
- `BasicContext.{RMW, Upsert, Read, Delete}` wrap the call in `UnsafeResumeThread()` / `UnsafeSuspendThread()` (in `ClientSession`) via try/finally. Custom code that calls `epoch.ProtectAndDrain()` (e.g., spin-waiters in `EpochOperations.SpinWaitUntilClosed`/`SpinWaitUntilRecordIsClosed`, `TransientLocking.LockForScan`) **must already hold the epoch** — the `Debug.Assert(entry > 0, "Trying to refresh unacquired epoch")` in `LightEpoch.ProtectAndDrain` fires otherwise.
- `BumpCurrentEpoch(Action)` increments the global epoch and queues `Action` against the *prior* epoch. The action fires on whatever thread next observes that epoch as safe-to-reclaim — typically inside `ProtectAndDrain` → `Drain`. Therefore actions must be **thread-agnostic** (no thread-affine state) and **safe to fire synchronously** from the bumping thread itself: `BumpCurrentEpoch(Action)` calls `ProtectAndDrain` internally and may execute the action it just queued.

#### Log address layout and invariants

The seven log addresses on `AllocatorBase` advance monotonically and obey:

```
BeginAddress  <=  ClosedUntilAddress  <=  SafeHeadAddress  <=  HeadAddress
              <=  FlushedUntilAddress
              <=  SafeReadOnlyAddress <=  ReadOnlyAddress  <=  TailAddress
```

| Address | Meaning |
|---------|---------|
| `BeginAddress` | Lowest valid address. Advancing it logically retires older addresses but **does not delete on-disk files** by itself; physical truncation only happens when a `ShiftBeginAddress` caller passes `truncateLog: true` (typically a checkpoint commit), and even then the device may defer file removal. |
| `ClosedUntilAddress` | Highest address whose page buffer has been freed (`pagePointers[idx] = 0`). |
| `SafeHeadAddress` | High-water set by `OnPagesClosed` *before* freeing — readers see it lead `ClosedUntilAddress`. |
| `HeadAddress` | Lowest in-memory address. May advance while you hold the epoch, **but any address that was `>= HeadAddress` at any point during your protected region cannot be evicted until you `Suspend`**. Capped at `FlushedUntilAddress` — eviction never gets ahead of disk durability. |
| `FlushedUntilAddress` | All bytes below have been written to disk. Updated by flush completion callbacks invoked from `AsyncFlushPagesForReadOnly`. Lags `SafeReadOnlyAddress` (a page is only flushed once it has become safely read-only). |
| `SafeReadOnlyAddress` | Below this, no writer can be in-place mutating. Set by `OnPagesMarkedReadOnly` after writers have drained; same call also kicks off the flush that will later advance `FlushedUntilAddress`. |
| `ReadOnlyAddress` | Maximum address of the immutable region. Records below are flushed/in-flush. |
| `TailAddress` | Next address to allocate; published via the `PageOffset` CAS in `HandlePageOverflow`. |

#### Cascade pattern: publish → epoch barrier → post-drain action

Address advancement uses a **publish → bump → action** cascade so that the post-barrier work runs only after every prior holder has observed the new value:

1. **Publish** the new address into the visible field via `MonotonicUpdate`.
2. **`BumpCurrentEpoch(Action)`** queues the post-barrier work against the prior epoch; it fires once every thread that observed the old value has either `Suspend`ed or `ProtectAndDrain`ed.
3. The **action** does the work that requires "all prior holders have moved past": flush pages, advance the `Safe*` companion, close pages, free buffers, truncate disk segments.

The two cascades you encounter on the runtime hot path:

- **Read-only / flush cycle** — `ShiftReadOnlyAddress(newRO)` publishes `ReadOnlyAddress`, then `BumpCurrentEpoch(OnPagesMarkedReadOnly)`. The action advances `SafeReadOnlyAddress` and issues `AsyncFlushPagesForReadOnly`; flush completion later advances `FlushedUntilAddress` via `FlushCallback`. Triggered by `PageAlignedShiftReadOnlyAddress` whenever the tail moves far enough past the read-only region.
- **Eviction / close cycle** — `ShiftHeadAddress(desiredHA)` publishes `HeadAddress`, then `BumpCurrentEpoch(OnPagesClosed)`. The action advances `SafeHeadAddress` and `ClosedUntilAddress`, and frees page buffers via the per-allocator `FreePage` (defined in `SpanByteAllocatorImpl` / `ObjectAllocatorImpl`). Triggered when `FlushedUntilAddress` moves past `HeadAddress + (some delta)`, or explicitly via `ShiftHeadAddressToBlocking`.

Other cascades:

- **`ShiftBeginAddress(newBA, truncateLog)`** — publishes `BeginAddress` (and cascades through `ShiftReadOnlyAddress` + `ShiftHeadAddress` if needed). When `truncateLog: true`, also bumps with `TruncateUntilAddress` to drop on-disk segments below the new begin; when `false` (the common case) on-disk segments are left in place to be reclaimed at the next checkpoint commit. Disk file removal itself is asynchronous — even after `TruncateUntilAddress` returns, the device may defer the actual unlink.
- **`ShiftReadOnlyAddressWithWait(newRO, wait)`** — convenience wrapper that uses `ResumeIfNotProtected`/`Suspend` to launch the shift and (optionally) blocks the caller on `FlushedUntilAddress < newRO`.

#### Rules when changing allocator/iterator/callback code

1. **Holding the epoch implies stability**: an address observed `>= HeadAddress` during the protected region cannot be evicted before `Suspend()`. Re-acquire after suspend and re-validate.
2. **`Suspend` and `Resume` must be balanced** on every code path. The only suspend inside the basic op path is the `ALLOCATE_FAILED` retry in `HandleRetryStatus`, balanced via try/finally.
3. **Drain-list actions run on arbitrary threads** that hold the epoch. Do not capture thread-static state; do not call code that asserts on a specific thread.
4. **Multi-phase mutations** that need to advance several addresses with barriers between them should use one `BumpCurrentEpoch(Action)` per phase with a `ManualResetEventSlim` to wait. **Drop the prior epoch before waiting** on the MRE — otherwise the drain list cannot make progress (the action you queued cannot fire while you hold the epoch it is gating on). Re-acquire to issue the next bump. `AllocatorBase.Reset` is an example: phase 1 publishes `ReadOnlyAddress` and waits for writers to drain before advancing `SafeReadOnlyAddress`/`FlushedUntilAddress`; phase 2 publishes `HeadAddress` and waits for readers to drain before closing/freeing pages.
5. **Address publication ordering**: when one operation advances multiple addresses, advance the more permissive ones (`HeadAddress`, `ReadOnlyAddress`) before the more restrictive ones (`BeginAddress`). The full invariant `BeginAddress <= ClosedUntilAddress <= SafeHeadAddress <= HeadAddress <= FlushedUntilAddress <= SafeReadOnlyAddress <= ReadOnlyAddress <= TailAddress` must hold throughout, and stale readers caching the older value will route through safer paths (e.g., disk-frame branch in `LoadPageIfNeeded` rather than dereferencing freed `pagePointers`). `AllocatorBase.Reset` publishes `BeginAddress` last for this reason — an iterator with a stale `nextAddress` then routes through the disk-frame path instead of the in-memory page that has just been freed.
6. **Page pointers**: after `OnPagesClosed` → `FreePage`, `pagePointers[idx] = 0`. Iterators must not dereference a page pointer outside the epoch protection that observed `addr >= HeadAddress`.
7. **Scan iterators and `BufferAndLoad`**: `ScanIteratorBase.BufferAndLoad` may internally call `BumpCurrentEpoch`, `ProtectAndDrain`, or `Suspend`+`Resume` on IO, any of which advances the iterator thread's `localCurrentEpoch` and may synchronously fire deferred drain-list actions. Reads stay safe because the IO frame is iterator-owned (allocated in the iterator's constructor) and `headAddress` advances monotonically — `LoadPageIfNeeded` only routes a record to the in-log path when it was `>= HeadAddress` at the time of sampling, so the snapshot's routing decision is always conservative.

#### Tests that exercise these paths

- `BasicLockTests.FunctionsLockTest` (in `libs/storage/Tsavorite/cs/test/BasicLockTests.cs`) — multi-threaded RMW/Upsert under contention; exercises Resume/Suspend balance and `ProtectAndDrain`.
- Cluster checkpoint/flush tests under `test/Garnet.test.cluster/` — exercise the full address cascade with live clients.

### Scratch Buffer Conventions

`StorageSession` has two scratch buffer types — use the right one:

- **`ScratchBufferBuilder` (SBB)** — Single contiguous buffer for temporary workspace. All data is laid out sequentially in one buffer. On expansion, the previous data is copied into a new larger buffer and the old buffer is freed — **any existing pointers into the old buffer become invalid**. Use for building command inputs, Lua serialization, or any data that is consumed immediately and then rewound. **Do not** return `PinnedSpanByte` from SBB to callers — it may be invalidated by subsequent allocations. Always rewind after use. Debug builds enforce single-outstanding-slice discipline via asserts.
  - Key APIs: `CreateArgSlice` (returns `PinnedSpanByte`, must rewind), `CreateArgSliceAsOffset` (returns `(Offset, Length)`, safe for multi-alloc since offsets survive reallocation), `ViewRemainingArgSlice`/`ViewFullArgSlice` (immediate-use views, do not store), `MoveOffset`, `Reset`, `RewindScratchBuffer`.

- **`ScratchBufferAllocator` (SBA)** — Maintains a collection of fragmented pinned buffers (via `GC.AllocateArray(_, true)`). When the current buffer fills, a new one is allocated and the old buffer is kept rooted in a stack — so previously returned `PinnedSpanByte` values remain valid. Use for `PinnedSpanByte` values returned via `out` parameters or `IGarnetApi` that callers retain across multiple API calls. Reset between batches.
  - Key APIs: `CreateArgSlice`, `ViewRemainingArgSlice`, `Reset`.

**Rules:**
1. Any `StorageSession` or `IGarnetApi` method returning `PinnedSpanByte` via `out` must use SBA, not SBB.
2. When using SBB's `CreateArgSlice`, always `RewindScratchBuffer` after use — debug asserts enforce at most one outstanding slice.
3. For multiple allocations without rewind, use `CreateArgSliceAsOffset` (returns offsets that survive reallocation).
4. When copying from `IMemoryOwner<byte>` (e.g., `ObjectOutput.SpanByteAndMemory.Memory`), always `Dispose()` after copying — do not leak pooled buffers.
5. `ViewFullArgSlice` and `ViewRemainingArgSlice` return immediate-use views — do not store or return them.

### Releasing a New Version

To update the version and make a new release, increment the `VersionPrefix` in `Version.props` at the repo root and submit a PR with that change.

### PR Protocol

1. Create a GitHub Issue (Enhancement / Bug / Task)
2. Branch naming: `<username>/branch-name`
3. Include unit tests (see Test Structure above)
4. Link PR to the issue in the development section
