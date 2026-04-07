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
- **Allure required**: All test fixtures must inherit from `AllureTestBase` and have the `[AllureNUnit]` attribute. CI enforces this via assembly reflection checks — builds will fail if any test fixture is missing either.
- **Server lifecycle**: Create in `[SetUp]` via `TestUtils.CreateGarnetServer(TestUtils.MethodTestDir)`, call `.Start()`, then `.Dispose()` in `[TearDown]`. Common optional parameters include `enableAOF`, `lowMemory`, `enableTLS`, `enableCluster`, `tryRecover`, `disableObjects`, `useAcl`, and `defaultPassword`.
- **Teardown**: Always call `TestUtils.OnTearDown()` (checks for leaked `LightEpoch` instances)
- **Test directory cleanup**: `TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true)` at the start of `[SetUp]`
- **Namespace**: Test files use `Garnet.test` namespace (even files in subdirectories like `DiskANN/`)
- **Clients in tests**: Use `StackExchange.Redis` for high-level operations, `LightClient` for raw RESP protocol testing

```csharp
[TestFixture]
[AllureNUnit]
public class MyTests : AllureTestBase
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
3. Include unit tests with Allure wiring (see Test Structure above)
4. Link PR to the issue in the development section
