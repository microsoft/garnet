---
name: add-garnet-command
description: Adds a new built-in RESP command to Garnet end-to-end. Covers enum registration, parsing, dispatch, RESP handler, API surface, storage session, RMW callbacks, command metadata JSON, ACL tests, and integration tests. Use when asked to "add a command", "implement RI.SET", "add RESP command", or any new server command. Do NOT use for custom extension commands (CustomRawStringFunctions) or object-type sub-operations.
---

# Add a New Built-in RESP Command to Garnet

Step-by-step guide for implementing a new built-in RESP command in Garnet. This covers every file that must be created or modified, the tools that must be run, caveats discovered during implementation, and how to verify correctness.

**Scope:** Built-in commands that are part of the Garnet server (not custom extensions registered via `REGISTERCS`).

---

## Overview: What Must Change

Adding a single new command touches **at minimum** these areas:

| # | Area | Files | Required? |
|---|------|-------|-----------|
| 1 | RespCommand enum | `libs/server/Resp/Parser/RespCommand.cs` | ✅ Always |
| 2 | Command parsing | `libs/server/Resp/Parser/RespCommand.cs` | ✅ Always |
| 3 | Command dispatch | `libs/server/Resp/RespServerSession.cs` | ✅ Always |
| 4 | RESP handler | `libs/server/Resp/<Category>/*.cs` | ✅ Always |
| 5 | API interface | `libs/server/API/IGarnetApi.cs` | ✅ Always |
| 6 | API delegation | `libs/server/API/GarnetApi.cs` | ✅ Always |
| 7 | Storage session | `libs/server/Storage/Session/` | ✅ Always |
| 8 | RMW/Read callbacks | `libs/server/Storage/Functions/MainStore/` | If using RMW/Read |
| 9 | VarLen methods | `libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs` | If using RMW |
| 10 | Command info JSON | `libs/resources/RespCommandsInfo.json` | ✅ Always (generated) |
| 11 | Command docs JSON | `libs/resources/RespCommandsDocs.json` | ✅ Always (generated) |
| 12 | Supported commands | `playground/CommandInfoUpdater/SupportedCommand.cs` | ✅ Always |
| 13 | Garnet command info | `playground/CommandInfoUpdater/GarnetCommandsInfo.json` | If Garnet-only command |
| 14 | Garnet command docs | `playground/CommandInfoUpdater/GarnetCommandsDocs.json` | If Garnet-only command |
| 15 | ACL test | `test/Garnet.test/Resp/ACL/RespCommandTests.cs` | ✅ Always |
| 16 | Integration tests | `test/Garnet.test/Resp*.cs` | ✅ Always |
| 17 | Website documentation | `website/docs/commands/` | ✅ Always |

---

## Step 1: Add the RespCommand Enum Value

**File:** `libs/server/Resp/Parser/RespCommand.cs`

The `RespCommand` enum is divided into sections with **ordering that matters**:

```
Read commands:     BITCOUNT ... ZSCORE    (before APPEND)
Write commands:    APPEND ... BITOP_DIFF  (after APPEND)
Script commands:   EVAL, EVALSHA
Non-key commands:  PING, SUBSCRIBE, etc.
Admin commands:    AUTH, CONFIG, etc.
```

**Read/write classification uses enum ordering:**
- `cmd < RespCommand.APPEND` → read-only
- `cmd >= RespCommand.APPEND && cmd <= RespCommand.BITOP_DIFF` → write

**Rules:**
- Read-only commands go **before** `APPEND`
- Write commands go **between** `APPEND` and `BITOP_DIFF`
- Update the boundary comments if you add before `APPEND` or after `BITOP_DIFF`
- Place alphabetically within the appropriate section

**Boundary markers to watch (search for these comments):**
```csharp
ZSCORE, // Note: Last read command should immediately precede FirstWriteCommand
APPEND, // Note: Update FirstWriteCommand if adding new write commands before this
BITOP_DIFF, // Note: Update LastWriteCommand if adding new write commands after this
EVALSHA, // Note: Update LastDataCommand if adding new data commands after this
```

---

## Step 2: Add Command Parsing

**File:** `libs/server/Resp/Parser/RespCommand.cs`

Two parsing paths exist:

### Fast path: `FastParseCommand()` / `FastParseArrayCommand()`
For commands with short, fixed-length names (≤8 chars, no dots). Uses `ulong` pointer comparisons on `(count << 4) | length` patterns. Only add here if the command name is a simple word.

### Slow path: `SlowParseCommand()`
For longer names, dot-prefixed names (like `RI.CREATE`), or names that don't fit the fast-path pattern.

**⚠️ Convention:** Define the command name string in **`libs/server/Resp/CmdStrings.cs`** and reference it from the parser, rather than using inline `"..."u8` literals. This keeps command name strings centralized and reusable (e.g., for error messages).

```csharp
// In CmdStrings.cs:
public static ReadOnlySpan<byte> DELIFGREATER => "DELIFGREATER"u8;
```

**Pattern for slow-path commands:**
```csharp
else if (command.SequenceEqual(CmdStrings.DELIFGREATER))
{
    return RespCommand.DELIFGREATER;
}
```

**Pattern for dot-prefixed commands (e.g., `RI.CREATE`):**
```csharp
else if (command.SequenceEqual(CmdStrings.RICREATE))
{
    return RespCommand.RICREATE;
}
```

Add this before the final `return RespCommand.INVALID;` at the end of `SlowParseCommand`.

**⚠️ Caveat: Dot-prefixed commands and ACL**
If your command uses a dot (e.g., `RI.CREATE`), you must also update **`libs/server/ACL/ACLParser.cs`** so that the ACL system can map the dotted wire name to the enum name. Search for how existing dot-handling works (look for `Replace(".", "")` or similar normalization).

---

## Step 3: Add Command Dispatch

**File:** `libs/server/Resp/RespServerSession.cs`

Three dispatch methods exist, and which one you use matters for latency tracking:

| Method | For | Latency |
|--------|-----|---------|
| `ProcessBasicCommands` | Fast single/dual-arg commands | @fast only |
| `ProcessArrayCommands` | Fast multi-arg commands | @fast only |
| `ProcessOtherCommands` | Slow commands, admin commands | @slow OK |

**⚠️ WARNING:** Do NOT add `@slow`-classified commands to `ProcessBasicCommands` or `ProcessArrayCommands`. This breaks latency tracking. If in doubt, use `ProcessOtherCommands`.

**Pattern:**
```csharp
RespCommand.MYCMD => NetworkMYCMD(ref storageApi),
```

Add before the `_ => ...` fallthrough in the appropriate method.

---

## Step 4: Implement the RESP Handler

**New file or existing file** in `libs/server/Resp/`

Command handlers are methods on the `RespServerSession` partial class:

```csharp
private bool NetworkMYCMD<TGarnetApi>(ref TGarnetApi storageApi)
    where TGarnetApi : IGarnetApi
{
    // 1. Validate argument count
    if (parseState.Count != N)
        return AbortWithWrongNumberOfArguments(nameof(RespCommand.MYCMD));

    // 2. Parse arguments
    var key = parseState.GetArgSliceByRef(0);
    var value = parseState.GetArgSliceByRef(1);

    // 3. Call storage API
    var status = storageApi.MyOperation(key, value, out var result);

    // 4. Write RESP response
    if (status == GarnetStatus.OK)
    {
        while (!RespWriteUtils.WriteDirect(CmdStrings.RESP_OK, ref dcurr, dend))
            SendAndReset();
    }
    else
    {
        while (!RespWriteUtils.WriteError("ERR message"u8, ref dcurr, dend))
            SendAndReset();
    }

    return true;
}
```

**Key patterns:**
- Arguments: `parseState.GetArgSliceByRef(i)` returns `ref PinnedSpanByte`
- Response: Use `RespWriteUtils.WriteDirect`, `WriteError`, `TryWriteInt32`, `WriteBulkString`, etc.
- Always call `SendAndReset()` in the while-not-written loop
- Return `true` (command handled) or `false` (need more data)

---

## Step 5: Add API Interface Method

**File:** `libs/server/API/IGarnetApi.cs`

Add method signature to `IGarnetApi` (read-write) or `IGarnetReadApi` (read-only):

```csharp
GarnetStatus MyOperation(PinnedSpanByte key, PinnedSpanByte value, out MyResult result);
```

**File:** `libs/server/API/GarnetApi.cs`

Add delegation in the `GarnetApi` partial struct:

```csharp
public GarnetStatus MyOperation(PinnedSpanByte key, PinnedSpanByte value, out MyResult result)
    => storageSession.MyOperation(key, value, out result);
```

**⚠️ Caveat:** `GarnetApi` is a generic partial struct: `GarnetApi<TStringContext, TObjectContext, TUnifiedContext>`. Method implementations go in the appropriate partial file (`GarnetApi.cs` for string ops, `GarnetApiObjectCommands.cs` for object ops, etc.).

---

## Step 6: Implement Storage Session Layer

**File:** New or existing file in `libs/server/Storage/Session/MainStore/` (for string-context ops) or `libs/server/Storage/Session/ObjectStore/` (for object-context ops)

This layer wraps Tsavorite API calls:

```csharp
public GarnetStatus MyOperation(PinnedSpanByte key, PinnedSpanByte value, out MyResult result)
{
    // Build input
    var input = new StringInput(RespCommand.MYCMD, ref parseState);
    var output = new StringOutput();

    // Call Tsavorite
    var status = stringBasicContext.RMW((FixedSpanByteKey)key, ref input, ref output);
    if (status.IsPending)
        CompletePendingForSession(ref status, ref output, ref stringBasicContext);

    // Interpret result
    result = status.Found ? MyResult.OK : MyResult.NotFound;
    return GarnetStatus.OK;
}
```

---

## Step 7: Add RMW/Read Callbacks (if applicable)

**File:** `libs/server/Storage/Functions/MainStore/RMWMethods.cs`

If your command uses `RMW`, you must handle these callbacks:

| Callback | When | Purpose |
|----------|------|---------|
| `NeedInitialUpdate` | Key doesn't exist | Return `true` to create record |
| `InitialUpdater` | Creating new record | Write initial value |
| `NeedCopyUpdate` | Key exists, record needs copy | Return `true` to copy-update, `false` to skip |
| `InPlaceUpdater` | Key exists, update in place | Modify existing value |
| `CopyUpdater` | Key exists, copy to new record | Write updated value to new record |

Add a `case RespCommand.MYCMD:` to each relevant switch statement.

**File:** `libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs`

If your command writes a value, you must specify the value length:

| Method | Purpose |
|--------|---------|
| `GetRMWInitialFieldInfo` | Size of value for new records |
| `GetRMWModifiedFieldInfo` | Size of value for updated records |

**⚠️ Caveat — RecordType:**
If your command creates records with a custom `RecordType` (e.g., for type discrimination), you can set it in `InitialUpdater` after record initialization:

```csharp
var header = logRecord.RecordDataHeader;
header.RecordType = MyManager.MyRecordType;
```

This works because `RecordDataHeader.RecordType` has a setter that writes through a raw pointer. No Tsavorite infrastructure changes needed (despite the TODO comments in `LogRecord.cs`).

---

## Step 8: Command Metadata Registration

### 8a. Add to SupportedCommand.cs

**File:** `playground/CommandInfoUpdater/SupportedCommand.cs`

Add entry in alphabetical order:
```csharp
new("MY.CMD", RespCommand.MYCMD, StoreType.Main),
```

`StoreType` values: `Main` (string store), `Object` (object store), `All` (both), `None` (no keys).

### 8b. Add to GarnetCommandsInfo.json (Garnet-only commands)

**File:** `playground/CommandInfoUpdater/GarnetCommandsInfo.json`

Only needed for commands that don't exist in standard Redis. Add a JSON entry:

```json
{
  "Command": "MYCMD",
  "Name": "MY.CMD",
  "IsInternal": false,
  "Arity": -2,
  "Flags": "DenyOom, Write",
  "FirstKey": 1,
  "LastKey": 1,
  "Step": 1,
  "AclCategories": "Slow, Write, Garnet",
  "KeySpecifications": [
    {
      "BeginSearch": { "TypeDiscriminator": "BeginSearchIndex", "Index": 1 },
      "FindKeys": { "TypeDiscriminator": "FindKeysRange", "LastKey": 0, "KeyStep": 1, "Limit": 0 },
      "Flags": "RW, Insert"
    }
  ],
  "StoreType": "Main"
}
```

**Key fields:**
- `Arity`: Positive = exact arg count (including command name); Negative = minimum
- `Flags`: `ReadOnly`, `Write`, `DenyOom`, `Fast`, `Admin`, `NoAuth`, `Module`, etc.
- `AclCategories`: Used for ACL permission checks. Use `Garnet` for Garnet-specific commands
- `KeySpecifications`: Drives automatic transaction key locking — no per-command switch needed
- `KeySpec.Flags`: `RO` (read-only), `RW` (read-write), `Access`, `Insert`, `Update`, `Delete`

### 8c. Add to GarnetCommandsDocs.json (Garnet-only commands)

**File:** `playground/CommandInfoUpdater/GarnetCommandsDocs.json`

Add documentation entry:

```json
{
  "Command": "MYCMD",
  "Name": "MY.CMD",
  "Summary": "Description of what the command does.",
  "Group": "Generic",
  "Complexity": "O(1)",
  "Arguments": [
    {
      "TypeDiscriminator": "RespCommandKeyArgument",
      "Name": "KEY",
      "DisplayText": "key",
      "Type": "Key",
      "KeySpecIndex": 0
    }
  ]
}
```

**⚠️ Caveat — Group must be a valid `RespCommandGroup` enum value:**
`Bitmap`, `Cluster`, `Connection`, `Generic`, `Geo`, `Hash`, `HyperLogLog`, `List`, `Module`, `PubSub`, `Scripting`, `Sentinel`, `Server`, `Set`, `SortedSet`, `Stream`, `String`, `Transactions`

Do NOT invent new group names — the JSON deserializer will fail.

### 8d. Generate the resource JSON files

**⚠️ CRITICAL: Never edit `libs/resources/RespCommandsInfo.json` or `libs/resources/RespCommandsDocs.json` directly.** These are generated by the CommandInfoUpdater tool.

**Steps:**
1. Build the server: `dotnet build main/GarnetServer/GarnetServer.csproj -c Debug -f net10.0`
2. Start a local Garnet server: `dotnet run --project main/GarnetServer/GarnetServer.csproj -c Debug -f net10.0 --no-build -- --port 6399 --logger-level Warning`
3. Build and run the tool:
   ```bash
   cd playground/CommandInfoUpdater
   dotnet build -f net10.0
   dotnet run -f net10.0 --no-build -- --port 6399 --output ../../libs/resources
   ```
4. The tool will prompt `Would you like to continue? (Y/N)` **twice** (once for info, once for docs). Press `Y` for both.
5. Kill the local server afterward.

**⚠️ Caveat:** The tool uses `Console.ReadKey()` which does NOT work with piped input. You must run it interactively (not via `echo "Y" | dotnet run ...`).

**⚠️ Caveat:** The tool requires a running RESP server to query standard Redis command metadata. For Garnet-only commands, the tool reads from `GarnetCommandsInfo.json` and `GarnetCommandsDocs.json` instead.

---

## Step 9: Add ACL Test

**File:** `test/Garnet.test/Resp/ACL/RespCommandTests.cs`

**⚠️ CRITICAL:** The `AllCommandsCovered` test automatically validates that **every** `RespCommand` enum value has a corresponding ACL test. If you add a command without an ACL test, `AllCommandsCovered` will fail.

**Test naming convention:**
- Method name must end with `ACLs` or `ACLsAsync`
- The name (minus the suffix) must match the command name with dots and underscores removed
- Example: `RI.CREATE` → `RICreateACLsAsync`

**Pattern (follow SADD for non-idempotent commands):**
```csharp
[Test]
public async Task MyCommandACLsAsync()
{
    int count = 0;

    await CheckCommandsAsync(
        "MY.CMD",
        [DoMyCommandAsync]
    ).ConfigureAwait(false);

    async Task DoMyCommandAsync(GarnetClient client)
    {
        var val = await client.ExecuteForStringResultAsync("MY.CMD",
            [$"key-{count}", "arg1"]).ConfigureAwait(false);
        count++;
        ClassicAssert.AreEqual("OK", val);
    }
}
```

**⚠️ Caveat — Idempotency:** The ACL framework calls your command multiple times (for different user/permission combinations). If your command is NOT idempotent (e.g., `RI.CREATE` fails on duplicate), use a counter to generate unique keys per invocation (see the SADD ACL test pattern).

---

## Step 10: Add Integration Tests

**New file:** `test/Garnet.test/Resp<Feature>Tests.cs`

**Required structure:**
```csharp
[TestFixture]
[AllureNUnit]
public class RespMyFeatureTests : AllureTestBase
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
    public void MyBasicTest()
    {
        using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
        var db = redis.GetDatabase(0);
        var result = db.Execute("MY.CMD", "key", "value");
        ClassicAssert.AreEqual("OK", (string)result);
    }
}
```

**⚠️ Required attributes:** `[AllureNUnit]` and inheriting `AllureTestBase` are enforced by CI assembly reflection checks.

**Recommended test cases:**
- Basic success case
- Error cases (wrong args, wrong type)
- Duplicate/idempotency behavior
- DEL/UNLINK interaction (if applicable)
- Type safety (e.g., string command on object key → WRONGTYPE)

---

## Step 11: Update Website Documentation

**File:** `website/docs/commands/` — choose the appropriate markdown file based on the command category (e.g., `garnet-specific.md` for Garnet-only commands, `api-compatibility.md` to mark a standard Redis command as supported).

Add a section documenting the command syntax, description, and response format:

```markdown
### **MY.CMD**

#### **Syntax**

```bash
MY.CMD key value
```

Description of what the command does.

#### **Response**

- **Type reply**: Description of the response.
```

Also mark the command as supported in `website/docs/commands/api-compatibility.md` if it corresponds to a standard Redis command.

---

## Step 12: Verify Everything

### Build
```bash
dotnet build Garnet.slnx -c Debug
```

### Format check
```bash
dotnet format Garnet.slnx --verify-no-changes
```

**⚠️ Caveat:** New files commonly fail with `FINALNEWLINE` errors. Ensure files do NOT have a trailing newline at the very end. Fix with: `perl -pi -e 'chomp if eof' path/to/file.cs`

### Run your tests
```bash
dotnet test test/Garnet.test -f net10.0 -c Debug --filter "FullyQualifiedName~RespMyFeatureTests"
```

### Run ACL coverage test
```bash
dotnet test test/Garnet.test -f net10.0 -c Debug --filter "FullyQualifiedName~AllCommandsCovered"
```

### Run broader regression tests
```bash
dotnet test test/Garnet.test -f net10.0 -c Debug --filter "FullyQualifiedName~RespTests"
```

---

## Transaction Support

For standard commands, transaction key locking is **automatic** — driven by `KeySpecifications` in the command metadata JSON. No per-command code is needed in `TxnKeyManager.cs`.

For custom multi-key operations that don't fit the standard key spec pattern, manually call `txnManager.SaveKeyEntryToLock(key, lockType)`.

---

## Common Caveats and Gotchas

1. **Dot-prefixed commands (e.g., `RI.CREATE`):** The RESP wire name uses a dot, but the enum name cannot. The ACL parser, `AllCommandsCovered` test, and `SupportedCommand.cs` all need to handle the dot-to-enum mapping. Check `ACLParser.cs` for normalization logic.

2. **`AllCommandsCovered` is strict:** It reflects over ALL `RespCommand` enum values and ALL entries in `RespCommandsInfo.json`. Missing either an ACL test or a JSON entry will fail this test.

3. **`NeedCopyUpdate` for create-only commands:** If your command should NOT overwrite existing records (like `SETNX`), return `false` from `NeedCopyUpdate` for your command. Otherwise Tsavorite will attempt a copy-update when the record can't be updated in-place.

4. **`VarLenInputMethods.cs` is easy to forget:** If your command creates or modifies records via RMW, you must add cases to `GetRMWInitialFieldInfo` and `GetRMWModifiedFieldInfo`. Without this, Tsavorite won't allocate the right amount of space for your value.

5. **Resource JSON files are generated, not hand-edited:** `libs/resources/RespCommandsInfo.json` and `libs/resources/RespCommandsDocs.json` are generated by `playground/CommandInfoUpdater`. Edit the source files (`GarnetCommandsInfo.json`, `GarnetCommandsDocs.json`, `SupportedCommand.cs`) and run the tool.

6. **`RespCommandGroup` enum is closed:** The `Group` field in docs JSON must match a value in the `RespCommandGroup` enum (`libs/server/Resp/RespCommandDocs.cs`). Use `Generic` if no existing group fits.

7. **File headers:** All C# files require `// Copyright (c) Microsoft Corporation.` / `// Licensed under the MIT license.`

8. **Test resource usage:** Use small values for buffer sizes, cache sizes, etc. in tests. Don't allocate 16MB+ buffers when 64KB will do.

---

## Reference: RI.CREATE Implementation

The `RI.CREATE` command was the first command implemented following this guide. Key files for reference:

| File | Purpose |
|------|---------|
| `libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs` | RESP handler with option parsing |
| `libs/server/Resp/RangeIndex/RangeIndexManager.cs` | Manager pattern for external data |
| `libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs` | Fixed-size stub struct in store |
| `libs/server/Storage/Session/MainStore/RangeIndexOps.cs` | StorageSession → RMW flow |
| `test/Garnet.test/RespRangeIndexTests.cs` | Integration tests |
| `test/Garnet.test/Resp/ACL/RespCommandTests.cs` | ACL test (search for `RICreateACLsAsync`) |
