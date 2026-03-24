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
| 5 | API interface | `libs/server/API/IGarnetApi.cs` | If key-value command (not blocking/admin) |
| 6 | API delegation | `libs/server/API/GarnetApi*.cs` | If key-value command (not blocking/admin) |
| 7 | Storage session | `libs/server/Storage/Session/[Main\|Object\|Unified]Store/*Ops.cs` | If key-value command (not blocking/admin) |
| 8 | RMW/Read callbacks | `libs/server/Storage/Functions/[Main\|Unified]Store/[RMW\|Read]Methods.cs` | If string/unified command using RMW/Read |
| 8b | Read response | `libs/server/Storage/Functions/MainStore/PrivateMethods.cs` | If string command using Read (add to `CopyRespToWithInput`) |
| 9 | VarLen methods | `libs/server/Storage/Functions/[Main\|Unified]Store/VarLenInputMethods.cs` | If string/unified command using RMW |
| 10 | Object operation enum | `libs/server/Objects/[ObjectName]/[ObjectName]Object.cs` | If new object sub-operation |
| 11 | Object implementation | `libs/server/Objects/[ObjectName]/[ObjectName]ObjectImpl.cs` | If new object sub-operation |
| 12 | ItemBroker | `libs/server/Objects/ItemBroker/CollectionItemBroker.cs` | If blocking command |
| 13 | Command info JSON | `libs/resources/RespCommandsInfo.json` | ✅ Always (generated) |
| 14 | Command docs JSON | `libs/resources/RespCommandsDocs.json` | ✅ Always (generated) |
| 15 | Supported commands | `playground/CommandInfoUpdater/SupportedCommand.cs` | ✅ Always |
| 16 | Garnet command info | `playground/CommandInfoUpdater/GarnetCommandsInfo.json` | If Garnet-only command |
| 17 | Garnet command docs | `playground/CommandInfoUpdater/GarnetCommandsDocs.json` | If Garnet-only command |
| 18 | ACL test | `test/Garnet.test/Resp/ACL/RespCommandTests.cs` | ✅ Always |
| 19 | Integration tests | `test/Garnet.test/Resp*.cs` | ✅ Always |
| 20 | Website documentation | `website/docs/commands/` | ✅ Always |
| 21 | Configuration settings | `Options.cs`, `GarnetServerOptions.cs`, `defaults.conf` | If command is optional/gated |

---

## Step 1: Add the RespCommand Enum Value

**File:** `libs/server/Resp/Parser/RespCommand.cs`

The `RespCommand` enum is divided into sections with **ordering that matters**:

```
Read commands:     BITCOUNT ... ZUNION     (before APPEND)
Write commands:    APPEND ... BITOP_DIFF   (after APPEND)
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
ZUNION,  // Note: Last read command is determined by APPEND - 1
APPEND, // Note: Update FirstWriteCommand if adding new write commands before this
BITOP_DIFF, // Note: Update LastWriteCommand if adding new write commands after this
EVALSHA, // Note: Update LastDataCommand if adding new data commands after this
```

**⚠️ Caveat:** The boundary comments in the source may not be on the actual last/first entry (e.g., `ZSCORE` has the comment but `ZUNION` follows it). The real boundary is determined by code: `LastReadCommand = RespCommand.APPEND - 1`. Always check the actual enum ordering, not just the comments.

---

## Step 2: Add Command Parsing

**File:** `libs/server/Resp/Parser/RespCommand.cs`

Two parsing paths exist:

### Fast path: `FastParseCommand()` / `FastParseArrayCommand()`
Two fast-path methods exist with different constraints:
- **`FastParseCommand()`**: For commands with a fixed number of arguments and command names up to **9 characters**. Uses `ulong` pointer comparisons on `(count << 4) | length` patterns.
- **`FastParseArrayCommand()`**: For commands with a variable number of arguments and command names up to **16 characters**. Uses similar `ulong` comparison patterns but accommodates longer names.

Only add here if the command name is a simple word (no dots or special characters).

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

    // 2. Validate other inputs (short-circuit before going to storage)
    var key = parseState.GetArgSliceByRef(0);
    // e.g., parse and validate optional flags, numeric arguments, etc.
    if (!parseState.TryGetInt(1, out var _))
    {
        WriteError(CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return true;
    }

    // 3. Build input/output and call storage API
    // Note: To avoid double-parsing a parameter, you can pass a pre-parsed
    // value in the input struct's auxiliary arguments (e.g., input.arg1).
    var input = new StringInput(RespCommand.MYCMD, ref parseState, startIdx: 1);
    var output = GetStringOutput();
    var status = storageApi.MyOperation(key, ref input, ref output);

    // 4. Write RESP response
    if (status == GarnetStatus.OK)
    {
        ProcessOutput(output);
    }
    else
    {
        WriteError(CmdStrings.RESP_ERR_MY_MESSAGE);
    }

    return true;
}
```

**Key patterns:**
- Arguments: `parseState.GetArgSliceByRef(i)` returns `ref PinnedSpanByte`
- Input/Output: Instantiate `StringInput`/`StringOutput` (for string commands), `ObjectInput`/`ObjectOutput` (for object commands), or `UnifiedInput`/`UnifiedOutput` (for unified commands) before calling the storage API
- Response (happy path): Use `ProcessOutput(output)` in the common case — this handles writing the RESP response from the output struct
- Response (errors/special cases): Use `RespServerSession` extension methods (e.g., `WriteError(...)`, `WriteDirect(...)`, `WriteInt64(...)`, etc.) — these handle `SendAndReset()` internally
- Error strings: Store as `u8` literals in `CmdStrings` (e.g., `CmdStrings.RESP_ERR_MY_MESSAGE`) rather than inline
- Always return `true` — there are no partial executions

### Object command RESP handler pattern

Object commands (Hash, List, Set, SortedSet) follow a similar pattern to string commands. The main difference is that the RESP handler uses `ObjectInput`/`ObjectOutput` with the appropriate operation enum and must handle `WRONGTYPE` errors:

**File:** `libs/server/Resp/Objects/[ObjectName]Commands.cs` (e.g., `SortedSetCommands.cs`)

```csharp
private unsafe bool SortedSetAdd<TGarnetApi>(ref TGarnetApi storageApi)
    where TGarnetApi : IGarnetApi
{
    if (parseState.Count < 3)
        return AbortWithWrongNumberOfArguments("ZADD");

    var key = parseState.GetArgSliceByRef(0);

    var header = new RespInputHeader(GarnetObjectType.SortedSet) { SortedSetOp = SortedSetOperation.ZADD };
    var input = new ObjectInput(header, ref parseState, startIdx: 1);
    var output = GetObjectOutput();

    var status = storageApi.SortedSetAdd(key, ref input, ref output);

    switch (status)
    {
        case GarnetStatus.WRONGTYPE:
            while (!RespWriteUtils.TryWriteError(CmdStrings.RESP_ERR_WRONG_TYPE, ref dcurr, dend))
                SendAndReset();
            break;
        default:
            ProcessOutput(output.SpanByteAndMemory);
            break;
    }

    return true;
}
```

**Key differences from string commands:**
- Uses `ObjectInput` with a `RespInputHeader(GarnetObjectType.XXX)` and an operation enum (e.g., `SortedSetOperation.ZADD`)
- Must handle `GarnetStatus.WRONGTYPE` — object commands can fail if the key holds a different object type
- The actual data operation logic lives in `libs/server/Objects/[ObjectName]/[ObjectName]ObjectImpl.cs`, dispatched via the operation enum

**For new object sub-operations:**
Add a value to the `[ObjectName]Operation` enum in `libs/server/Objects/[ObjectName]/[ObjectName]Object.cs` and handle it in the `Operate` method's switch statement.

### Unified command note

Unified commands (EXISTS, DELETE, TYPE, TTL, EXPIRE, RENAME, etc.) are type-agnostic — they work on both raw string and object values. The RESP handler pattern is the same as string and object commands, but uses `UnifiedInput`/`UnifiedOutput`. The storage session layer uses the unified context (`unifiedBasicContext`), and the callbacks go in `libs/server/Storage/Functions/UnifiedStore/`.

### Blocking command RESP handler pattern

Blocking commands (`BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`) follow a distinct pattern. They do **not** use `ref storageApi` and instead interact with the `CollectionItemBroker` (`libs/server/Objects/ItemBroker/CollectionItemBroker.cs`), which manages blocking/waiting behavior:

```csharp
private unsafe bool SortedSetBlockingPop(RespCommand command)
{
    if (parseState.Count < 2)
        return AbortWithWrongNumberOfArguments(command.ToString());

    if (!parseState.TryGetTimeout(parseState.Count - 1, out var timeout, out var error))
        return AbortWithErrorMessage(error);

    var keysBytes = new byte[parseState.Count - 1][];
    for (var i = 0; i < keysBytes.Length; i++)
        keysBytes[i] = parseState.GetArgSliceByRef(i).ToArray();

    var result = storeWrapper.itemBroker.GetCollectionItemAsync(command, keysBytes, this, timeout).Result;

    if (!result.Found)
    {
        WriteNull();
    }
    else
    {
        // Write RESP response with result.Key, result.Item, result.Score, etc.
    }

    return true;
}
```

**Key differences from regular commands:**
- The dispatch in `RespServerSession.cs` does NOT pass `ref storageApi`: `RespCommand.BZMPOP => SortedSetBlockingMPop(),`
- The handler calls `storeWrapper.itemBroker.GetCollectionItemAsync()` which blocks (with timeout) until data is available
- No `IGarnetApi` method, no storage session method, and no RMW callbacks are needed for the blocking command itself (Steps 5-7 are skipped)
- The `CollectionItemBroker` is notified when data is added to a collection (e.g., `ZADD` calls `itemBroker.HandleCollectionUpdate(key)`), which wakes up blocked clients
- When adding a new blocking command, you must also update the `TryGetResult` method in `CollectionItemBroker.cs` to map your `RespCommand` to the correct `GarnetObjectType` and implement the retrieval logic

---

## Steps 5–7: Storage Layer (skip for admin/non-key commands)

> **Note:** Steps 5, 6, and 7 apply only to commands that read or write key-value data through the store (e.g., `SET`, `GET`, `DELIFGREATER`). Admin commands like `DEBUG`, `PING`, `CONFIG`, etc. handle their logic entirely in the RESP handler (Step 4) and do **not** need API interface methods, storage session ops, or RMW callbacks. Skip to Step 8 for those. Blocking commands (e.g., `BZMPOP`) also skip Steps 5-7 — see the blocking command pattern in Step 4.
>
> **Note on context types:** The unified single-store has three context types: **string context** (for raw string commands like GET/SET), **object context** (for collection commands like ZADD/LPUSH), and **unified context** (for type-agnostic commands like EXISTS/DELETE/TTL/EXPIRE). Most new commands use either the string or object context — the unified context is only for commands that must work across both value types.

## Step 5: Add API Interface Method

**File:** `libs/server/API/IGarnetApi.cs`

Add method signature to `IGarnetApi` (read-write) or `IGarnetReadApi` (read-only):

```csharp
// String command:
GarnetStatus MyOperation(PinnedSpanByte key, ref StringInput input, ref StringOutput output);

// Object command:
GarnetStatus MyOperation(PinnedSpanByte key, ref ObjectInput input, ref GarnetObjectStoreOutput output);

// Unified command:
GarnetStatus MyOperation(PinnedSpanByte key, ref UnifiedInput input, ref UnifiedOutput output);
```

**File:** `libs/server/API/GarnetApi*.cs`

Add delegation in the `GarnetApi` partial struct. The implementation goes in the appropriate partial file based on the context type:
- `GarnetApi.cs` — string commands
- `GarnetApiObjectCommands.cs` — object commands
- `GarnetApiUnifiedCommands.cs` — unified commands

```csharp
public GarnetStatus MyOperation(PinnedSpanByte key, ref StringInput input, ref StringOutput output)
    => storageSession.MyOperation(key, ref input, ref output);
```

**⚠️ Caveat:** `GarnetApi` is a generic partial struct: `GarnetApi<TStringContext, TObjectContext, TUnifiedContext>`. Always add your method to the correct partial file for the context type you're using.

**Overloads for programmatic callers:** In addition to the primary signature (used by the network handler), you can add simpler overloads for programmatic/embedded callers that avoid forcing them to create the Input/Output structs. For example:

```csharp
public GarnetStatus MyOperation(PinnedSpanByte key, double val, out double output)
```

This overload internally creates the appropriate input/output structs and only returns the desired value to the caller, instead of writing to the output buffer.

---

## Step 6: Implement Storage Session Layer

**File:** New or existing file in `libs/server/Storage/Session/MainStore/` (for string-context ops), `libs/server/Storage/Session/ObjectStore/` (for object-context ops), or `libs/server/Storage/Session/UnifiedStore/` (for unified-context ops)

### String command pattern

This layer wraps Tsavorite API calls. The network path uses a generic context parameter:

```csharp
public GarnetStatus MyOperation<TStringContext>(PinnedSpanByte key, ref StringInput input, ref StringOutput output, ref TStringContext context)
    where TStringContext : ITsavoriteContext<...>
{
    var status = context.RMW((FixedSpanByteKey)key, ref input, ref output);
    if (status.IsPending)
        CompletePendingForSession(ref status, ref output, ref context);

    return GarnetStatus.OK;
}
```

Object and unified commands follow the same pattern — just substitute the appropriate context, input, and output types:

| Context type | Input type | Output type | Helper method |
|-------------|-----------|------------|---------------|
| String | `StringInput` | `StringOutput` | `context.RMW(...)` / `context.Read(...)` |
| Object | `ObjectInput` | `GarnetObjectStoreOutput` | `RMWObjectStoreOperation(...)` / `ReadObjectStoreOperation(...)` |
| Unified | `UnifiedInput` | `UnifiedOutput` | `context.RMW(...)` / `context.Read(...)` |

**Programmatic overloads:** You can also add simpler overloads for programmatic callers (see Step 5 note). These internally create the input/output structs and return only the desired value.

### Object-specific: HandleCollectionUpdate

Object commands use the same pattern as above with `ObjectInput`/`GarnetObjectStoreOutput` and the object context.

**⚠️ Caveat — `HandleCollectionUpdate`:** If your object command modifies a collection (adds/removes elements), call `itemBroker.HandleCollectionUpdate(key)` after the store operation. This wakes up any clients blocked on that key (e.g., via `BZPOPMIN`). The actual data logic is implemented in the object class, not in the storage session.

### Object implementation

**File:** `libs/server/Objects/[ObjectName]/[ObjectName]ObjectImpl.cs`

For object commands, the core logic lives in the object implementation. The `Operate` method in `[ObjectName]Object.cs` dispatches to implementation methods based on the operation enum:

```csharp
case SortedSetOperation.ZADD:
    SortedSetAdd(ref input, ref output.SpanByteAndMemory);
    break;
```

The implementation methods in `[ObjectName]ObjectImpl.cs` directly manipulate the object's internal data structures (e.g., `sortedSet`, `sortedSetDict` for SortedSet).

---

## Step 7: Add RMW/Read Callbacks (if applicable)

**File:** `libs/server/Storage/Functions/MainStore/RMWMethods.cs` (string commands) or `libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs` (unified commands)

If your command uses `RMW`, you must handle these callbacks:

| Callback | When | Purpose |
|----------|------|---------|
| `NeedInitialUpdate` | Key doesn't exist | Return `true` to create record |
| `InitialUpdater` | Creating new record | Write initial value |
| `NeedCopyUpdate` | Key exists, record needs copy | Return `true` to copy-update, `false` to skip |
| `InPlaceUpdater` | Key exists, update in place | Modify existing value |
| `CopyUpdater` | Key exists, copy to new record | Write updated value to new record |

Add a `case RespCommand.MYCMD:` to each relevant switch statement.

**For Read commands (MainStore):** If your command uses `Read` (not RMW), the read response logic lives in `libs/server/Storage/Functions/MainStore/PrivateMethods.cs` — add a case to the `CopyRespToWithInput` method.

**File:** `libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs` (string commands) or `libs/server/Storage/Functions/UnifiedStore/VarLenInputMethods.cs` (unified commands)

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

Add entry following the existing ordering/grouping in the file:
```csharp
new("MY.CMD", RespCommand.MYCMD, StoreType.Main),
```

> **Note:** The file is not strictly alphabetical — entries are grouped by category (e.g., script commands at the end). Follow the existing grouping conventions rather than inserting strictly alphabetically.

For admin/non-key commands (e.g., `DEBUG`, `PING`), omit `StoreType` or use `StoreType.None`:
```csharp
new("DEBUG", RespCommand.DEBUG),
```

`StoreType` values: `Main` (string store), `Object` (object store), `All` (both), `None` (no keys).

### 8b. Add to GarnetCommandsInfo.json (Garnet-only commands)

**File:** `playground/CommandInfoUpdater/GarnetCommandsInfo.json`

Needed for commands that don't exist in standard Redis (e.g., `DELIFGREATER`, `SETIFMATCH`), or standard Redis commands whose info you need to override. Standard Redis commands (e.g., `DEBUG`, `GETDEL`) normally get their metadata from a running RESP server automatically via the CommandInfoUpdater tool — skip this step and Step 8c for those unless you need to override their info.

Add a JSON entry:

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

> **Note:** This step is not necessary for internal commands. The main purpose of command docs is to enable client auto-complete for the command.

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
1. Start a local RESP-compatible server (e.g., **Valkey** or Redis) — the tool queries it for standard Redis command metadata:
   ```bash
   valkey-server --port 6399
   ```
2. Build and run the tool:
   ```bash
   cd playground/CommandInfoUpdater
   dotnet build -f net10.0
   dotnet run -f net10.0 --no-build -- --port 6399 --output ../../libs/resources
   ```
   (The `--port` must match the port of the local RESP server.)
3. The tool will prompt `Would you like to continue? (Y/N)` **twice** (once for info, once for docs). Press `Y` for both.
4. Kill the local RESP server afterward.

**⚠️ Caveat:** The tool uses `Console.ReadKey()` which does NOT work with piped input. You must run it interactively (not via `echo "Y" | dotnet run ...`). For AI agents, use an async shell session and send `Y` keystrokes via interactive input (e.g., `write_bash`).

**⚠️ Caveat:** The tool requires a running RESP-compatible server (e.g., Valkey or Redis — **not** Garnet) to query standard command metadata. For Garnet-only commands, the tool reads from `GarnetCommandsInfo.json` and `GarnetCommandsDocs.json` instead.

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

**File:** Add tests to an existing or new file in `test/Garnet.test/`:
- **String / Unified commands**: Add to `test/Garnet.test/RespTests.cs`
- **Object commands**: Add to `test/Garnet.test/Resp[ObjectName]Tests.cs` (e.g., `RespSortedSetTests.cs`)
- **New feature area**: Create `test/Garnet.test/Resp<Feature>Tests.cs` if the command doesn't fit existing test files

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

## Step 11b: Add Configuration Settings (if needed)

If the command is optional, gated behind a feature flag, or needs a server-side configuration parameter (e.g., `DEBUG` requires `--enable-debug-command`), you must wire up a configuration option across four files:

### 1. Add property to `Options` class

**File:** `libs/host/Configuration/Options.cs`

Add a property with the `[Option]` attribute (from CommandLineParser):

```csharp
[OptionValidation]
[Option("enable-my-feature", Required = false, HelpText = "Enable MY.CMD for 'no', 'local' or 'all' connections")]
public ConnectionProtectionOption EnableMyFeature { get; set; }
```

The `[Option]` attribute defines the CLI flag name (kebab-case). Use `Required = false` for optional settings. Common types: `bool`, `int`, `string`, `ConnectionProtectionOption` (for no/local/yes connection gating), or custom enums.

### 2. Map to `GarnetServerOptions`

**File:** `libs/server/Servers/GarnetServerOptions.cs`

Add a matching field:

```csharp
/// <summary>
/// Enables MY.CMD
/// </summary>
public ConnectionProtectionOption EnableMyFeature;
```

Then in `Options.GetServerOptions()` (in `Options.cs`), map the property:

```csharp
EnableMyFeature = EnableMyFeature,
```

### 3. Add default value

**File:** `libs/host/defaults.conf`

Add the default in the appropriate section:

```json
/* Enable MY.CMD for clients - no/local/yes */
"EnableMyFeature": "no",
```

### 4. Check the setting in your RESP handler

Access the setting via `storeWrapper.serverOptions`:

```csharp
if (storeWrapper.serverOptions.EnableMyFeature == ConnectionProtectionOption.No)
{
    while (!RespWriteUtils.TryWriteError("ERR command not enabled"u8, ref dcurr, dend))
        SendAndReset();
    return true;
}
```

### 5. Add config tests

**File:** `test/Garnet.test/GarnetServerConfigTests.cs`

Test that the setting is parsed correctly from CLI args and config files.

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
