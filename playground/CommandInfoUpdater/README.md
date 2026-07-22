# CommandInfoUpdater

A developer tool that (re)generates Garnet's two command‑metadata resource files:

- `libs/resources/RespCommandsInfo.json` — command **info** (arity, flags, ACL categories, key specs, store type, tips). Drives `COMMAND`, `COMMAND INFO`, `COMMAND COUNT`, ACL category checks, and automatic transaction key‑locking.
- `libs/resources/RespCommandsDocs.json` — command **docs** (summary, group, complexity, arguments). Drives `COMMAND DOCS` and client auto‑complete.

> ⚠️ **Never hand‑edit `libs/resources/RespCommandsInfo.json` or `libs/resources/RespCommandsDocs.json`.** They are generated. Edit the tool inputs described below and re‑run the tool.

The tool fills in metadata for **standard** commands by querying a live RESP‑compatible server (we use **Valkey**), and fills in metadata for **Garnet‑only** commands (and any per‑command/per‑subcommand overrides) from two JSON override files that ship with the tool.

---

## TL;DR — full run

```bash
# 1. Start the baseline RESP server (Valkey) on port 6399
docker run -d --rm --name garnet-cmdinfo-valkey -p 6399:6379 valkey/valkey:8.1
docker exec garnet-cmdinfo-valkey valkey-cli -p 6379 PING          # -> PONG

# 2. Edit the inputs you need (see "Inputs you edit" below):
#    - playground/CommandInfoUpdater/SupportedCommand.cs
#    - playground/CommandInfoUpdater/GarnetCommandsInfo.json   (Garnet-only / info overrides)
#    - playground/CommandInfoUpdater/GarnetCommandsDocs.json   (Garnet-only / docs overrides)

# 3. Build and run the tool (rebuild is REQUIRED after editing any input – see note below)
cd playground/CommandInfoUpdater
dotnet build -f net10.0
dotnet run -f net10.0 --no-build -- \
    --port 6399 --host 127.0.0.1 \
    --output ../../libs/resources

# The tool prompts "Would you like to continue? (Y/N)" TWICE (once for info, once for docs).
# Press Y for both — or pass --yes to auto-confirm for non-interactive runs.

# 4. Stop the baseline server
docker stop garnet-cmdinfo-valkey

# 5. Rebuild Garnet so the server embeds the regenerated resources, then review the diff
cd ../..
dotnet build Garnet.slnx -c Debug -f net10.0
git diff --stat libs/resources/
```

---

## Baseline RESP server (Docker)

The tool queries a running RESP server for the metadata of **standard** commands (via `COMMAND INFO` and `COMMAND DOCS`). Use **Valkey** (not Garnet — Garnet is the thing being described, and not plain Redis):

```bash
# Start (foreground logs: drop -d). --rm auto-removes on stop. Host 6399 -> container 6379.
docker run -d --rm --name garnet-cmdinfo-valkey -p 6399:6379 valkey/valkey:8.1

# Verify it is up
docker exec garnet-cmdinfo-valkey valkey-cli -p 6379 PING            # -> PONG
docker exec garnet-cmdinfo-valkey valkey-cli -p 6379 INFO server | grep version

# Stop when done
docker stop garnet-cmdinfo-valkey
```

The `--port` you pass to the tool must match the **host** port you published (`6399` above).

---

## Inputs you edit

All three inputs are **compiled/embedded into the tool**, so you must **rebuild the tool** after editing any of them (i.e. run `dotnet build` before `dotnet run --no-build`, or just drop `--no-build`).

### 1. `SupportedCommand.cs` — the source of truth for *which* commands exist

`AllSupportedCommands` lists every command (and sub‑command) Garnet supports, mapping the wire name to its `RespCommand` enum value and `StoreType`.

```csharp
// Parent command with sub-commands and no keys (StoreType.None):
new("ACL", RespCommand.ACL, StoreType.None,
[
    new("ACL|CAT", RespCommand.ACL_CAT),
    new("ACL|SETUSER", RespCommand.ACL_SETUSER),
    // ...
]),

// Simple key-value command in the main (string) store:
new("APPEND", RespCommand.APPEND, StoreType.Main),
```

`StoreType`: `Main` (string store), `Object` (object store), `All` (both), `None` (no keys / admin).

- **Add a command** → add an entry here. If it also exists on the baseline server, its info/docs are fetched from the server; if it is Garnet‑only, add it to the override files (below).
- **Remove a command** → delete its entry here; the tool will drop it from the resource files.

### 2. `GarnetCommandsInfo.json` — info for Garnet‑only commands (and info overrides)

Add an entry for any command that is **not** returned by the baseline RESP server (e.g. `DELIFGREATER`, `SETIFMATCH`, `RI.*`), or to override the info of a standard command.

```json
{
  "Command": "MYCMD",
  "Name": "MY.CMD",
  "IsInternal": false,
  "Arity": -2,
  "Flags": "DenyOom, Write",
  "FirstKey": 1, "LastKey": 1, "Step": 1,
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

### 3. `GarnetCommandsDocs.json` — docs for Garnet‑only commands (and per‑subcommand overrides)

Add an entry for Garnet‑only command docs, **or override individual summaries** of a standard command (see "Overriding individual summaries" below).

```json
{
  "Command": "MYCMD",
  "Name": "MY.CMD",
  "Summary": "Description of what the command does.",
  "Group": "Generic",
  "Complexity": "O(1)",
  "Arguments": [
    { "TypeDiscriminator": "RespCommandKeyArgument", "Name": "KEY", "DisplayText": "key", "Type": "Key", "KeySpecIndex": 0 }
  ]
}
```

`Group` must be a valid `RespCommandGroup` value (`Bitmap`, `Cluster`, `Connection`, `Generic`, `Geo`, `Hash`, `HyperLogLog`, `List`, `Module`, `PubSub`, `Scripting`, `Sentinel`, `Server`, `Set`, `SortedSet`, `Stream`, `String`, `Transactions`). Do **not** invent group names.

---

## How it works

`Program.cs` runs two stages in sequence: **info first, then docs**. Each stage:

1. **Loads the current ("existing") resource** — `RespCommandsInfo.json` / `RespCommandsDocs.json` read from the compiled **`Garnet.resources`** assembly (not the files on disk). This is the baseline it diffs against.
2. **Computes the diff** (`CommonUtils.GetCommandsToAddAndRemove`) between `SupportedCommand.cs` and the existing resource:
   - in `SupportedCommand.cs` but not in existing → **to add**
   - in existing but not in `SupportedCommand.cs` (and not `--ignore`d) → **to remove**
3. **Prompts for confirmation** (`Would you like to continue? (Y/N)`), printing exactly what will be added/removed. If there is nothing to do it logs `No commands to update` and returns.
4. **Queries the baseline server** for the metadata of the *to‑add* standard commands (`COMMAND INFO` / `COMMAND DOCS`).
5. **Merges** server results with the Garnet override file:
   - **Parent/base command:** the Garnet override wins when a command is present in both.
   - **Sub‑commands:** merged **by name** — Garnet override sub‑commands are added first, the server fills in only the ones the override did not specify (`Add` then `TryAdd`).
6. **Writes** the resource file to `--output` (unchanged commands are preserved verbatim from the existing resource).

> The **docs stage only runs if the info stage returns success.** If the info stage has nothing to update (or you answer `N`), the docs stage is skipped. To regenerate docs, the info stage must also have at least one add/remove — see "Regenerating one command surgically".

### Overriding individual summaries (while still using the tool)

Because the merge is **per‑subcommand and override‑wins**, you can override a single summary without hand‑editing the generated file:

1. Set the desired `Summary` for the sub‑command in **`GarnetCommandsDocs.json`** (list the parent command with the sub‑commands you want to control; sub‑commands you omit come from the server).
2. Rebuild the tool and run it. The generated `RespCommandsDocs.json` picks up your summary; everything else comes from the server.

Example (from the `OBJECT` command): `GarnetCommandsDocs.json` lists `OBJECT` with sub‑commands `OBJECT|ENCODING`, `OBJECT|FREQ`, `OBJECT|IDLETIME`, `OBJECT|REFCOUNT`, `OBJECT|HELP`, each carrying a Garnet‑accurate `Summary`. The tool copies those verbatim and takes nothing from the server for them.

---

## Command‑line options (`Options.cs`)

| Option | Alias | Default | Description |
|--------|-------|---------|-------------|
| `--port` | `-p` | `6379` | Baseline RESP server port (must match the published Docker host port). |
| `--host` | `-h` | `127.0.0.1` | Baseline RESP server host. |
| `--output` | `-o` | *(cwd)* | Directory to write the JSON files. Use `../../libs/resources` from the tool folder. |
| `--force` | `-f` | `false` | Ignore the existing resource and regenerate **every** command from server + overrides. |
| `--ignore` | `-i` | *(none)* | Comma‑separated commands to leave untouched (neither add nor remove). |
| `--yes` | `-y` | `false` | Auto-confirm all prompts (non-interactive / scripted runs). |

### Why `--ignore`?

A command that is in the current resource files but **not** in `SupportedCommand.cs` would otherwise be flagged for **removal**. `--ignore` tells the tool to leave such commands untouched (neither add nor remove).

Every command Garnet ships is registered in `SupportedCommand.cs` (with its metadata in the override files where it isn't queryable from the baseline server), so a normal regeneration needs **no `--ignore` flag**. Reach for it only as a temporary escape hatch — e.g. if you are mid‑way through adding a command and its resource entry exists before you have finished wiring up `SupportedCommand.cs`, or to protect an entry you are intentionally hand‑maintaining outside the tool.

`--ignore` is command‑level only (there is no CLI flag to ignore a single sub‑command).

---

## After running

```bash
# Rebuild Garnet so the server embeds the new resources
dotnet build Garnet.slnx -c Debug -f net10.0

# Sanity-check the resource consistency tests
dotnet test test/standalone/Garnet.test -f net10.0 -c Debug \
  --filter "FullyQualifiedName~RespCommandTests.CommandsInfoCoverageTest|FullyQualifiedName~RespCommandTests.CommandsDocsCoverageTest"

# Review the diff — it should contain only your intended change
git diff libs/resources/
```

---

## Caveats & troubleshooting

- **Interactive prompt (and how to skip it).** By default the tool prompts `Would you like to continue? (Y/N)` via `Console.ReadKey()`, which does **not** read piped input (`echo Y | dotnet run …` does not work). For non-interactive / scripted runs, pass **`-y` / `--yes`** to auto-confirm both prompts:

  ```bash
  dotnet run -f net10.0 --no-build -- --port 6399 --output ../../libs/resources --yes
  ```

- **Rebuild after editing inputs.** `SupportedCommand.cs`, `GarnetCommandsInfo.json`, and `GarnetCommandsDocs.json` are compiled/embedded into the tool assembly. If you use `--no-build`, run `dotnet build` first, otherwise your edits are not picked up.
- **`--force` re‑queries everything.** It regenerates all ~250 commands from the baseline server, so the output reflects *that server's* exact metadata — e.g. a Valkey baseline yields Valkey wording ("primary" vs "master"), Valkey‑specific flags, and its sub‑command ordering (an internal sub‑command such as `CLUSTER|RESERVE` may move). Use it to rebuild from scratch or to intentionally re‑baseline against a server; for a focused change, prefer a targeted non‑force run.
- **Docs stage skipped ("No commands to update").** The docs stage only runs after the info stage succeeds with at least one change. See below.
- **Unrecognized flags/ACL categories are skipped with a warning.** If the baseline server reports a command flag or ACL category Garnet does not model (e.g. a newer server renamed one), the info parser logs a warning and skips it rather than aborting. Review such warnings — a genuine rename should be added as a `[Description]`/`[EnumDescriptionAlias]` on the relevant enum.
- **Do not hand‑edit the generated files.** They carry a UTF‑8 BOM, use relaxed JSON escaping (literal `'`, `` ` ``, `+` rather than `\uXXXX`), and have no trailing newline. The tool writes them consistently; hand‑edits will drift.

### Regenerating one command surgically

To re‑emit **only one command** (e.g. after changing just its override summary) without touching any other entry and without a full `--force`:

1. Temporarily remove that command's entry from `libs/resources/RespCommandsInfo.json` **and** `libs/resources/RespCommandsDocs.json`.
2. Rebuild the resources so the embedded baseline reflects the removal:
   `dotnet build playground/CommandInfoUpdater/CommandInfoUpdater.csproj -f net10.0`
3. Run the tool non‑force. The command is now "to add" in **both** stages (so the info stage has a change and the docs stage runs), and only that command is re‑queried/re‑merged; every other entry is preserved verbatim.
4. `git diff libs/resources/` — the diff is limited to the single command.

---

## Related

- Adding a new command end‑to‑end: `.github/skills/add-garnet-command/SKILL.md` (Step 8 covers this tool).
- Developer docs: <https://microsoft.github.io/garnet/docs/dev/garnet-api>
