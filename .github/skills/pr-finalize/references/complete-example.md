# Complete Example: Agent-Optimized PR

This example shows a PR description optimized for future agent success in the Garnet codebase.

## Title

```
[RESP] ZADD: Support GT/LT flags for conditional score updates
```

## Description

```markdown
### Root Cause

The ZADD command handler in `SortedSetCommands.cs` only supported the `NX`, `XX`, and `CH` flags. The Redis-compatible `GT` (only update if new score is greater) and `LT` (only update if new score is less) flags were missing, causing compatibility failures with clients that rely on these flags for leaderboard and rate-limiting patterns.

### Description of Change

Added `GT` and `LT` flag parsing and conditional update logic to the ZADD command pipeline.

**Key changes:**
- Parse `GT`/`LT` flags in `SortedSetCommands.cs` alongside existing `NX`/`XX`/`CH` flags
- Added validation: `GT` and `LT` are mutually exclusive with `NX` (returns error per Redis spec)
- Passed flags through `ObjectInput` to `SortedSetObjectImpl.SortedSetAdd()`
- Implemented conditional score comparison before update in the sorted set object

### Key Technical Details

**Affected types:**
- `RespServerSession` (partial in `SortedSetCommands.cs`) — flag parsing from RESP buffer
- `SortedSetObject` (`SortedSetObjectImpl.cs`) — conditional update logic
- `StorageSession` (`SortedSetOps.cs`) — flag propagation via `ObjectInput`

**Flag interaction matrix:**

| Flags | Behavior |
|-------|----------|
| `GT` alone | Update only if new score > current score |
| `LT` alone | Update only if new score < current score |
| `GT XX` | Update existing members only if new score > current |
| `GT NX` | ❌ Error — mutually exclusive |
| `LT NX` | ❌ Error — mutually exclusive |

### What NOT to Do (for future agents)

- ❌ **Don't allocate strings for flag comparison** — Use `SpanByte` comparison against known byte sequences; the RESP hot path must stay allocation-free
- ❌ **Don't modify `SortedSetOperation` enum for flags** — Flags modify behavior of existing operations, they are not separate operations. Pass via `ObjectInput` fields instead
- ❌ **Don't skip the NX mutual exclusion check** — Redis returns an error for `GT NX` and `LT NX` combinations; clients depend on this

### Edge Cases

| Scenario | Risk | Mitigation |
|----------|------|------------|
| `GT` with equal scores | Low | Strict `>` comparison, no update on equal |
| `LT` with `NaN` score | Low | `NaN` comparison returns false, no update |
| Mixed `GT CH` counting | Medium | `CH` counts "changed" — includes not-updated members; tested explicitly |
| Cluster mode with key migration | Low | Flags are per-command, no cross-node impact |

### Issues Fixed

Fixes #456
```

## Why This Works for Agents

- **Searchable title** — Agents searching "ZADD GT LT" or "conditional score" will find this
- **Flag interaction matrix** — Agents know exactly how flags combine
- **What NOT to do** — Agents won't allocate on the hot path or misuse the operation enum
- **Edge cases** — Agents know the risk profile for NaN, equal scores, and CH interaction
- **Affected types listed** — Agents know which files to modify for similar flag additions
