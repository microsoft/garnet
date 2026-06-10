# Redis Stream Commands — Implementation Status

## Implemented

| Command | Status | Options Supported | Missing Options |
|---------|--------|-------------------|-----------------|
| **XADD** | ✅ Done | `NOMKSTREAM`, `*`, explicit IDs (`ts-seq`, `ts-*`) | `MAXLEN`/`MINID` inline trim, `LIMIT`, `IDMPAUTO`/`IDMP` (idempotent producers, Redis 8.6+) |
| **XLEN** | ✅ Done | — | — |
| **XRANGE** | ✅ Done | `COUNT`, `-`/`+` bounds | Incomplete ID handling (e.g. `1-`) |
| **XREVRANGE** | ✅ Done | `COUNT`, `-`/`+` bounds | Same as XRANGE |
| **XDEL** | ✅ Done | Multiple IDs | — |
| **XTRIM** | ✅ Done | `MAXLEN`, `MINID`, `~` (approximate) | `LIMIT`, `=` exact flag |

> **XLAST** is also implemented (non-standard / Garnet extension).

## Not Implemented

| Command | Category | Description |
|---------|----------|-------------|
| **XREAD** | Core | Blocking/non-blocking read from one or more streams |
| **XINFO** | Introspection | `STREAM`, `GROUPS`, `CONSUMERS`, `FULL` subcommands |
| **XSETID** | Admin | Manually set the last delivered ID of a stream |
| **XGROUP** | Consumer Groups | `CREATE`, `SETID`, `DESTROY`, `CREATECONSUMER`, `DELCONSUMER` |
| **XREADGROUP** | Consumer Groups | Read as a consumer within a group |
| **XACK** | Consumer Groups | Acknowledge processed messages |
| **XPENDING** | Consumer Groups | Inspect pending entries list (PEL) |
| **XCLAIM** | Consumer Groups | Change ownership of pending messages |
| **XAUTOCLAIM** | Consumer Groups | Automatic claim of idle pending messages |
| **XCFGSET** | Config | Set stream config (`IDMP-DURATION`, `IDMP-MAXSIZE`) for idempotency |

## Suggested Priority
1. **XREAD** — most commonly used after XADD/XRANGE; needed for pub-sub style patterns
2. **XADD inline trim** (`MAXLEN`/`MINID`) — frequently used in production for capped streams
3. **XINFO STREAM** — basic introspection, easy to add
4. **XSETID** — simple admin command
5. **Consumer Groups** (XGROUP → XREADGROUP → XACK → XPENDING → XCLAIM → XAUTOCLAIM) — large feature set, implement as a block
