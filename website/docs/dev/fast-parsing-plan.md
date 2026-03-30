# Optimize RESP Command Parsing (`ParseCommand`)

## Problem Statement

`ParseCommand` in `libs/server/Resp/Parser/RespCommand.cs` is the entry point for parsing every RESP command from the network buffer. It uses a multi-tier approach:

| Tier | Method | Commands Covered | Complexity |
|------|--------|-----------------|------------|
| 1 | `FastParseCommand` | ~40 most common (GET, SET, PING, DEL, INCR, TTL...) | O(1) — matches RESP header+command in one 8-byte read |
| 2 | `FastParseArrayCommand` | ~100 more (HSET, ZADD, LPUSH, SUBSCRIBE, GEOADD...) | O(depth) — 950 lines of nested switch/if-else by length→first-char→ulong compare |
| 3 | `SlowParseCommand` | ~80 remaining (CLUSTER *, CONFIG *, ACL *, admin cmds...) | O(n) — sequential `SequenceEqual` comparisons |

**The hot path (Tier 1) works well** — it reads `*N\r\n$L\r\nCMD\r\n` as a single ulong and matches in one comparison. However:

- **Tier 1** still does sequential ulong comparisons per candidate when multiple commands share the same `(count << 4 | length)` bucket — e.g., 3 commands at `(1<<4)|3`: GET, DEL, TTL
- **Tier 2** walks through deeply nested switch→switch→if-else chains — ~5-20 comparisons per command
- **Tier 3** does sequential `SequenceEqual` through ~80 entries — worst case ~80 comparisons for unknown commands
- **MakeUpperCase** is called on every non-fast-path command, using byte-by-byte scanning

Estimated cost: Tier 1 ≈ 5-15 cycles, Tier 2 ≈ 30-60 cycles, Tier 3 ≈ 100-300 cycles.

## Proposed Solution: SIMD-Accelerated Parsing + Cache-Friendly Hash Table

Two complementary optimizations:
1. **SIMD-based FastParseCommand** for the top ~20 commands using Vector128 full-pattern matching
2. **Cache-line-optimized hash table** with CRC32 intrinsic hashing and SIMD validation for all remaining ~170 commands

### Architecture

```
ParseCommand
  ├─ SimdFastParseCommand (REWRITE — Vector128 full 16-byte pattern match for top ~20 commands)
  │   ├─ Load first 16 bytes as Vector128
  │   ├─ Group candidates by total encoded length (13-byte, 14-byte, 15-byte groups)
  │   ├─ One AND (mask) per group + one Vector128.EqualsAll per candidate
  │   └─ ~3 ops per candidate vs. current ~8-10 ops
  │
  └─ On miss → ArrayParseCommand
      ├─ MakeUpperCase (REWRITE — SIMD Vector128/256 bulk conversion)
      ├─ SimdFastParseCommand retry (catches lowercase clients)
      ├─ Parse RESP array header
      └─ HashLookupCommand (NEW — replaces FastParseArrayCommand + SlowParseCommand)
          ├─ Extract command name (byte* + length)
          ├─ CRC32 hardware hash (single instruction via Sse42.Crc32 / Crc32.Arm)
          ├─ Index into cache-line-aligned table (L1-resident, 8-16KB)
          ├─ Validate via Vector128.EqualsAll (up to 16 bytes in one op)
          ├─ Linear probe within same cache line for collisions
          └─ If parent has subcommands → SubcommandHashLookup (same design)
```

### SIMD Design: FastParseCommand (Tier 1)

**Core idea**: Replace the two-step "load header ulong + load lastWord ulong" approach with a single Vector128 load that matches the FULL RESP-encoded pattern in one comparison.

For the top commands, the complete RESP encoding is:
```
GET:   *2\r\n$3\r\nGET\r\n   → 13 bytes total
SET:   *3\r\n$3\r\nSET\r\n   → 13 bytes total
DEL:   *2\r\n$3\r\nDEL\r\n   → 13 bytes total
TTL:   *2\r\n$3\r\nTTL\r\n   → 13 bytes total
PING:  *1\r\n$4\r\nPING\r\n  → 14 bytes total
INCR:  *2\r\n$4\r\nINCR\r\n  → 14 bytes total
HGET:  *3\r\n$4\r\nHGET\r\n  → 14 bytes total
HSET:  *4\r\n$4\r\nHSET\r\n  → 14 bytes total
MGET:  *2\r\n$4\r\nMGET\r\n  → 14 bytes total
ZADD:  *4\r\n$4\r\nZADD\r\n  → 14 bytes total
```

**Algorithm**:
```csharp
if (remainingBytes >= 16)
{
    var input = Vector128.LoadUnsafe(ref Unsafe.AsRef<byte>(ptr));

    // Group 1: 13-byte patterns (3-char command names)
    // Mask out bytes 13-15 (they contain the next argument, not part of command)
    var masked13 = Vector128.BitwiseAnd(input, Mask13);

    // Each comparison checks FULL RESP header + command name in ONE op
    // Pattern includes *N\r\n$3\r\nXXX\r\n — the N distinguishes arg count
    if (masked13 == PatternGET)  { readHead += 13; count = 1; return RespCommand.GET; }
    if (masked13 == PatternSET)  { readHead += 13; count = 2; return RespCommand.SET; }
    if (masked13 == PatternDEL)  { readHead += 13; count = 1; return RespCommand.DEL; }
    if (masked13 == PatternTTL)  { readHead += 13; count = 1; return RespCommand.TTL; }

    // Group 2: 14-byte patterns (4-char command names)
    var masked14 = Vector128.BitwiseAnd(input, Mask14);
    if (masked14 == PatternPING) { readHead += 14; count = 0; return RespCommand.PING; }
    if (masked14 == PatternINCR) { readHead += 14; count = 1; return RespCommand.INCR; }
    if (masked14 == PatternHGET) { readHead += 14; count = 2; return RespCommand.HGET; }
    if (masked14 == PatternHSET) { readHead += 14; count = 3; return RespCommand.HSET; }
    // ... more 14-byte patterns

    // Group 3: 15-byte patterns (5-char command names)
    var masked15 = Vector128.BitwiseAnd(input, Mask15);
    if (masked15 == PatternLPUSH) { readHead += 15; count = 1; return RespCommand.LPUSH; }
    // ...
}
```

**Why this is faster**: Each comparison verifies the ENTIRE command encoding (header + name + terminators) in a single `Vector128.EqualsAll` (~1 cycle), vs. the current approach which needs 2 ulong loads + 2 comparisons + count/length extraction (~8-10 ops). The mask is computed once per length group. Pattern vectors are `static readonly` fields, resolved at JIT time.

**Limitation**: Only works for single-digit array length and single-digit command name length (covers commands with 1-9 args and 1-9 char names). This covers the top ~60 commands. Longer/rarer commands fall through to the hash table.

### Cache-Friendly Hash Table Design (Tier 2+3 Replacement)

**Key constraint**: Single-threaded access, so no concurrency overhead. Must be as fast as possible.

**Entry structure** — 32 bytes, exactly half a cache line:
```csharp
[StructLayout(LayoutKind.Explicit, Size = 32)]
struct CommandEntry
{
    [FieldOffset(0)]  public RespCommand Command;    // 2 bytes (ushort)
    [FieldOffset(2)]  public byte NameLength;        // 1 byte
    [FieldOffset(3)]  public byte Flags;             // 1 byte (HasSubcommands, etc.)
    [FieldOffset(4)]  public int Reserved;           // 4 bytes (alignment padding)
    [FieldOffset(8)]  public ulong NameWord0;        // First 8 bytes of uppercase name
    [FieldOffset(16)] public ulong NameWord1;        // Bytes 8-15 (zero-padded)
    [FieldOffset(24)] public ulong NameWord2;        // Bytes 16-23 (zero-padded)
}
```

**Table layout**:
- Size: 256 entries (power of 2) for ~180 primary commands → ~70% load factor
- Total memory: 256 × 32 bytes = **8 KB** — fits entirely in L1 cache (typically 32-64KB)
- Two entries per 64-byte cache line → linear probing hits same cache line for 1st probe
- `GC.AllocateArray<CommandEntry>(256, pinned: true)` for zero-GC, pointer-stable access

**Hash function** — Hardware CRC32 (single instruction):
```csharp
[MethodImpl(MethodImplOptions.AggressiveInlining)]
static uint ComputeHash(byte* name, int length)
{
    // Read first 8 bytes (zero-extended for short names)
    ulong word0 = length >= 8
        ? *(ulong*)name
        : ReadPartialWord(name, length);

    // Hardware CRC32 — single-cycle instruction on x86 (SSE4.2) and ARM
    if (Sse42.IsSupported)
        return Sse42.Crc32(Sse42.Crc32(0u, word0), (uint)length);

    if (System.Runtime.Intrinsics.Arm.Crc32.IsSupported)
        return System.Runtime.Intrinsics.Arm.Crc32.ComputeCrc32C(
            System.Runtime.Intrinsics.Arm.Crc32.ComputeCrc32C(0u, word0), (uint)length);

    // Software fallback: Fibonacci multiply-shift
    return (uint)((word0 * 0x9E3779B97F4A7C15UL) >> 32) ^ (uint)(length * 2654435761U);
}
```

**Probe with SIMD validation**:
```csharp
[MethodImpl(MethodImplOptions.AggressiveInlining)]
static RespCommand Lookup(byte* name, int length)
{
    uint hash = ComputeHash(name, length);
    int idx = (int)(hash & (TableSize - 1));  // Power-of-2 mask, no modulo

    // Linear probe — typically 1-2 iterations, within same cache line
    for (int probe = 0; probe < MaxProbes; probe++)
    {
        ref CommandEntry entry = ref table[idx];

        // Empty slot → command not found
        if (entry.NameLength == 0) return RespCommand.NONE;

        // Length mismatch → skip (single byte compare, very fast rejection)
        if (entry.NameLength != (byte)length) { idx = (idx + 1) & (TableSize - 1); continue; }

        // SIMD validation: Compare up to 16 bytes of command name in ONE operation
        if (length <= 8)
        {
            // Single ulong compare for short names (most commands)
            ulong word0 = ReadPartialWord(name, length);
            if (entry.NameWord0 == word0) return entry.Command;
        }
        else
        {
            // Vector128 compare for names 9-16 bytes (SUBSCRIBE, ZRANGEBYSCORE, etc.)
            var input = Vector128.Create(*(ulong*)name, *(ulong*)(name + length - 8));
            var expected = Vector128.Create(entry.NameWord0, entry.NameWord1);
            if (Vector128.EqualsAll(input.AsByte(), expected.AsByte()))
                return entry.Command;
        }

        idx = (idx + 1) & (TableSize - 1);
    }
    return RespCommand.NONE;
}
```

**Why this is fast**:
- **Hash**: 1 CRC32 instruction (~3 cycles on x86/ARM)
- **Index**: 1 AND instruction (~1 cycle)
- **Load entry**: 1 memory load from L1 (~4 cycles, cache-warm)
- **Length check**: 1 byte compare (~1 cycle, fast rejection for 90%+ of misses)
- **Name validate**: 1 ulong compare for names ≤8 bytes (most commands), or 1 Vector128 compare for longer names
- **Total**: ~10-12 cycles for a hit, ~8 cycles for a miss
- **Linear probe**: 2nd probe hits same cache line (32-byte entries, 64-byte cache line)

### SIMD Case Conversion (MakeUpperCase)

Replace the current byte-by-byte loop with bulk SIMD conversion:
```csharp
// Detect lowercase bytes: compare against 'a' and 'z' ranges
var lower_a = Vector128.Create((byte)('a' - 1));  // 0x60
var upper_z = Vector128.Create((byte)'z');          // 0x7A
var caseBit = Vector128.Create((byte)0x20);

var input = Vector128.LoadUnsafe(ref Unsafe.AsRef<byte>(ptr));

// Find bytes in range 'a'-'z' using saturating subtract technique
var aboveA = Vector128.GreaterThan(input, lower_a);
var belowZ = Vector128.LessThanOrEqual(input, upper_z);
var isLower = Vector128.BitwiseAnd(aboveA, belowZ);

if (isLower != Vector128<byte>.Zero)
{
    // Clear bit 5 (0x20) for lowercase bytes only
    var toSubtract = Vector128.BitwiseAnd(isLower, caseBit);
    var result = Vector128.Subtract(input, toSubtract);
    result.StoreUnsafe(ref Unsafe.AsRef<byte>(ptr));
    return true;  // Modified
}
return false;  // Already uppercase
```

This converts 16 bytes at once (32 with Vector256). Most RESP command headers are 12-20 bytes, so one or two SIMD operations cover the entire command.

### Subcommand Hash Lookup

Commands with subcommands (CLUSTER ~40, CLIENT 8, ACL 10, CONFIG 3, COMMAND 5, SCRIPT 3, LATENCY 3, SLOWLOG 4, MODULE 1, PUBSUB 3, MEMORY 1 — total ~80 subcommands across 12 parents) use the same hash table design but smaller:
- CLUSTER: 64-entry table (~40 subcommands, 62% load)
- Others: 16 or 32-entry tables
- Same CRC32 hash + linear probe, same 32-byte entries
- Each parent's `Flags` field indicates `HasSubcommands`

### Expected Performance Impact

| Command Type | Current Cost | New Cost | Speedup |
|-------------|-------------|---------|---------|
| GET, SET (SIMD fast tier) | ~8-10 ops | ~3-4 ops (Vector128 match) | 2-3x |
| PING, INCR (SIMD fast tier) | ~10-12 ops | ~5-6 ops (2nd group match) | 2x |
| HSET, ZADD, LPUSH (Tier 2) | ~30-60 cycles | ~10-12 cycles (hash) | 3-5x |
| CLUSTER INFO, ACL LIST (Tier 3) | ~100-300 cycles | ~15-20 cycles (2 hash lookups) | 7-15x |
| Unknown commands | ~300+ cycles | ~8 cycles (hash miss) | 40x+ |

### Risk Mitigation

- All ~2000 existing tests serve as regression suite
- The hash table is built at static init time and is read-only — no concurrency issues
- The SIMD patterns are statically verified at build time
- Fallback paths exist for all SIMD operations (non-SIMD hardware)
- The hash table construction can assert zero collisions at startup

## Implementation Todos

### Phase 1: Hash Table Infrastructure
1. **build-hash-table** — Create `RespCommandHashLookup` static class in `libs/server/Resp/Parser/RespCommandHashLookup.cs`:
   - 32-byte `CommandEntry` struct with `StructLayout(Explicit)`
   - Pinned GC array for zero-GC pointer-stable access
   - CRC32 intrinsic hash with Sse42/Arm.Crc32/software fallback
   - `Lookup(byte* name, int length)` with linear probing + SIMD validation
   - Static constructor populates all ~180 primary commands
   - Assert zero unresolved collisions at init (fail-fast if hash function degrades)

2. **build-subcommand-tables** — Per-parent subcommand hash tables (same design, smaller):
   - CLUSTER (64 entries), CLIENT/ACL/COMMAND (16-32 entries), others (16 entries)
   - `LookupSubcommand(RespCommand parent, byte* name, int length)` method
   - Preserve specific error messages for unknown subcommands

### Phase 2: SIMD Fast Tier
3. **simd-fast-parse** — Rewrite `FastParseCommand` to use Vector128 pattern matching:
   - Pre-build Vector128 patterns for top ~20-30 commands (static readonly fields)
   - Group by total encoded byte length (13, 14, 15, 16+ bytes)
   - One mask per group, one Vector128.EqualsAll per candidate
   - Extract count and readHead advance from the pattern metadata
   - Preserve inline command handling (PING/QUIT without array framing)

### Phase 3: Integration
4. **replace-array-parse** — Modify `ArrayParseCommand` to use hash lookup:
   - After parsing RESP array header, extract command name (bytes + length)
   - Call `RespCommandHashLookup.Lookup()` replacing FastParseArrayCommand + SlowParseCommand
   - For `HasSubcommands` results, extract subcommand and do second lookup
   - Delete or gut `FastParseArrayCommand` (~950 lines) and simplify `SlowParseCommand` (~800 lines)

5. **handle-special-commands** — Preserve special-case behavior:
   - BITOP + pseudo-subcommands (AND/OR/XOR/NOT/DIFF): after hash identifies BITOP, parse the operator subcommand inline or via subcommand hash
   - Custom commands via TryParseCustomCommand: checked before hash lookup, as currently done
   - SET variants (SETEXNX etc.): keep in SIMD fast tier via separate patterns for different arg counts

### Phase 4: SIMD Case Conversion
6. **simd-uppercase** — Replace `MakeUpperCase` with SIMD version:
   - Vector128 (SSE2/AdvSimd): 16 bytes at a time
   - Vector256 (AVX2): 32 bytes at a time when available
   - Keep existing two-ulong fast-check as the first gate (already uppercase → skip)
   - Falls back to scalar loop for remaining bytes

### Phase 5: Validation
7. **run-tests** — Full regression testing:
   - `dotnet test test/Garnet.test -f net10.0 -c Debug`
   - Focus on `RespTests`, `RespCommandTests`, ACL tests, Cluster tests
   - Verify command parsing identity for all commands (every enum value must be reachable)

8. **benchmark** — Performance validation:
   - Microbenchmark each tier: top commands, moderate commands, rare commands, unknown commands
   - Compare cycle counts before/after using BenchmarkDotNet
   - Profile branch misprediction rates and cache miss rates

## Files to Modify

- `libs/server/Resp/Parser/RespCommand.cs` — Rewrite FastParseCommand (SIMD), replace FastParseArrayCommand + SlowParseCommand with hash lookup calls
- `libs/server/Resp/RespServerSession.cs` — SIMD MakeUpperCase
- **New file**: `libs/server/Resp/Parser/RespCommandHashLookup.cs` — Hash table + subcommand tables
