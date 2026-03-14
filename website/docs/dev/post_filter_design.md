# VSIM Post-Filter — Design Document

## Overview

The VSIM post-filter evaluates user-supplied filter expressions (e.g. `.year > 1980 and .genre == "action"`) against vector search candidate results. It runs in the hot path of every filtered vector similarity query.

**Key design constraint:** Zero heap allocation in the per-candidate evaluation loop. All buffers are borrowed from the session-local `ScratchBufferBuilder` (~9 KB), a pinned `byte[]` that persists for the session's lifetime. After the first VSIM FILTER query, the buffer is already large enough — subsequent calls have zero allocation cost and zero GC pressure.

---

## High-Level Architecture

```
 VSIM SEARCH REQUEST
 ┌─────────────────────────────────────────────────────────────────────┐
 │  VSIM.SEARCH myindex 3 FILTER ".year > 1980 and .genre == 'action'"│
 └──────────────────────────────────┬──────────────────────────────────┘
                                    │
                                    ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │  Vector Index (DiskANN)                                              │
 │  Returns top-K nearest neighbors by vector distance                 │
 │  e.g. 100 candidates for K=3 (over-fetch to allow post-filtering)  │
 └──────────────────────────────────┬──────────────────────────────────┘
                                    │ candidates + JSON attributes
                                    ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │  ApplyPostFilter (this pipeline)                                    │
 │                                                                      │
 │  Stage 1: COMPILE filter → postfix program     (once per query)     │
 │  Stage 2: COLLECT unique field selectors        (once per query)     │
 │  Stage 3: For each candidate:                   (×N, zero alloc)    │
 │           EXTRACT fields → EVALUATE program → SET bitmap bit        │
 └──────────────────────────────────┬──────────────────────────────────┘
                                    │ filterBitmap (packed bit array)
                                    ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │  Return top-K results that passed the filter                        │
 └──────────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Stages

### Stage 1: Compile (ExprCompiler)

Transforms the filter expression UTF-8 bytes into a flat postfix instruction array using the shunting-yard algorithm.

```
 Input:   ".year > 1980 and .genre == \"action\""   (raw UTF-8 bytes)

 Phase 1 — Tokenize:
   [SEL:year] [NUM:1980] [OP:>] [OP:and] [SEL:genre] [STR:"action"] [OP:==]

 Phase 2 — Shunting-yard (infix → postfix):
   [SEL:year] [NUM:1980] [OP:Gt] [SEL:genre] [STR:"action"] [OP:Eq] [OP:And]
```

**Key properties:**
- Zero heap allocation — compiler uses caller-provided Span buffers (borrowed from session scratch buffer) for all scratch and output
- String/selector tokens store `(offset, length)` byte-range references into the original filter span — no copies
- Numbers parsed via `Utf8Parser.TryParse` directly from UTF-8 bytes
- Booleans normalized to `double`: `true` → 1.0, `false` → 0.0

### Stage 2: Selector Collection (GetSelectorRanges)

Scans the compiled instructions to find unique field selectors.

```
 Instructions: [SEL:year] [NUM:1980] [OP:Gt] [SEL:genre] [STR:"action"] [OP:Eq] [OP:And]
                    ↓                              ↓
 selectorRanges: [(offset=1, len=4), (offset=23, len=5)]
                      "year"              "genre"
```

Deduplication ensures each field is extracted only once, even if referenced multiple times in the filter.

### Stage 3: Per-Candidate Evaluation (×N, zero alloc)

For each candidate JSON document:

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │ 3a. EXTRACT — AttributeExtractor.ExtractFields                     │
 │                                                                     │
 │   JSON:  {"year":1980,"rating":4.5,"genre":"action",...}            │
 │                 ↓                         ↓                         │
 │   extractedFields[0] = ExprToken{Num=1980.0}       ← .year         │
 │   extractedFields[1] = ExprToken{Str=(32,6)}       ← .genre        │
 │                                                                     │
 │   Single-pass scan: walks JSON once, extracts ALL needed fields.    │
 │   String values are zero-copy (offset, length) into JSON bytes.     │
 │   Early exit when all requested fields are found.                   │
 ├─────────────────────────────────────────────────────────────────────┤
 │ 3b. EVALUATE — ExprRunner.Run                                      │
 │                                                                     │
 │   Walk postfix instructions left-to-right:                          │
 │                                                                     │
 │   Instruction         Stack (after)                                 │
 │   ────────────────    ──────────────────────────────────            │
 │   [SEL:year]          [1980.0]                  ← lookup in extractedFields │
 │   [NUM:1980]          [1980.0, 1980.0]                              │
 │   [OP:Gt]             [0.0]                     ← 1980 > 1980 = false      │
 │   [SEL:genre]         [0.0, "action"]                               │
 │   [STR:"action"]      [0.0, "action", "action"]                     │
 │   [OP:Eq]             [0.0, 1.0]               ← "action"=="action"= true  │
 │   [OP:And]            [0.0]                     ← false AND true = false    │
 │                                                                     │
 │   Top-of-stack = 0.0 → candidate EXCLUDED                          │
 ├─────────────────────────────────────────────────────────────────────┤
 │ 3c. RECORD — set filterBitmap[i/8] |= (1 << (i%8)) if passed      │
 └─────────────────────────────────────────────────────────────────────┘
```

---

## Memory Architecture

### Design Principle: Caller Owns All Memory

```
 VectorManager.Filter (ApplyPostFilter)
    │
    │ scratch = ActiveThreadSession.scratchBufferBuilder
    │ poolSlice = scratch.CreateArgSlice(560 × 16 bytes)
    │ selectorSlice = scratch.CreateArgSlice(32 × 8 bytes)
    │ Cast to Span<ExprToken> and Span<(int,int)> via MemoryMarshal
    │ Slice into sub-spans:
    │   instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf,
    │   runtimePoolBuf, extractedFields, stackBuf
    │
    ├──→ ExprCompiler.TryCompile(filter, instrBuf, tuplePoolBuf,
    │                            tokensBuf, opsStackBuf, ...)
    │         │
    │         │ writes into instrBuf, tuplePoolBuf
    │         │ uses tokensBuf, opsStackBuf as scratch
    │         │ OWNS NOTHING — pure function over borrowed Spans
    │         │
    │ ←───────┘
    │
    ├──→ ExprProgram (ref struct)
    │         │
    │         │ Instructions = instrBuf[..instrCount]    ← Span slice
    │         │ TuplePool    = tuplePoolBuf[..tupleCount]← Span slice
    │         │ RuntimePool  = runtimePoolBuf            ← Span
    │         │ OWNS NOTHING — just bundles Span references
    │         │
    ├──→ AttributeExtractor.ExtractFields(...)
    │         │ OWNS NOTHING
    │         │
    ├──→ ExprRunner.Run(...)
    │         │ OWNS NOTHING
    │         │
    └──→ finally: scratch.RewindScratchBuffer(ref selectorSlice)
                  scratch.RewindScratchBuffer(ref poolSlice)  ← LIFO
```

### Buffer Layout — Session Scratch Buffer

```
 ScratchBufferBuilder (session-local pinned byte[])
 ┌──────────────────────────────────────────────────────────────┐
 │  poolSlice = CreateArgSlice(560 × 16 = 8,960 bytes):    │
 │                                                              │
 │  ┌──────────────────────────────────────────────┐            │
 │  │ instrBuf         128 × 16 B = 2,048 B        │ ← compiled│
 │  │ tuplePoolBuf      64 × 16 B = 1,024 B        │   output  │
 │  ├──────────────────────────────────────────────┤            │
 │  │ tokensBuf        128 × 16 B = 2,048 B        │ ← compiler│
 │  │ opsStackBuf      128 × 16 B = 2,048 B        │   scratch │
 │  ├──────────────────────────────────────────────┤            │
 │  │ runtimePoolBuf    64 × 16 B = 1,024 B        │ ← runtime │
 │  │ extractedFields   32 × 16 B =   512 B        │   data    │
 │  │ stackBuf          16 × 16 B =   256 B        │           │
 │  └──────────────────────────────────────────────┘            │
 │                                                              │
 │  selectorSlice = CreateArgSlice(32 × 8 = 256 bytes):        │
 │  ┌──────────────────────────────────────────────┐            │
 │  │ selectorBuf       32 ×  8 B =   256 B        │            │
 │  └──────────────────────────────────────────────┘            │
 │                                                              │
 │  On the stack: only ExprProgram + ExprStack ref structs       │
 │  and local variables (~64 B).                                 │
 └──────────────────────────────────────────────────────────────┘

 Cleanup: RewindScratchBuffer in LIFO order in the finally block.
 The scratch buffer is never freed — it persists for the session's lifetime.
 After the first VSIM FILTER query, it's already large enough.
```

**Why ScratchBufferBuilder instead of ArrayPool or stackalloc?**
- **Session-local, already warm** — the pinned `byte[]` is already allocated and in cache
  from RESP command parsing earlier in the same session
- **Zero contention** — no shared pool lock (even lock-free CAS has overhead under
  high thread counts); scratch buffer is strictly thread-local
- **Zero stack pressure** — no risk of stack overflow from ~9 KB of stackalloc
- **Ref safety** — C# ref safety rules (CS8350/CS8352) prohibit mixing stackalloc'd
  spans with heap-backed spans when passing them to methods that take `ref struct`
  parameters. Using the scratch buffer avoids this entirely.
- **Consistent with Garnet idiom** — other Vector Set operations (FetchAttributes, etc.)
  already use the same `ScratchBufferBuilder` pattern

### ExprToken — The Universal Data Type (16 bytes)

Every value in the system — numbers, strings, operators, selectors, tuples, nulls — is represented as a single 16-byte `ExprToken` struct, laid out as a tagged union:

```
 ExprToken — [StructLayout(LayoutKind.Explicit, Size = 16)]

 Byte offset:  0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15
              ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐
              │Type│ Op │Flag│         padding (5 bytes)   │       Union payload (8 bytes)     │
              │Code│Code│  s │                             │                                   │
              └────┴────┴────┴────────────────────────────┴───────────────────────────────────┘
              ◄─── header (8 bytes) ──────────────────────►◄─── payload (8 bytes) ───────────►

 The payload at offset 8 is a UNION (overlapping fields):
   ● double Num       [8..15]  — 8 bytes, for numbers and booleans
   ● int Utf8Start    [8..11]  — 4 bytes ┐ for strings, selectors, tuples
   ● int Utf8Length   [12..15] — 4 bytes ┘
```

#### TokenType — The Discriminator (byte 0)

The `TokenType` at byte 0 is the **discriminator tag** that tells the system which kind of value this token represents and which payload fields are valid. Without it, the union payload is ambiguous — the same 8 bytes could be a `double` or a pair of `int`s.

```
 ExprTokenType enum (1 byte):

 Value  Name       What it represents                   Payload used
 ─────  ─────────  ───────────────────────────────────  ─────────────────────────
   0    None       Default/uninitialized (all zeros)    (none — sentinel value)
   1    Num        Numeric literal or boolean            double Num
   2    Str        String value (byte-range reference)   int Utf8Start + Utf8Length
   3    Tuple      Collection of values (for IN op)      int poolStart + count
   4    Selector   JSON field name (e.g. .year)          int Utf8Start + Utf8Length
   5    Op         Operator (e.g. >, ==, and)            OpCode in byte 1
   6    Null       JSON null or missing value             (none)
```

**Why we need it:**
- The postfix instruction array is a flat `Span<ExprToken>`. The evaluator walks it left-to-right and must know how to handle each token: push a value? look up a field? execute an operation?
- `TokenType` answers that question in a single byte comparison — no virtual dispatch, no type checks, no polymorphism overhead.
- The `None` value (0) is critical: since all stackalloc buffers start as zeros, any uninitialized slot is automatically `None`, which is detectable via `token.IsNone`.

#### OpCode — The Operator Identity (byte 1)

The `OpCode` at byte 1 identifies which operation to perform when `TokenType == Op`. It's only meaningful for operator tokens — for all other token types, byte 1 is unused.

```
 OpCode enum (1 byte):

 Value  Name     Arity  Prec  Meaning              Example
 ─────  ───────  ─────  ────  ───────────────────── ────────────────────
   0    Or         2     0    Logical OR             .a > 1 or .b > 2
   1    And        2     1    Logical AND            .a > 1 and .b > 2
   2    Gt         2     2    Greater than           .year > 1980
   3    Gte        2     2    Greater or equal       .rating >= 4.0
   4    Lt         2     2    Less than              .year < 2000
   5    Lte        2     2    Less or equal          .rating <= 5.0
   6    Eq         2     2    Equality               .genre == "action"
   7    Neq        2     2    Not equal              .genre != "drama"
   8    In         2     2    Membership / contains  "x" in .tags
   9    Add        2     3    Addition               .year + 10
  10    Sub        2     3    Subtraction            .year - 5
  11    Mul        2     4    Multiplication         .rating * 2
  12    Div        2     4    Division               .budget / 1000000
  13    Mod        2     4    Modulo                 .year % 100
  14    Pow        2     5    Power (right-assoc)    (.year - 2000) ** 2
  15    Not        1     6    Logical NOT            not (.deleted)
  16    OParen     0     7    Open parenthesis       (  ← compile-time only
  17    CParen     0     7    Close parenthesis      )  ← compile-time only
```

**Why we need it:**

1. **During compilation (shunting-yard):** The `OpCode` determines **precedence** and **associativity**. The shunting-yard algorithm uses precedence to decide when to pop operators from the stack to the output. For example, `*` (prec 4) binds tighter than `+` (prec 3), so `.rating * 2 + 1` correctly compiles to `[.rating] [2] [*] [1] [+]`.

2. **During evaluation:** The `OpCode` tells `ExecuteInstruction` which operation to perform. The **arity** (1 or 2) determines how many operands to pop from the evaluation stack. For example, `Not` pops 1 operand, `And` pops 2.

3. **OParen/CParen are compile-time only:** They exist in the `OpCode` enum so the shunting-yard algorithm can push/pop them, but they are never emitted to the output instruction array. If a `CParen` is found in the output, it means unbalanced parentheses — compile error.

#### How TokenType and OpCode Work Together

```
 Example: .year > 1980 and .genre == "action"

 Compiled postfix instruction array (7 ExprTokens, 112 bytes):

 Index  TokenType   OpCode    Payload              What happens at eval time
 ─────  ──────────  ────────  ───────────────────  ──────────────────────────
   0    Selector    (unused)  Utf8Start=1, Len=4   Push extractedFields["year"]
   1    Num         (unused)  Num=1980.0            Push 1980.0
   2    Op          Gt        (unused)              Pop 2, push (1980 > 1980 = 0.0)
   3    Selector    (unused)  Utf8Start=22, Len=5   Push extractedFields["genre"]
   4    Str         (unused)  Utf8Start=31, Len=6   Push ref to "action" in filter
   5    Op          Eq        (unused)              Pop 2, push ("action"=="action" = 1.0)
   6    Op          And       (unused)              Pop 2, push (0.0 AND 1.0 = 0.0)

 → Top of stack = 0.0 → candidate EXCLUDED
```

The evaluator's dispatch is a two-level check:
1. **Check TokenType** — is this a value to push, a selector to look up, or an operator to execute?
2. **If Op, check OpCode** — which specific operation? How many operands?

This is intentionally flat and branchless-friendly. No virtual method tables, no interface dispatch — just byte comparisons and a switch statement that the JIT can optimize into a jump table.

**Flags byte (offset 2) — 3 bits used:**

| Bit | Mask | Name | Meaning |
|-----|------|------|---------|
| 0 | `0x01` | HasEscape | String contains `\"`, `\\`, `\n` — needs unescape-aware comparison |
| 1 | `0x02` | FilterOrigin | Byte range references the filter buffer, not the JSON buffer |
| 2 | `0x04` | RuntimeTuple | Tuple elements are in RuntimePool (from JSON), not TuplePool (from `[...]` literal) |
| 3–7 | — | Reserved | Available for future use |

**Payload by token type:**

| TokenType | Payload | Source buffer |
|-----------|---------|---------------|
| Num | `double Num` (booleans: 1.0/0.0) | — |
| Str | `(Utf8Start, Utf8Length)` byte range | JSON bytes (default), or filter bytes if FilterOrigin |
| Selector | `(Utf8Start, Utf8Length)` field name | filter bytes (by convention; FilterOrigin is NOT set — identified by TokenType) |
| Tuple | `(poolStart, count)` into pool | TuplePool (compile-time) or RuntimePool (if RuntimeTuple) |
| Op | `OpCode` enum in header byte 1 | — |
| Null | (no payload) | — |
| None | all zeros — default/sentinel | — |

**Why 16 bytes?**
- Power-of-two: array indexing is `index << 4` (single shift instruction)
- Cache-friendly: 4 tokens fit in one 64-byte cache line
- The `double` payload requires 8-byte alignment
- No managed references → safe for `stackalloc`, `Span<T>`, and contiguous buffers

### ExprProgram — Zero-Allocation View (ref struct)

`ExprProgram` is a `ref struct` that bundles `Span<ExprToken>` references into slices of the caller's pooled array. It owns no memory — it's a view.

```csharp
internal ref struct ExprProgram
{
    public Span<ExprToken> Instructions;     // → instrBuf[..instrCount]
    public int Length;
    public Span<ExprToken> TuplePool;        // → tuplePoolBuf[..tupleCount]
    public int TuplePoolLength;
    public Span<ExprToken> RuntimePool;      // → runtimePoolBuf
    public int RuntimePoolLength;
}
```

Because it's a `ref struct`, it can only live on the stack — the compiler enforces this. It cannot be stored in a field, captured by a lambda, or boxed.

### ExprStack — Zero-Allocation Evaluation Stack (ref struct)

```csharp
internal ref struct ExprStack
{
    private readonly Span<ExprToken> _buffer;  // → stackBuf
    private int _count;

    public bool IsFull;                        // bounds check helper
    public bool TryPush(ExprToken t);          // bounds-checked, returns false on overflow
    public ExprToken Pop();
    public ExprToken Peek();
    public void Clear();
}
```

### Zero-Copy String Handling

Strings are never allocated or copied. Instead, `ExprToken` stores `(offset, length)` byte-range references:

```
 Filter: ".genre == \"action\""
          0123456789...
          ↑               ↑
 Selector token:  Utf8Start=1,  Utf8Length=5   → "genre"  (into filter bytes)
 String literal:  Utf8Start=11, Utf8Length=6   → "action" (into filter bytes, FilterOrigin=true)

 JSON:   {"genre":"action","year":1980}
          0123456789...
          ↑         ↑
 Extracted string:  Utf8Start=9,  Utf8Length=6  → "action" (into JSON bytes, FilterOrigin=false)
```

When comparing, `ExprRunner.GetStrSpan` resolves the correct source buffer:

```csharp
return t.IsFilterOrigin
    ? filterBytes.Slice(t.Utf8Start, t.Utf8Length)
    : json.Slice(t.Utf8Start, t.Utf8Length);
```

For strings without escapes: `SequenceEqual` (JIT-vectorized with SIMD in .NET 8+).
For strings with escapes (`HasEscape` flag): `UnescapedEquals` (on-the-fly unescape, still zero allocation).

---

## Overflow Safety

Every buffer write is bounds-checked. The system never crashes on pathological input — it fails gracefully:

| Buffer | Capacity | On overflow |
|--------|----------|-------------|
| `instrBuf` | 128 tokens | `TryCompile` returns -1 → all candidates filtered out |
| `tuplePoolBuf` | 64 elements | `TryCompile` returns -1 → compile error |
| `tokensBuf` | 128 tokens | `TryCompile` returns -1 → compile error |
| `opsStackBuf` | 128 entries | `ProcessOperator` returns false → compile error |
| `runtimePoolBuf` | 64 elements | `ParseArrayToken` returns Null → array skipped |
| `selectorBuf` | 32 selectors | Extra selectors silently not collected |
| `extractedFields` | 32 fields | Sliced to actual selector count (max 32) |
| `stackBuf` | 16 depth | `TryPush` returns false → candidate excluded |

---

## Component Responsibilities

| Component | Responsibilities | Owns memory? |
|-----------|-----------------|--------------|
| `VectorManager.Filter` | borrow scratch buffer, slice into sub-spans, orchestrate pipeline, rewind on exit | **Yes** — single owner via ScratchBufferBuilder |
| `ExprCompiler` | tokenize + shunting-yard → postfix instructions | **No** — writes to caller's spans |
| `ExprProgram` | bundle Span references into a convenient struct | **No** — ref struct, just a view |
| `ExprRunner` | walk instructions, evaluate postfix program | **No** — reads program + stack spans |
| `AttributeExtractor` | single-pass JSON field extraction | **No** — writes to caller's spans |
| `ExprStack` | push/pop evaluation stack | **No** — ref struct over caller's span |
| `ExprToken` | universal 16-byte tagged union data type | **No** — blittable value type |

---

## VSIM EF Parameters and How They Drive the Filter Pipeline

Three `EF` parameters control how many candidates the post-filter processes:

```
 VADD key FP32 <vector> element EF 200
                                ^^^^^^
                                Build exploration factor — controls index quality at insert time.

 VSIM key FP32 <query> element COUNT 10 EF 100 FILTER ".year > 1980" FILTER-EF 200
                                        ^^^^^^                       ^^^^^^^^^^^
                                        Search EF — beam width       Filter EF — extra effort
                                        during graph traversal.       to compensate for filtering.
```

### Parameter Details

| Parameter | Command | Type | Default | Range | What it controls |
|-----------|---------|------|---------|-------|-----------------|
| `EF` | `VADD` | int | index-dependent | > 0 | Build exploration factor. How many neighbors the index probes when inserting a new vector. Higher = better graph quality, slower inserts. |
| `EF` | `VSIM` | int | index-dependent | ≥ 0 | Search exploration factor (beam width). How many candidates the graph algorithm visits during search. Must be ≥ COUNT. Higher = better recall, more compute. |
| `FILTER-EF` | `VSIM` | int | COUNT×200 | ≥ 0 | How many candidates to retrieve upfront when FILTER is specified. The index over-fetches this many candidates in a single search pass, then post-filter runs once. No retry loop. |

### How EF and FILTER-EF Affect the Post-Filter

When a `FILTER` is specified, `FILTER-EF` controls how many candidates the index retrieves
in a **single, upfront over-fetch** — there is no retry loop.

```
 VSIM myindex FP32 <query> element COUNT 10 EF 50 FILTER ".genre == 'action'" FILTER-EF 200

 Without FILTER:
   retrieveCount = COUNT = 10
   effectiveEF   = EF = 50
   → Index returns 10 candidates, no post-filter.

 With FILTER:
   retrieveCount = FILTER-EF = 200            ← over-fetch upfront
   effectiveEF   = max(EF, FILTER-EF) = 200   ← widen graph search
   → Index returns up to 200 candidates in ONE search.
   → ApplyPostFilter runs ONCE on all 200 candidates.
   → Enough pass the filter to fill COUNT=10.
```

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │ Step 1: Graph search with effectiveEF = max(EF, FILTER-EF) = 200   │
 │                                                                      │
 │   Index traverses the graph with beam width 200.                    │
 │   Returns up to FILTER-EF (200) candidates sorted by distance.     │
 │                                                                      │
 │   Result: e.g. 200 candidates                                      │
 └──────────────────────┬───────────────────────────────────────────────┘
                        │
                        ▼
 ┌──────────────────────────────────────────────────────────────────────┐
 │ Step 2: ApplyPostFilter — single pass, zero GC allocation            │
 │                                                                      │
 │   Evaluates ".genre == 'action'" against all 200 candidates.       │
 │   Uses ~9 KB from session scratch buffer (contiguous, cache-warm). │
 │                                                                      │
 │   Result: e.g. 60 out of 200 pass → filterBitmap with 60 bits set  │
 │   Caller picks top COUNT=10 by distance from the passing set.      │
 └──────────────────────────────────────────────────────────────────────┘

 Note: there is NO retry loop. The index search and post-filter each
 run exactly once. FILTER-EF simply inflates the initial retrieval.
 Default FILTER-EF = COUNT * 200 (e.g. COUNT=10 → FILTER-EF=2000).
```

### EF/FILTER-EF vs Post-Filter Cost

| Scenario | Candidates evaluated | Pool usage | Heap alloc |
|----------|---------------------|------------|------------|
| COUNT=10, EF=50, no FILTER | 0 (no filter) | 0 B | 0 B |
| COUNT=10, EF=50, FILTER, FILTER-EF=100 | up to 100 | ~9 KB (pooled) | **0 B** |
| COUNT=10, EF=100, FILTER, FILTER-EF=200 | up to 200 | ~9 KB (pooled) | **0 B** |
| COUNT=10, EF=50, FILTER (default) | up to 2000 (COUNT×200) | ~9 KB (pooled) | **0 B** |
| COUNT=100, EF=500, FILTER, FILTER-EF=5000 | up to 5000 | ~9 KB (pooled) | **0 B** |

The scratch buffer usage is constant (~9 KB) regardless of how many candidates are evaluated — the buffers are borrowed once and reused across all candidates in the loop. The session's scratch buffer persists for its lifetime, so repeated queries reuse the same physical memory.

