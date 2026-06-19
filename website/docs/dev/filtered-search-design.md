# Filtered Vector Search — End-to-End Design Document

## 1. Motivation

Garnet's vector search (`VSIM` command family) supports similarity search over DiskANN graph indexes. Users frequently need to combine similarity search with metadata filtering (e.g., "find the 10 nearest images where `year > 2020 AND genre IN ['action', 'comedy']`").

### Problem with post-filtering

The naive approach — fetch K results, then discard non-matching ones — suffers from two issues:

1. **Overfetch waste**: To return K filtered results, you must fetch K×(1/selectivity) candidates. At 1% selectivity, that's 100× overfetch.
2. **Recall loss**: Even with overfetch, the final result set may contain fewer than K results or miss closer matches that were pruned before the filter was applied.

### Solution: Inline filtering

Evaluate the filter predicate *during* graph traversal so that non-matching candidates never occupy result slots. This eliminates overfetch and improves recall for selective filters. This requires changes on both the Garnet side (attribute storage design) and the DiskANN library side (search algorithm).

---

## 2. Garnet-Side: Attribute Storage Design for Inline Filtering (Current Change)

### Existing Attribute Store

The existing Garnet attribute store was designed for general-purpose access — attributes are stored as **raw JSON keyed by external (user-facing) ID**. This is the natural choice for a key-value store: the user inserts a vector with key `"doc:42"` and attributes `{"year": 2021, "genre": "action"}`, so the attributes are stored under that same key. This store serves RESP command operations (e.g., `VGETATTR`) and remains unchanged.

However, this store creates a mismatch with how DiskANN's graph traversal operates during inline filtering. DiskANN works entirely in **internal ID space** — every candidate is a `uint32` internal ID. To evaluate a filter using only the existing store, the callback must:

1. **Read `ExternalIdMap[internal_id]`** → translate the internal ID to the external key (one Garnet store read)
2. **Read `Attributes[external_key]`** → fetch the raw JSON payload (second Garnet store read)
3. **Parse JSON at query time** → `ExtractFields()` runs a JSON tokenizer to locate and parse the fields referenced by the filter expression

With inline filtering, this callback runs on **every candidate the graph traversal considers** (potentially thousands per query). The two store reads and JSON parsing per candidate become the dominant cost on the hot path.

### Solution: Add a second attribute store optimized for query-time filter evaluation

The current change **adds a new attribute store** alongside the existing one. The two stores serve different purposes:

| Store | Keyed by | Format | Purpose |
|-------|----------|--------|---------|
| Existing | External ID (user key) | Raw JSON | RESP command operations (`VGETATTR`, `VSETATTR`, etc.) |
| **New** | Internal ID (DiskANN ID) | Binary | Inline filter evaluation at query time |

The existing external ID keyed JSON store is untouched — it continues to serve all RESP command operations. The new internal ID keyed binary store is a **write-time derived projection** of the same data, optimized purely for the inline filter callback's access pattern.

### Why key by internal ID

DiskANN hands the callback an internal ID; the existing attribute store expects an external key. Bridging this gap requires reading the `ExternalIdMap` — a store read that exists purely because of the keying mismatch. By adding a store keyed by internal ID, the filter callback can look up attributes directly without any ID translation. This eliminates the `ExternalIdMap` read entirely — one fewer store read per candidate.

### Why store in binary format

Raw JSON forces parsing on every candidate at query time. Extracting a numeric field like `.year` requires scanning for the key, skipping whitespace, and parsing a number string into a double. This work is repeated identically for every candidate, every query. The JSON structure does not change between queries — this is wasted work.

The binary store **shifts the cost of JSON parsing from query time to ingestion time:**

- **At ingestion** (vector insert/update): JSON is parsed once and converted to binary via `ConvertJsonToBinary()`. The binary format is `[0xFF marker][field count][per-field: name_len, name, type_tag, value_len, value_bytes]`, with numbers pre-converted to 8-byte LE f64. This is a one-time cost, written to the new store alongside the existing JSON store.
- **At query time** (per-candidate): `ExtractFieldsBinary()` performs a direct scan over length-prefixed fields. No JSON tokenizer. Field names compared as raw byte spans. Numbers read directly as f64 — no string parsing. ~10× faster than JSON extraction.

Since each vector is inserted once but may be evaluated as a candidate across thousands of queries, this tradeoff — pay more at write, pay less at read — is the correct one for a read-heavy similarity search workload.

### Per-candidate callback comparison

```
Without binary attribute store (2 store reads + JSON parse per candidate):
  1. Read ExternalIdMap[internal_id] → external key       ← ID translation
  2. Read Attributes[external_key] → JSON bytes           ← existing JSON store
  3. ExtractFields(json, selectors) → field values         ← JSON parse at query time
  4. ExprRunner.Run(program) → bool

With binary attribute store (1 store read + binary scan per candidate):
  1. Read BinaryAttributes[internal_id] → binary bytes     ← new store, direct lookup
  2. ExtractFieldsBinary(binary, selectors) → field values ← pre-parsed, ~10× faster
  3. ExprRunner.Run(program) → bool
```

### Summary of inline filter per-candidate cost

| Aspect | Only external ID keyed JSON attribute store | Current change (internal ID keyed binary attribute) | Further optimization (co-locate binary attribute with vector data) |
|--------|---------------------------------------------|---------------------------------------|----------------------------------------------|
| Store reads per candidate | 2 (ExternalIdMap + Attributes) | 1 (Attributes only) | 0 (already accessible during traversal) |
| ID translation | Required (internal → external) | Eliminated (keyed by internal ID) | Eliminated |
| Field extraction | JSON parse at query time | Binary scan (~10× faster) | Binary scan (~10× faster) |
| Parse cost paid at | Query time (per candidate, per query) | Ingestion time (once per insert) | Ingestion time (once per insert) |
| Total per-candidate overhead | 2 reads + JSON parse + eval | 1 read + binary scan + eval | Binary scan + eval |

### Further optimization: Co-locate attributes with vector data

The current change still requires one Garnet store read per candidate to fetch the binary attributes by internal ID. A further optimization is to **co-locate the binary attribute payload directly after the vector data** in the same Garnet record.

During graph traversal, DiskANN already accesses the vector record for each candidate to compute distances. If the binary attributes are stored as trailing bytes in the same record, the callback can read them from the data DiskANN already has a reference to — no additional store read required.

```
Current change (1 store read per candidate):
  1. Read Attributes[internal_id] → binary bytes           ← still a separate read
  2. ExtractFieldsBinary(binary, selectors) → field values
  3. ExprRunner.Run(program) → bool

Co-located (0 extra store reads per candidate):
  1. Read trailing bytes from vector record[internal_id]   ← already accessible during traversal
  2. ExtractFieldsBinary(binary, selectors) → field values
  3. ExprRunner.Run(program) → bool
```

This would reduce the per-candidate cost to **zero extra store reads** — the only remaining overhead is the binary field scan and expression evaluation.

### Further with attibute index: Pre-built attribute index to replace per-candidate filter evaluation

If an attribute index is available (e.g., inverted indexes or roaring bitmaps built over attribute values), the filter predicate can be evaluated **at query planning time** rather than per-candidate during graph traversal. The index would produce a pre-computed set of matching internal IDs (e.g., a bitmap), which can be fed directly into DiskANN as a `GarnetFilter::Bitmap`. This replaces the per-candidate FFI callback entirely — DiskANN checks the bitmap with a single bit lookup instead of reading attributes and running the expression evaluator.

This would shift the filter cost from O(candidates_visited) callback invocations to a single O(matching_vectors) bitmap construction at query start, eliminating per-candidate attribute reads and expression evaluation altogether.

---

## 3. DiskANN-Side: Filtered Search Algorithms

The DiskANN library provides multiple search algorithms for filtered queries. All receive a filter predicate and differ in how they integrate filtering into graph traversal.

### 3.1 Comparison of DiskANN Filtered Search Algorithms

| Aspect                                 | Inline (w adaptive L)                                                       | BetaFilter                                                     |
|----------------------------------------|-----------------------------------------------------------------------------|----------------------------------------------------------------|
| Filter integration                     | Evaluate filter during search, scaling Lsearch based on sampled selectivity | Scale distances by beta factor for non-matching nodes          |
| Data structures                        | `NeighborPriorityQueue` (sorted array)                                      | Wraps any search strategy                                      |
| Exploration breadth at low selectivity | Bounded by the adaptive Lsearch                                             | Moderate — non-matching nodes appear farther but still compete |
| Convergence                            | Standard greedy convergence                                                 | Standard greedy convergence                                    |
| Adaptive budget                        | Yes                                                                         | No                                                             |

#### Performance Comparison (TBD)

Benchmark results on the 100K YFCC dataset comparing recall and latency are pending, but our choice of inline with adaptive-L was based on DiskANN benchmarks with in-mem providers across
a range of selectivities and across various datasets.

### 3.2 Inline w/ Adaptive-L Algorithm (Current Choice)

Please see the algorithm description in DiskANN.

### 3.3 Filter Mode Dispatch (Rust)

**File**: `DiskANN/diskann-garnet/src/provider.rs`, `dyn_index.rs`

A filter callback is provided which DiskANN will invoke to check whether vectors match the filter expression.


## 4. Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│  Client (RESP)                                       │
│  VSIM key 10 VALUES vec... FILTER ".year > 2020"     │
│         FILTER-EF 32                                 │
└──────────┬───────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────┐
│  Garnet Server (C#)                                  │
│                                                      │
│  VectorManager.ValueSimilarity()                     │
│    ├─ ExprCompiler.TryCompile(filter) → postfix pgm  │
│    ├─ Pin scratch buffers, set t_inlineFilterState   │
│    └─ DiskANNService.SearchVector(                   │
│         ..., filterData, filterLen, maxFilterEffort)  │
└──────────┬───────────────────────────────────────────┘
           │  P/Invoke (FFI)
           ▼
┌──────────────────────────────────────────────────────┐
│  DiskANN (Rust, diskann-garnet)                      │
│                                                      │
│  search_vector()                                     │
│    │  For each candidate node:                       │
│    │    ├─ Call filterCallback(ctx, internal_id)──┐  │
│    │    │                    ┌────────────────────┘  │
│    │    │                    ▼                       │
│    │    │  ┌─────────────────────────────────────┐   │
│    │    │  │ C# InlineFilterCandidateCallback    │   │
│    │    │  │  ├─ Read BinaryAttrs[internal_id]   │   │
│    │    │  │  ├─ ExtractFieldsBinary(selectors)  │   │
│    │    │  │  └─ ExprRunner.Run(program)→0/1     │   │
│    │    │  └─────────────────────────────────────┘   │
│    │                                                 │
│    └─ Return top-K                                   │
└──────────────────────────────────────────────────────┘
```

---

## 5. Filter Compilation (C#)

**File**: `libs/server/Resp/Vector/VectorManager.Filter.cs`

### Expression Language

Supports boolean expressions over JSON attributes:

```
.year > 2020 AND .genre IN ["action", "comedy"] AND NOT .archived
```

Operators: `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`, `AND`, `OR`, `NOT`

### Compilation Pipeline

1. **Tokenize** — extract field selectors (`.field`), operators, literals
2. **Shunting-yard** — convert infix to postfix via `ExprCompiler.TryCompile`
3. **Output** — array of `ExprToken` (instruction stream) + selector ranges (unique field names referenced)

### Zero-Allocation Design

All compilation and evaluation buffers come from a session-local `ScratchBufferBuilder` with a fixed ~9 KB layout:

| Buffer | Size | Purpose |
|--------|------|---------|
| `instrBuf` | 2048 B | Compiled instructions |
| `tuplePoolBuf` | 2048 B | Tuple literal storage |
| `tokensBuf` | 1024 B | Tokenizer workspace |
| `opsStackBuf` | 512 B | Shunting-yard operator stack |
| `runtimePoolBuf` | 1024 B | IN-operator array expansion |
| `extractedFields` | 1024 B | Field extraction output |
| `stackBuf` | 1024 B | Expression evaluation stack |

No heap allocations occur during filter compilation or evaluation.

---

## 6. FFI Callback Protocol

### Registration

At index creation (`CreateIndex` / `RecreateIndex`), C# passes `InlineFilterCallbackPtr` to Rust:

```csharp
delegate* unmanaged[Cdecl]<ulong, uint, byte> InlineFilterCallbackPtr
    = &InlineFilterCandidateCallbackImpl;
```

Rust stores this in its `Callbacks` struct alongside read/write/delete callbacks.

### Per-Search Setup (C# side)

Before each FFI search call:

1. Compile filter expression
2. Pin all scratch buffers
3. Populate `[ThreadStatic] t_inlineFilterState` with pointers to:
   - Compiled instructions
   - Tuple pool
   - Selector ranges
   - Filter bytes
   - Garnet storage context
4. Call `Service.SearchVector(...)` with `filter_data`, `filter_len`, `max_filtering_effort`

### Per-Candidate Callback (Rust → C#)

```
Rust calls: filterCallback(context: u64, internal_id: u32) → u8
                                                            └─ 1 = pass, 0 = reject

C# InlineFilterCandidateCallbackImpl:
  1. Read BinaryAttributes[internal_id] → binary bytes (via ReadSizeUnknown)
  2. ExtractFieldsBinary(binary, selectors) → field values
  3. ExprRunner.Run(instructions, fields) → bool
  4. Return 1 or 0
```

### Thread Safety

- DiskANN search is single-threaded per query
- `[ThreadStatic]` state ensures no cross-query interference
- `ActiveThreadSession` is set before FFI and cleared on lock release

---

## 7. Attribute Extraction

**File**: `libs/server/Resp/Vector/AttributeExtractor.cs`

Two storage formats are supported:

### JSON Format

Default format for the existing external ID keyed store. Attributes stored as raw JSON (e.g., `{"year": 2021, "genre": "action"}`). `ExtractFields()` performs a single-pass scan, matching field names against selectors and parsing values into `ExprToken`.

### Binary Format

Used by the new internal ID keyed store. Pre-extracted binary layout: `[0xFF marker][field count][per-field: name_len, name, type_tag, value_len, value_bytes]`. Numbers stored as 8-byte LE f64. `ExtractFieldsBinary()` is ~10× faster than JSON extraction. Conversion via `ConvertJsonToBinary()`.

Both paths are zero-allocation, operating on `ReadOnlySpan<byte>`.

---

## 8. End-to-End Data Flow

```
1. VSIM command parsed → filter bytes + maxFilteringEffort extracted

2. VectorManager.ValueSimilarity()
   ├─ filter non-empty → inline filtered path
   ├─ ExprCompiler.TryCompile(filter) → postfix program
   ├─ Pin buffers, populate t_inlineFilterState
   └─ DiskANNService.SearchVector(query, k, ef, filterData, filterLen, maxEffort)

3. P/Invoke → Rust search_vector()
   ├─ Detect GarnetFilter::Callback
   ├─ Create TwoQueueSearch with GarnetFilterProvider
   └─ Run two-queue algorithm:
       For each candidate:
         ├─ Compute distance
         ├─ Insert into candidates min-heap
         ├─ FFI callback → C# evaluates filter → accept/reject
         └─ If accepted → insert into filtered_results max-heap

4. Return top-K internal IDs + distances (only matching candidates)

5. Back in C# VectorManager:
   ├─ Map internal IDs → external keys via ExternalIdMap
   ├─ Optionally fetch attributes for results
   └─ Serialize RESP response to client
```

---

## 9. Performance Characteristics

### Compared to Post-Filtering

| Aspect                    | Post-Filter                  | Inline with Adaptive-L                                 |
|---------------------------|------------------------------|--------------------------------------------------------|
| Overfetch required        | Yes (K/selectivity)          | No                                                     |
| Recall at low selectivity | Poor (misses nearby matches) | High (explores broadly)                                |
| Per-candidate cost        | Distance only                | Distance + FFI callback + attribute read + filter eval |
| Memory                    | Large result buffers         | Fixed-size heaps                                       |

### Tuning

- Use FILTER-EF to control the scaling of Lsearch when selectivity is low. Defaults to 16.
