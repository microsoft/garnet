# YFCC Filtered Vector Search Benchmark: Garnet vs Redis 8

**Date:** 2026-02-24
**Benchmark tool:** `playground/VectorSearchBench`
**Branch:** `haixu/bench-filter`

---

## 1. Dataset

| Property | Value |
|----------|-------|
| Source | YFCC10M (Yahoo Flickr Creative Commons 10M) |
| Base vectors loaded | 100,000 (of 1M available) |
| Dimensionality | 192 |
| Element type | uint8 |
| Similarity metric | Cosine similarity |
| Per-element attributes | JSON: `year`, `month`, `camera`, `country` (country may be absent) |
| Element IDs | Numeric strings `"0"` .. `"99999"` |

**Example attribute blob:**
```json
{"year":"2010","month":"May","camera":"Panasonic DMC-FZ28","country":"United States"}
```

---

## 2. Server Configuration

| | Garnet | Redis 8 |
|---|---|---|
| Version | Development build (branch `haixu/bench-filter`) | Redis 8.0.0 (Docker) |
| Port | 6380 | 6379 |
| Host | 127.0.0.1 (local) | 127.0.0.1 (local) |
| Vector format (ingest) | `XB8` (native uint8 binary) | `FP32` (uint8 converted to float32 binary) |
| Vector format (query) | `XB8` | `FP32` |
| Index type | DiskANN (graph-based ANN) | Built-in vector set (HNSW-like) |
| Search EF | 200 | 200 |
| Quantization | Native uint8 via XB8 | Q8 (default; Redis internally quantizes FP32 input to int8) |

**Note on vector format asymmetry:** Garnet supports `XB8` (raw uint8 binary), which avoids the 4x memory expansion to float32. Redis 8 `VADD`/`VSIM` only accept `VALUES` (float strings) or `FP32` (float32 binary). The benchmark converts uint8 vectors to float32 for the Redis path. Both servers internally store quantized representations, so the search-time precision should be comparable.

---

## 3. Data Ingestion

### Garnet
```
VADD bench:vset XB8 <192_raw_bytes> <element_id> SETATTR <json_attrs>
```
- Key: `bench:vset`
- `XB8` sends the 192-byte uint8 vector as a raw binary blob
- `SETATTR` attaches the JSON attribute string for later filtering

### Redis 8
```
VADD bench:vset:redis FP32 <768_fp32_bytes> <element_id> SETATTR <json_attrs>
```
- Key: `bench:vset:redis`
- `FP32` sends the vector as 768 bytes (192 × 4-byte little-endian floats), converted from uint8
- `SETATTR` uses the same JSON attribute string; Redis 8 supports identical per-element attribute syntax

Both servers loaded 100,000 vectors sequentially via StackExchange.Redis client.

---

## 4. Query Phases

The benchmark runs 6 query phases with varying filter selectivity. Query vectors are drawn from pre-built query sets (10K queries per set, 200 used per phase). Filters are translated from MongoDB-style JSON to dot-syntax:

```
{"year": {"$eq": "2009"}}             →  .year == "2009"
{"$and": [{"camera": {"$eq": "NIKON"}},
          {"year": {"$eq": "2006"}}]}  →  .camera == "NIKON" && .year == "2006"
```

| Phase | Filter example | Avg selectivity | Avg passing | Range | Description |
|-------|----------------|-----------------|-------------|-------|-------------|
| **unfiltered** | (none) | 100% | 100,000 | -- | Pure ANN search, no filtering |
| **single-high** | `.year == "2012"` | 15.45% | 15,448 | 5.5%–33.9% | Single equality on year or country (high-frequency values) |
| **single-medium** | `.country == "ES"` | 1.28% | 1,281 | 0.16%–3.3% | Single equality on country or camera (medium-frequency values) |
| **single-low** | `.camera == "EPSON"` | 0.02% | 16 | 0%–0.10% | Single equality on rare camera or country values |
| **multi-medium** | `.camera == "NIKON" && .year == "2006"` | 1.48% | 1,483 | 0.19%–4.8% | AND of two predicates (camera+year or country+year) |
| **multi-low** | `.country == "CA" && .year == "2005"` | 0.02% | 24 | 0%–0.11% | AND of two predicates with rare attribute values |

### How selectivity is calculated

**Selectivity** is the fraction of the base dataset that passes a given filter expression. It is computed empirically: for each of the 200 query filters in a phase, we count how many of the 100,000 base records satisfy the filter predicate, then divide by 100,000. The table above shows the average across all 200 queries per phase.

For example, in the `single-high` phase, the filter `.year == "2012"` matches 8,627 out of 100,000 records (8.6% selectivity), while `.country == "US"` matches 33,924 records (33.9%). Averaged across the 200 queries (which use different attribute values), the mean selectivity is 15.45%.

Low-selectivity phases are particularly challenging for ANN+filter because the filter eliminates the vast majority of nearest-neighbor candidates returned by the graph search.

### Query Command

**Garnet:**
```
VSIM bench:vset XB8 <192_raw_bytes> COUNT 10 WITHSCORES EF 200 FILTER ".year == \"2009\""
```

**Redis 8:**
```
VSIM bench:vset:redis FP32 <768_fp32_bytes> COUNT 10 WITHSCORES EF 200 FILTER ".year == \"2009\""
```

Both use the same `VSIM ... FILTER` syntax with identical filter expressions. The only differences are the key name and vector encoding format.

---

## 5. Ground Truth Method

**Method:** Client-side brute-force cosine similarity

For each query vector, the benchmark:
1. Iterates all 100,000 base records in memory
2. Applies the same filter expression client-side (parsing `&&` conjunctions and `.field == "value"` predicates)
3. Computes cosine similarity between the query and each passing candidate
4. Returns the top-K by descending similarity

This produces exact ground-truth results for both filtered and unfiltered queries.

**Recall@K** = |target_topK ∩ bruteforce_topK| / K, averaged across all 200 queries per phase.

Both Garnet and Redis results are measured against this same brute-force ground truth, making the recall numbers directly comparable.

**Why not `VSIM TRUTH`?** Redis 8's `VSIM TRUTH` command performs an exact linear scan but does not support the `FILTER` clause. Since most phases use filters, client-side brute-force is used uniformly.

---

## 6. Results

### Parameters
- **Queries per phase:** 200
- **Top-K:** 10
- **Search EF:** 200
- **Warmup:** 20 queries (discarded)

### 6.1 Unfiltered (no filter)

| Target | Mean (us) | p50 (us) | p95 (us) | p99 (us) | Recall@10 |
|--------|-----------|----------|----------|----------|-----------|
| **Garnet** | **3,690** | 3,551 | 5,147 | 5,803 | **98.65%** |
| Redis 8 | 3,876 | 3,756 | 4,735 | 5,299 | 82.25% |
| BruteForce (GT) | 114,594 | 112,925 | 128,722 | 139,810 | -- |

**Garnet is 1.05x faster. Garnet recall is 16.4 percentage points higher.**

### 6.2 single-high (15.45% avg selectivity)

| Target | Mean (us) | p50 (us) | p95 (us) | p99 (us) | Recall@10 |
|--------|-----------|----------|----------|----------|-----------|
| **Garnet** | **7,126** | 7,492 | 10,483 | 11,447 | 86.60% |
| Redis 8 | 13,573 | 13,208 | 20,912 | 23,172 | **86.70%** |
| BruteForce (GT) | 43,468 | 40,081 | 65,070 | 75,579 | -- |

**Garnet is 1.90x faster. Recall is essentially identical (~86.6%).**

### 6.3 single-medium (1.28% avg selectivity)

| Target | Mean (us) | p50 (us) | p95 (us) | p99 (us) | Recall@10 |
|--------|-----------|----------|----------|----------|-----------|
| **Garnet** | **9,007** | 8,953 | 11,816 | 13,361 | 13.15% |
| Redis 8 | 16,086 | 15,870 | 22,012 | 27,041 | **85.50%** |
| BruteForce (GT) | 26,123 | 25,495 | 31,598 | 35,015 | -- |

**Garnet is 1.79x faster but recall drops to 13.15% vs Redis's 85.50%.**

### 6.4 single-low (0.02% avg selectivity)

| Target | Mean (us) | p50 (us) | p95 (us) | p99 (us) | Recall@10 |
|--------|-----------|----------|----------|----------|-----------|
| **Garnet** | **8,352** | 8,243 | 10,855 | 12,067 | 1.12% |
| Redis 8 | 14,455 | 14,268 | 19,167 | 21,984 | **18.93%** |
| BruteForce (GT) | 24,017 | 23,613 | 27,679 | 29,611 | -- |

**Garnet is 1.73x faster. Both have low recall; Redis is significantly better (18.93% vs 1.12%).**

### 6.5 multi-medium (1.48% avg selectivity)

| Target | Mean (us) | p50 (us) | p95 (us) | p99 (us) | Recall@10 |
|--------|-----------|----------|----------|----------|-----------|
| **Garnet** | **8,604** | 8,574 | 11,111 | 12,436 | 15.10% |
| Redis 8 | 15,935 | 15,807 | 21,431 | 23,337 | **87.25%** |
| BruteForce (GT) | 28,362 | 26,913 | 36,542 | 40,246 | -- |

**Garnet is 1.85x faster. Redis recall is 87.25% vs Garnet's 15.10%.**

### 6.6 multi-low (0.02% avg selectivity)

| Target | Mean (us) | p50 (us) | p95 (us) | p99 (us) | Recall@10 |
|--------|-----------|----------|----------|----------|-----------|
| **Garnet** | **8,376** | 8,294 | 11,345 | 11,906 | 1.20% |
| Redis 8 | 15,440 | 15,146 | 21,474 | 24,544 | **22.19%** |
| BruteForce (GT) | 23,917 | 23,517 | 27,519 | 29,079 | -- |

**Garnet is 1.84x faster. Both have low recall on narrow filters.**

---

## 7. Summary

### Latency

| Phase | Selectivity | Garnet Mean (us) | Redis Mean (us) | Speedup |
|-------|-------------|------------------|-----------------|---------|
| unfiltered | 100% | 3,690 | 3,876 | 1.05x |
| single-high | 15.45% | 7,126 | 13,573 | 1.90x |
| single-medium | 1.28% | 9,007 | 16,086 | 1.79x |
| single-low | 0.02% | 8,352 | 14,455 | 1.73x |
| multi-medium | 1.48% | 8,604 | 15,935 | 1.85x |
| multi-low | 0.02% | 8,376 | 15,440 | 1.84x |

**Garnet is consistently 1.7-1.9x faster than Redis on filtered queries**, and roughly on par for unfiltered.

### Recall@10

| Phase | Selectivity | Garnet | Redis | Delta |
|-------|-------------|--------|-------|-------|
| unfiltered | 100% | **98.65%** | 82.25% | Garnet +16.4 pp |
| single-high | 15.45% | 86.60% | **86.70%** | Tied |
| single-medium | 1.28% | 13.15% | **85.50%** | Redis +72.4 pp |
| single-low | 0.02% | 1.12% | **18.93%** | Redis +17.8 pp |
| multi-medium | 1.48% | 15.10% | **87.25%** | Redis +72.2 pp |
| multi-low | 0.02% | 1.20% | **22.19%** | Redis +21.0 pp |

### Selectivity vs Recall (key observation)

The recall results show a clear pattern tied to filter selectivity:

| Selectivity range | Garnet recall | Redis recall | Assessment |
|-------------------|---------------|--------------|------------|
| 100% (unfiltered) | 98.65% | 82.25% | Garnet dominates |
| ~15% (single-high) | 86.60% | 86.70% | Parity |
| ~1-1.5% (single-medium, multi-medium) | 13-15% | 85-87% | Redis dominates |
| ~0.02% (single-low, multi-low) | ~1% | 19-22% | Both low, Redis better |

The **inflection point** is around 1-2% selectivity. Above ~15%, Garnet's 5x over-fetch is sufficient to find enough qualifying candidates. Below ~1%, the post-filter approach fundamentally cannot retrieve enough matching results from a blind graph traversal, regardless of retry/paging strategy.

---

## 8. Garnet's Current Filter Implementation

Garnet currently uses a **pure post-filter** approach for `VSIM ... FILTER`:

### How it works

1. **Parse** the filter expression into an AST (recursive descent parser supporting `==`, `!=`, `>`, `<`, `>=`, `<=`, `&&`, `||`, `not`, `in`, arithmetic, parentheses)
2. **Over-fetch** from the DiskANN graph index: request `count * 5` candidates (5x the desired top-K)
3. **Fetch attributes** for each candidate (JSON stored per-element)
4. **Evaluate** the filter expression against each candidate's attributes
5. **Retry** with `count * 10` if not enough results pass the filter
6. **Paged fallback** (up to 20 pages) if retry still insufficient

### Key constants

| Constant | Value | Location |
|----------|-------|----------|
| `FilterOverFetchMultiplier` | 5 | `VectorManager.cs:61` |
| `FilterOverFetchRetryMultiplier` | 10 | `VectorManager.cs:66` |
| `MaxPagedSearchPages` | 20 | `VectorManager.cs:71` |

### Why recall degrades on narrow filters

The post-filter approach searches the DiskANN graph **without awareness of the filter**. The graph traversal returns candidates that are nearest neighbors in vector space, but many of those candidates fail the filter. With narrow filters (low selectivity), the vast majority of ANN candidates are discarded, and the over-fetch multiplier (5x or 10x) is insufficient to find enough qualifying results.

For example, with `single-low` (0.02% selectivity, ~16 matching records) and top-K=10:
- First attempt fetches 50 candidates (5 × 10), expects ~0.01 to pass → insufficient
- Retry fetches 100 candidates (10 × 10), expects ~0.02 to pass → still insufficient
- Paged fallback iterates more but the DiskANN graph locality means it keeps visiting the same region of the index
- The 16 matching records are scattered across the 100K-element graph — finding them requires essentially a full scan

Redis 8 likely uses a different strategy (integrated filter-aware search or more aggressive candidate generation), which explains its better recall on narrow filters despite higher latency.

### Supported filter syntax

Both Garnet and Redis 8 share the same dot-syntax for filter expressions:

```
.field == "value"              # string equality
.field != "value"              # string inequality
.field > 2000                  # numeric comparison
.field >= 2000 && .field < 2010  # conjunction
.field == "a" || .field == "b"   # disjunction
not (.field == "value")        # negation
"value" in .field              # array membership
```

Garnet additionally supports arithmetic in filters (`.year / 10 >= 200`), parenthesized grouping, and `**` exponentiation.

### Query engine (in development)

The `libs/server/Resp/Vector/QueryEngine/` directory contains infrastructure for a cost-based query planner with:
- **Attribute indexes** (hash indexes for equality, range indexes for comparisons)
- **Selectivity estimation** (cardinality tracking for query planning)
- **Filter bitmaps** (pre-computed candidate sets from index lookups)
- **Cost model** (chooses between post-filter, pre-filter, and hybrid strategies)

This query engine is not yet integrated into the VSIM command path. When enabled, it would allow **pre-filter** (narrow candidates via attribute indexes before ANN search) and **hybrid** (use selectivity to choose the best strategy per query) approaches, which should significantly improve recall on narrow filters.

---

## 9. Reproducing This Benchmark

```bash
# Terminal 1: Garnet
dotnet run --project main/GarnetServer -- --port 6380 --enable-vector-set-preview

# Terminal 2: Redis 8
docker run -d --name redis8 -p 6379:6379 redis:8.0.0

# Terminal 3: Benchmark
dotnet run --project playground/VectorSearchBench -- \
  --data-dir "<path-to-yfcc-data>" \
  --garnet-port 6380 --redis-port 6379 \
  --yfcc-count 100000 --num-queries 200 --top-k 10 --ef 200
```

The YFCC data directory must contain `base.1M.u8bin`, `base.1M.label.jsonl`, query vector files, query filter JSONL files, and `yfcc-config.json`.
