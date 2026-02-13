# Vector Filter Execution Pipeline & Architecture

## Overview

Garnet's vector filter system provides Redis VSIM-compatible filtered vector search with a layered architecture designed for incremental performance optimization. The system is organized into three layers: **Filter** (expression parsing & evaluation), **QueryEngine** (index-based optimization & strategy selection), and **VectorManager** (integration & execution).

---

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│  VSIM Command (RespServerSessionVectors.cs)                      │
│  Parses RESP protocol: VSIM key FP32 VALUES ... FILTER expr     │
└───────────────────────────┬──────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  VectorManager (VectorManager.cs + VectorManager.QueryEngine.cs) │
│  Orchestrates search: DiskANN call → attribute fetch → filter    │
│                                                                  │
│  ValueSimilarity()              ValueSimilarityWithQueryEngine() │
│  ├─ DiskANN SearchVector()      ├─ QueryEngine.Plan()            │
│  ├─ FetchVectorElementAttrs()   ├─ Strategy dispatch:            │
│  └─ ApplyPostFilter()           │   ├─ PostFilter (Phase 1)      │
│                                 │   ├─ PreFilter  (Phase 2)      │
│                                 │   └─ FilterAware (Phase 2)     │
│                                 └─ ExecutePostFilterSearch()     │
└───────────────────────────┬──────────────────────────────────────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
┌──────────────────┐ ┌────────────┐ ┌─────────────────┐
│  Filter Layer    │ │ DiskANN    │ │ QueryEngine     │
│  (parsing/eval)  │ │ (native)   │ │ (optimization)  │
└──────────────────┘ └────────────┘ └─────────────────┘
```

---

## Layer 1: Filter — Expression Parsing & Evaluation

**Namespace:** `Garnet.server.Vector.Filter`  
**Location:** `libs/server/Resp/Vector/Filter/`

This layer implements a complete expression language compatible with Redis vector search filter syntax.

### Components

| File | Responsibility |
|------|---------------|
| **VectorFilterTokenizer.cs** | Lexer — converts filter string into token stream. Handles numbers, strings (single/double quoted with escapes), identifiers (`.field` syntax), operators (`==`, `!=`, `>=`, `<=`, `&&`, `||`, `**`), keywords (`and`, `or`, `not`, `in`), delimiters `()` |
| **VectorFilterParser.cs** | Recursive descent parser — builds AST from tokens. Precedence levels (low→high): logical OR → logical AND → equality → comparison → containment (`in`) → additive → multiplicative → exponentiation → unary → primary |
| **VectorFilterExpression.cs** | AST node types: `Expr` (base), `LiteralExpr` (number/string/bool), `MemberExpr` (field access like `.year`), `UnaryExpr` (`not`, `-`), `BinaryExpr` (all binary ops) |
| **VectorFilterEvaluator.cs** | Tree-walk evaluator — evaluates AST against a `JsonElement` (parsed vector attributes). Handles type coercion, truthiness, `in` operator for arrays, and floating-point equality with epsilon |

### Expression Examples

```
.year > 1950
.genre == "action"
.year > 1970 and .rating > 4.0
(.year > 2000 or .year < 1970) and .rating >= 4.0
"classic" in .tags
not (.genre == "drama")
.rating * 2 > 8 and (.year >= 1980 or "modern" in .tags)
.year / 10 >= 200
```

### Execution Flow (Filter Only)

```
Filter string → Tokenizer → Token list → Parser → AST (Expr tree)
                                                        │
JSON attributes → JsonDocument.Parse() → JsonElement ───┤
                                                        ▼
                                              Evaluator → bool (match/no match)
```

---

## Layer 2: QueryEngine — Index-Based Optimization

**Namespace:** `Garnet.server.Vector.QueryEngine`  
**Location:** `libs/server/Resp/Vector/QueryEngine/`

This layer adds attribute indexes and cost-based strategy selection to accelerate filtered searches. Inspired by DuckDB's minimal query engine design.

### Execution Strategies

| Strategy | When Used | How It Works |
|----------|-----------|-------------|
| **PostFilter** | No indexes, or selectivity > 10% | Run DiskANN search first, then filter results. Phase 1 behavior. |
| **PreFilter** | Selectivity < 0.1% (very selective) | Query indexes first to get small candidate set, then compute distances on those candidates only. |
| **FilterAware** | Selectivity 0.1%–10% | Build bitmap from indexes, pass to DiskANN for graph traversal constrained to matching vectors. |

### Components

#### Index Infrastructure

| File | Responsibility |
|------|---------------|
| **IAttributeIndex.cs** | Interface for attribute indexes. Defines `Add`, `Remove`, `GetEqual`, `GetRange`, `Clear`, `Statistics`. Also defines `IndexStatistics` (entry count, distinct values, min/max) and `AttributeIndexType` enum (Hash, Range) |
| **HashIndex.cs** | O(1) equality lookup index. Thread-safe (`ReaderWriterLockSlim`). `Dictionary<string, HashSet<long>>` — maps field values to vector IDs. Case-insensitive. Supports selectivity estimation |
| **RangeIndex.cs** | O(log n) range query index. Thread-safe. `SortedDictionary<double, HashSet<long>>` — supports `GetGreaterThan`, `GetLessThan`, `GetRange`, equality lookups on numeric values |
| **AttributeIndexManager.cs** | Per-vector-set index registry. Create/drop indexes, route `OnVectorAdded`/`OnVectorRemoved` to all indexes (parses JSON attributes), find relevant indexes for a filter expression by walking the AST |

#### Bitmap

| File | Responsibility |
|------|---------------|
| **FilterBitmap.cs** | Dense bitmap (`ulong[]`). Set/Clear/IsSet per bit. `PopCount` via `BitOperations.PopCount`. Static `And`/`Or`/`Not` operations. `EnumerateSetBits()` for iteration. `GetBytes()` for FFI serialization |
| **FilterBitmapBuilder.cs** | Walks filter AST, queries indexes, builds bitmap. Handles `==`, `!=`, `>`, `>=`, `<`, `<=` against indexes. Combines via `AND`/`OR`/`NOT`. Tracks `HasUnresolvedPredicates` flag when predicates can't be resolved from indexes (triggers post-filter fallback) |

#### Query Planning & Cost

| File | Responsibility |
|------|---------------|
| **ExecutionStrategy.cs** | Enum: `PostFilter`, `PreFilter`, `FilterAware` |
| **SelectivityEstimator.cs** | Estimates filter selectivity from index statistics. Equality: `1/cardinality` or exact count ratio. Range: linear interpolation. AND: multiply. OR: inclusion-exclusion. NOT: complement. Default 0.5 when no index available |
| **ExecutionCost.cs** | Cost model with tunable weights. Tracks `IndexLookups`, `DistanceComputations`, `IOOperations`, `FilterEvaluations`. Computes weighted `TotalCost`. Provides `CostWeights` constants for tuning |
| **QueryPlan.cs** | Immutable plan: `Strategy`, `Filter` (AST), `FilterString`, `IndexNames`, `EstimatedSelectivity`, `EstimatedCandidates`, `EstimatedCost`. `ToString()` for debugging/EXPLAIN |
| **VectorQueryPlanner.cs** | Threshold-based strategy selection. Analyzes filter + index stats → selectivity → strategy. Computes cost estimate per strategy. Falls back to PostFilter when no indexes exist |

#### Engine & Results

| File | Responsibility |
|------|---------------|
| **VectorQueryEngine.cs** | Main orchestrator. `Plan()` — parses filter + builds plan. `BuildFilterBitmap()` — for filter-aware strategy. `GetPreFilterCandidates()` — for pre-filter strategy. Expression caching (bounded to 1000 entries). `LogQueryMetrics()` for telemetry |
| **VectorQueryResult.cs** | Result container: `Found`, `StrategyUsed`, `PostFilterApplied`, `CandidatesExamined`, `ElapsedTicks`. Plus `QueryExecutionContext` for passing DiskANN params |

### Query Planning Flow

```
Filter string ──► VectorQueryEngine.Plan()
                    │
                    ├─ ParseFilter() ─► AST (cached)
                    │
                    └─ VectorQueryPlanner.BuildPlan()
                         │
                         ├─ AttributeIndexManager.GetIndexesForFilter()
                         │    └─ Walk AST, collect field refs, match to indexes
                         │
                         ├─ SelectivityEstimator.Estimate()
                         │    └─ Estimate from index stats (cardinality, min/max)
                         │
                         ├─ Strategy selection:
                         │    selectivity < 0.001  → PreFilter
                         │    selectivity < 0.10   → FilterAware
                         │    selectivity >= 0.10  → PostFilter
                         │    no indexes           → PostFilter (always)
                         │
                         └─ ExecutionCost estimation
                              └─ Model I/O, CPU, index lookups per strategy
```

---

## Layer 3: VectorManager — Integration

**Location:** `libs/server/Resp/Vector/`

### Files

| File | Responsibility |
|------|---------------|
| **VectorManager.cs** | Core vector operations. `TryAdd()`, `TryRemove()`, `ValueSimilarity()`, `ElementSimilarity()`. Contains `ApplyPostFilter()` and `EvaluateFilter()` — the active Phase 1 post-filter path |
| **VectorManager.QueryEngine.cs** | Partial class adding query engine integration. `CreateAttributeIndex()`, `DropAttributeIndex()`, `NotifyVectorAdded()`, `NotifyVectorRemoved()`, `ValueSimilarityWithQueryEngine()` (full strategy-based search), `ExecutePostFilterSearch()` (extracted reusable post-filter path) |

### Active Execution Path (Phase 1 — Current)

```
VSIM command
  → RespServerSessionVectors.NetworkVSIM()
    → storageApi.VectorSetValueSimilarity()
      → VectorManager.ValueSimilarity()
        ├─ DiskANN SearchVector() — get K nearest neighbors
        ├─ FetchVectorElementAttributes() — load JSON attributes
        └─ ApplyPostFilter() — evaluate filter on each result
             └─ EvaluateFilter() per result
                  ├─ VectorFilterTokenizer.Tokenize()
                  ├─ VectorFilterParser.ParseExpression()
                  └─ VectorFilterEvaluator.EvaluateExpression()
```

### Future Execution Path (Phase 2 — Query Engine)

```
VSIM command
  → VectorManager.ValueSimilarityWithQueryEngine()
    ├─ VectorQueryEngine.Plan() — choose strategy
    │
    ├─ switch(plan.Strategy):
    │   ├─ PreFilter:
    │   │   ├─ GetPreFilterCandidates() → HashSet<long>
    │   │   └─ Distance computation on candidates only
    │   │
    │   ├─ FilterAware:
    │   │   ├─ BuildFilterBitmap() → FilterBitmap
    │   │   └─ DiskANN SearchVector(bitmap) — constrained traversal
    │   │
    │   └─ PostFilter:
    │       └─ ExecutePostFilterSearch() — same as Phase 1
    │
    └─ LogQueryMetrics() — telemetry
```

---

## Wiring Status

| Integration Point | Status | Notes |
|-------------------|--------|-------|
| VADD → `NotifyVectorAdded()` | ❌ Not wired | `TryAdd()` doesn't call notify; indexes won't be populated |
| VREM → `NotifyVectorRemoved()` | ❌ Not wired | `TryRemove()` doesn't call notify |
| VSIM → `ValueSimilarityWithQueryEngine()` | ❌ Not wired | VSIM still calls `ValueSimilarity()` (Phase 1 path) |
| VCREATEINDEX / VDROPINDEX commands | ❌ Not implemented | `CreateAttributeIndex()`/`DropAttributeIndex()` exist but no RESP handlers |
| DiskANN FFI bitmap parameter | ❌ Not implemented | FilterAware strategy falls through to PostFilter |

---

## Test Coverage

### Unit Tests — VectorQueryEngineTests.cs (28 tests)

Run with: `dotnet test <path>/Garnet.test.csproj --filter "FullyQualifiedName~VectorQueryEngineTests" -f net8.0 -v n`

| Region | Tests | What They Validate |
|--------|-------|--------------------|
| **HashIndex** (9 tests) | `HashIndex_AddAndLookup`, `HashIndex_CaseInsensitiveLookup`, `HashIndex_Remove`, `HashIndex_RemoveLastEntry`, `HashIndex_NotFound`, `HashIndex_Statistics`, `HashIndex_SelectivityEstimation`, `HashIndex_Clear`, `HashIndex_DuplicateAdd` | O(1) equality lookup, case insensitivity, add/remove lifecycle, statistics tracking, selectivity estimation, idempotent adds |
| **RangeIndex** (6 tests) | `RangeIndex_AddAndRangeQuery`, `RangeIndex_GreaterThan`, `RangeIndex_LessThan`, `RangeIndex_Statistics`, `RangeIndex_Remove`, `RangeIndex_EqualityLookup`, `RangeIndex_SelectivityEstimation` | Range queries, boundary conditions, statistics (min/max/count), removal, equality fallback on numeric index |
| **AttributeIndexManager** (5 tests) | `IndexManager_CreateAndGetIndex`, `IndexManager_DuplicateCreate`, `IndexManager_Drop`, `IndexManager_OnVectorAdded`, `IndexManager_OnVectorRemoved`, `IndexManager_GetIndexesForFilter`, `IndexManager_GetIndexesForComplexFilter` | Index CRUD lifecycle, JSON attribute parsing, AST→index matching for simple and complex filters |
| **FilterBitmap** (7 tests) | `Bitmap_SetAndCheck`, `Bitmap_Clear`, `Bitmap_And`, `Bitmap_Or`, `Bitmap_Not`, `Bitmap_FromIds`, `Bitmap_EnumerateSetBits`, `Bitmap_Selectivity` | Bit-level operations, boolean algebra (AND/OR/NOT), enumeration, population count, selectivity calculation |
| **SelectivityEstimator** (5 tests) | `Estimator_NoFilter`, `Estimator_EqualityWithIndex`, `Estimator_AndCombination`, `Estimator_OrCombination`, `Estimator_RangeWithIndex`, `Estimator_NoIndex_DefaultSelectivity` | Selectivity math: equality (1/cardinality), range (linear interpolation), AND (multiply), OR (inclusion-exclusion), default fallback |
| **VectorQueryPlanner** (5 tests) | `Planner_NoFilter_PostFilter`, `Planner_HighSelectivity_PostFilter`, `Planner_LowSelectivity_PreFilter`, `Planner_MediumSelectivity_FilterAware`, `Planner_NoIndex_AlwaysPostFilter`, `Planner_PlanContainsIndexNames` | Threshold-based strategy selection, no-filter fast path, no-index fallback, plan metadata |
| **FilterBitmapBuilder** (5 tests) | `BitmapBuilder_SimpleEquality`, `BitmapBuilder_AndCombination`, `BitmapBuilder_UnresolvedPredicate`, `BitmapBuilder_NotOperator`, `BitmapBuilder_RangeQuery` | AST→bitmap translation, AND/NOT composition, unresolved predicate flag, range queries via bitmap |
| **VectorQueryEngine** (4 tests) | `QueryEngine_PlanNoFilter`, `QueryEngine_PlanWithFilter`, `QueryEngine_ExpressionCaching`, `QueryEngine_BuildBitmap`, `QueryEngine_GetPreFilterCandidates` | End-to-end planning, expression cache hit, bitmap generation, candidate extraction |
| **ExecutionCost** (2 tests) | `ExecutionCost_PostFilterHigherThanPreFilter_LowSelectivity`, `ExecutionCost_PostFilterCheaper_HighSelectivity` | Cost model validates that PreFilter is cheaper for low selectivity and PostFilter is cheaper for high selectivity |
| **QueryPlan** (1 test) | `QueryPlan_ToString` | Debug/EXPLAIN output formatting |

### Integration Tests — RespVectorSetTests.cs (33 tests)

Run with: `dotnet test <path>/Garnet.test.csproj --filter "FullyQualifiedName~RespVectorSetTests" -f net8.0 -v n -- NUnit.NumberOfTestWorkers=1`

| Test | What It Validates |
|------|-------------------|
| **VSIM** | Basic vector similarity search via RESP protocol |
| **VSIMWithAttribs** | Similarity search returning attribute JSON |
| **VSIMWithAttributeFiltering** | Post-filter with `.year > 1950`, `.year > 1990` (empty result) |
| **VSIMWithAdvancedFiltering** | Complex filters: AND, OR, equality, arithmetic, containment (`in`), NOT, parenthesized groups |
| **VADD / VADDErrors / VADDXPREQB8 / VADDVariableLengthElementIds** | Vector insertion, error handling, quantization, variable-length IDs |
| **VREM** | Vector removal |
| **VDIM / VEMB / VINFO** | Metadata queries |
| **VGETATTR / VGETATTR_NotFound** | Attribute retrieval |
| **DeleteVectorSet / RepeatedVectorSetDeletes / Interrupted...** | Vector set deletion lifecycle and crash recovery |
| **DisabledWithFeatureFlag** | Feature flag gating |

### Running Tests

```bash
# Query engine unit tests (fast, no server needed)
dotnet test Garnet.test.csproj --filter "FullyQualifiedName~VectorQueryEngineTests" -f net8.0 -v n

# Integration tests (starts Garnet server, sequential to avoid port conflicts)
dotnet test Garnet.test.csproj --filter "FullyQualifiedName~RespVectorSetTests" -f net8.0 -v n -- NUnit.NumberOfTestWorkers=1

# All vector tests
dotnet test Garnet.test.csproj --filter "FullyQualifiedName~Vector" -f net8.0 -v n -- NUnit.NumberOfTestWorkers=1
```

> **Note:** Use `-f net8.0` (single TFM) and `NUnit.NumberOfTestWorkers=1` for integration tests to prevent port 33278 conflicts between parallel test runners.

---

## File Map

```
libs/server/Resp/Vector/
├── Filter/                              # Layer 1: Expression language
│   ├── VectorFilterExpression.cs        #   AST node types
│   ├── VectorFilterTokenizer.cs         #   Lexer
│   ├── VectorFilterParser.cs            #   Recursive descent parser
│   └── VectorFilterEvaluator.cs         #   Tree-walk evaluator
│
├── QueryEngine/                         # Layer 2: Index-based optimization
│   ├── IAttributeIndex.cs              #   Index interface + stats + types
│   ├── HashIndex.cs                    #   O(1) equality index
│   ├── RangeIndex.cs                   #   O(log n) range index
│   ├── AttributeIndexManager.cs        #   Per-set index registry
│   ├── FilterBitmap.cs                 #   Dense bitmap with boolean ops
│   ├── FilterBitmapBuilder.cs          #   AST → bitmap via indexes
│   ├── SelectivityEstimator.cs         #   Filter selectivity estimation
│   ├── ExecutionStrategy.cs            #   Strategy enum
│   ├── ExecutionCost.cs                #   Cost model
│   ├── QueryPlan.cs                    #   Immutable execution plan
│   ├── VectorQueryPlanner.cs           #   Strategy selection
│   ├── VectorQueryEngine.cs            #   Main orchestrator
│   └── VectorQueryResult.cs            #   Result + context types
│
├── VectorManager.cs                     # Layer 3: Core operations (Phase 1 active)
├── VectorManager.QueryEngine.cs         # Layer 3: Query engine integration (Phase 2 ready)
├── VectorManager.Index.cs              #   DiskANN Index struct
├── DiskANNService.cs                   #   Native FFI to DiskANN (Rust)
└── RespServerSessionVectors.cs         #   RESP command handlers

test/Garnet.test/
├── VectorQueryEngineTests.cs            # 28 unit tests for QueryEngine layer
└── RespVectorSetTests.cs               # 33 integration tests (RESP commands)
```
