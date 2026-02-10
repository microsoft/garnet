# VSIM Search with Predicates - Implementation Plan

---

## Goal

**Bring feature compatibility with Redis VSIM command with significantly better filter search performance.**

- **API Compatibility**: Maintain full compatibility with Redis vector search syntax (VSIM/FT.SEARCH)
- **Performance Goal**: 5x speedup for selective filters through intelligent query routing

---

## Overview

**Three Main Phases:** Post-Filtering → Attribute Index + Filter-Aware Search → Complex Queries with Query Engine

---

## Phase 1: Post-Filtering

**Goal:** Current implementation - Redis VSIM compatible post-filtering with expression evaluation.

**Redis Compatibility:**
- ✅ Supports VSIM command with FILTER parameter
- ✅ Full expression syntax: arithmetic, comparison, logical, containment operators
- ✅ Compatible with Redis vector search filter expressions 

**What we need:**
- Full expression parser with arithmetic, comparison, logical, and containment operators
- Recursive descent parser with proper precedence
- Post-filtering after DiskANN returns candidates
- Proactive candidate fetching (paging)
- Type coercion and error handling

**Status:** 
- Filter expressions working: `.year > 1950`, `not (.genre == "drama")`, `.rating >= 4.0 and .views > 1000`
- All 35 vector tests passing
- Limitations: Low selectivity filters return fewer results than requested

---

## Phase 2: Attribute Index + Filter-Aware Search

**Goal:** Add performance optimizations while maintaining Redis VSIM API compatibility.

**Key Principle:** Same API, better performance - existing queries automatically benefit from optimizations.

**Filtering Types:**
- **Single Equality Filtering**: `.category == 'electronics'` - Uses Hash Index
- **Single Range Filtering**: `.price > 100`, `.year >= 1990 and .year < 2000` - Uses Range Index
- **Complex Filtering**: Multiple predicates combined with AND/OR (Phase 3)

### 1. Index Infrastructure
**What:** Core indexing system that other components depend on
- Design index storage schema in Garnet (`__idx:<vectorset>:<field>`)
- **Deliverable:** Working index infrastructure that can store and query atrribute indexes

### 2. Pre-Filtering with Attribute Index
**What:** First concrete index type for single predicate
  - For small candidate sets (<5k): linear scan + top-K
  - For larger sets: build filter bitset and delegate to DiskANN filteraware search
- **Deliverable:** Single predicate queries like `.category == 'electronics'`, `.status == 'active'`, '.price<100' use single index to filter first

### 3. Bitmap Generation (C#)
**What:** C# layer to build and pass bitmaps to DiskANN
  - Build bitmap from index query results
  - Support AND/OR/NOT operations (for complex multi-predicate filters in Phase 3)
  - Callback to check bitmap for FFI
- Unit tests (operations, serialization, performance)
- Notes: for larger dataset that generate bitmap is costly(we can have a budget for it), we might generate a lossy bitmap that will have fale postive and let the post filtering stage filtering results.  
- **Deliverable:** Can create bitmaps from index queries and pass to DiskANN

### 4. DiskANN Filter Aware Integration 
**What:** Integrate DiskANN native library to accept bitset constraints
- Provide LabelQueryProvider via FFI
- Performance optimization (profile overhead, optimize hot path)
- Unit tests with various selectivities (1%, 10%, 50%)
- **Deliverable:** DiskANN can navigate graph with bitset constraint

---

## Phase 3: Complex Queries with Query Engine

**Goal:** Enable complex filtering (multiple predicates), add composite/inverted indexes, implement cost-based query engine.

**Filtering Types:**
- **Complex Filtering**: Multiple predicates with AND/OR
  - `.category == 'electronics' AND .price > 100 AND .inStock == true`
  - `.genre == 'action' OR .genre == 'thriller'`
  - `(.rating > 4.5 OR .views > 10000) AND .year >= 2020`

---

### Query Engine Abstraction Architecture

**Minimal Query Engine Design** - Abstraction to support extensible strategies:

```
┌─────────────────────────────────────────────────────────────┐
│  VSIM Command Handler                                        │
│  (Entry point - receives query string)                       │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│  Query Engine (VectorQueryEngine)                            │
│  - Parse filter expression                                   │
│  - Build execution plan (IQueryPlan)                         │
│  - Execute via strategy (IQueryExecutor)                     │
│  - Return unified results                                    │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ├─────────────► Query Planner (IQueryPlanner)
                 │                - Analyze filter
                 │                - Estimate cost
                 │                - Select strategy
                 │                - Return IQueryPlan
                 │
                 └─────────────► Strategy Registry
                                  - PostFilterStrategy
                                  - PreFilterStrategy
                                  - FilterAwareStrategy
                                  - (Future strategies...)
```

**Core Abstractions:**

1. **IQueryExecutor** - Strategy pattern for execution
```csharp
public interface IQueryExecutor
{
    string Name { get; }
    
    // Can this executor handle the query?
    bool CanExecute(VectorQuery query, IndexStatistics stats);
    
    // Estimate cost (latency, I/O, CPU)
    ExecutionCost EstimateCost(VectorQuery query, IndexStatistics stats);
    
    // Execute the query
    VectorSearchResult Execute(VectorQuery query, ExecutionContext context);
}
```

2. **IQueryPlan** - Execution plan representation
```csharp
public interface IQueryPlan
{
    IQueryExecutor Executor { get; }
    FilterExpression Filter { get; }
    double EstimatedSelectivity { get; }
    ExecutionCost EstimatedCost { get; }
    Dictionary<string, object> Metadata { get; } // Debug info, explain output
}
```

3. **VectorQueryEngine** - Main orchestrator
```csharp
public class VectorQueryEngine
{
    private readonly IQueryPlanner _planner;
    private readonly ExecutorRegistry _registry;
    
    public VectorSearchResult Execute(VectorQuery query)
    {
        // 1. Plan: Choose strategy
        var plan = _planner.BuildPlan(query);
        
        // 2. Execute: Run strategy
        var result = plan.Executor.Execute(query, context);
        
        // 3. Telemetry: Log metrics
        LogMetrics(plan, result);
        
        return result;
    }
}
```

4. **ExecutorRegistry** - Strategy registration
```csharp
public class ExecutorRegistry
{
    private List<IQueryExecutor> _executors = new();
    
    public void Register(IQueryExecutor executor) => _executors.Add(executor);
    
    public IQueryExecutor SelectBestExecutor(VectorQuery query, IndexStatistics stats)
    {
        // Filter executors that can handle query
        var candidates = _executors.Where(e => e.CanExecute(query, stats));
        
        // Return lowest cost
        return candidates.OrderBy(e => e.EstimateCost(query, stats)).First();
    }
}
```

**Benefits:**
- ✅ New strategies added by implementing IQueryExecutor
- ✅ No changes to VSIM command handler
- ✅ Easy to test strategies in isolation
- ✅ Cost-based selection extensible
- ✅ Query plans can be cached/reused

---

### 1. Query Engine Core
**What:** Build minimal abstraction layer
- Implement `IQueryExecutor` interface
- Implement `IQueryPlan` interface  
- Implement `VectorQueryEngine` orchestrator
- Implement `ExecutorRegistry` for strategy registration
- Refactor existing strategies to implement IQueryExecutor:
  - `PostFilterExecutor` (Phase 1)
  - `PreFilterExecutor` (Phase 2)
  - `FilterAwareExecutor` (Phase 2)
- **Deliverable:** Unified query engine with pluggable strategies

### 2. Advanced Index Types
**What:** Support for complex query patterns
- **Composite Indexes**:
  - Index on 2-3 fields: `(field1, field2, field3)`
  - Support prefix queries
  - Query planner: choose composite vs single-field
- **Inverted Index**:
  - For string fields and arrays
  - Support `in` operator: `'value' in .tags`
  - Integration with Roaring bitmaps (union for OR)
- **Deliverable:** Multi-field queries and array containment optimized

### 3. Cost-Based Query Planner
**What:** Implement IQueryPlanner for optimal strategy selection
- Implement cost model (index lookup, bitset build, graph nav, data loading)
- Implement `ExecutionCost` class (latency, I/O, CPU, memory)
- Each executor provides cost estimation via EstimateCost()
- Planner selects executor with minimum cost
- Adaptive threshold adjustment (learn from actual query performance)
- Cost model validation (predicted vs actual)
- **Deliverable:** Automatic strategy selection based on cost, not just selectivity

### 4. Query Optimization (Complex Filtering)
**What:** Optimize complex multi-predicate execution
- Predicate pushdown (evaluate cheapest first)
- Short-circuit evaluation (stop early for AND/OR)
- Index intersection/union (combine multiple indexes efficiently)
- Handle complex expressions: `(A OR B) AND (C OR D)`
- **Deliverable:** Complex filters with multiple predicates execute optimally

### 5. Monitoring & Telemetry
**What:** Observability for production use
- Add query metrics: selectivity, strategy chosen, latency, recall
- Query logging for slow queries
- Index usage statistics (hit rate, size, build time)
- Integration with Garnet metrics (Prometheus/Grafana dashboards)
- **Deliverable:** Full observability into query execution

**Phase 3 Success Criteria:**
- ✅ Multi-predicate queries automatically optimized
- ✅ Cost model accuracy ≥80% correct strategy selection
- ✅ Full metrics and dashboards available

---

**Success Criteria:**
- ✅ Phase 1: Redis VSIM compatible post-filtering (current state)
- ✅ Phase 2: 5x speedup for equality/range filters, full API compatibility maintained
- ✅ Phase 3: Complex queries optimized, cost-based query engine, no breaking changes
- ✅ Backward Compatibility: All existing VSIM queries work without modification

---
