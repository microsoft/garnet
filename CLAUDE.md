# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Garnet is a high-performance remote cache-store from Microsoft Research. It implements the Redis RESP wire protocol, making it compatible with existing Redis clients. Built in C# on .NET 8.0/10.0, it uses a custom storage engine called Tsavorite.

## Build Commands

```bash
# Restore dependencies
dotnet restore

# Build entire solution
dotnet build Garnet.slnx

# Build specific configuration
dotnet build --configuration Release

# Format check (used in CI)
dotnet format Garnet.slnx --no-restore --verify-no-changes --verbosity diagnostic

# Run the server
dotnet run --project main/GarnetServer
```

## Test Commands

```bash
# Run all tests in a project
dotnet test test/Garnet.test -f net8.0

# Run a single test by name
dotnet test test/Garnet.test -f net8.0 --filter "FullyQualifiedName~RespTests.TestMethodName"

# Run tests with detailed output (CI style)
dotnet test test/Garnet.test -f net8.0 --configuration Debug --logger "console;verbosity=detailed" -- NUnit.DisplayName=FullName

# Cluster tests
dotnet test test/Garnet.test.cluster -f net8.0
```

Test projects: `test/Garnet.test/` (main), `test/Garnet.test.cluster/` (cluster), `test/Garnet.fuzz/` (fuzzing).

## Test Patterns

Tests use NUnit with Allure reporting. Every test fixture **must**:
- Inherit from `AllureTestBase`
- Have the `[AllureNUnit]` attribute on the class
- Use `[TestFixture]` attribute

CI enforces these requirements with a validation script.

```csharp
[AllureNUnit]
[TestFixture]
public class MyTests : AllureTestBase
{
    [SetUp]
    public void Setup() { /* create server via TestUtils.CreateGarnetServer() */ }

    [TearDown]
    public void TearDown() { /* dispose server, delete test dirs */ }

    [Test]
    public void MyTest() { /* test logic */ }
}
```

## Architecture

### Layered Design

1. **RESP Wire Protocol** (`libs/server/Resp/`) - Parses Redis protocol commands. `RespServerSession` is the main dispatcher, with commands split across files by category (BasicCommands, KeyAdminCommands, ObjectCommands, etc.).

2. **Storage Engine (Tsavorite)** (`libs/storage/Tsavorite/`) - Git submodule. Provides two key-value stores: a main store for raw strings (GC-optimized) and an object store for complex types (SortedSet, List, Hash, etc.). Uses a "narrow-waist" API of read/upsert/delete/RMW with async callbacks.

3. **Cluster** (`libs/cluster/`) - Sharding via hash slots, gossip-based state, replication, key migration. Passive design (no leader election).

4. **Network** (`libs/server/`) - Shared-memory design where TLS and storage I/O run on the network completion thread to avoid context switches.

### Key Libraries

| Directory | Purpose |
|-----------|---------|
| `libs/server/` | Server core: RESP parsing, command handlers, sessions, ACL, Lua |
| `libs/storage/Tsavorite/` | Storage engine (submodule) |
| `libs/cluster/` | Cluster mode |
| `libs/host/` | Server hosting and configuration |
| `libs/client/` | Client library |
| `libs/common/` | Shared utilities |
| `main/GarnetServer/` | Server entry point |
| `modules/` | Optional modules (GarnetJSON, NoOpModule) |

### Custom Extensibility

Custom RESP commands, procedures, and object types can be defined in C# via `CustomCommandBase`, `CustomProcedureBase`, and `CustomObjectBase` in `libs/server/Custom/`. Modules are loaded dynamically.

### Vector Search (Active Development)

Located in `libs/server/Resp/Vector/`. Uses DiskANN for approximate nearest neighbor search with filter support:
- `Filter/` - Tokenizer, parser, expression AST, evaluator
- `QueryEngine/` - Cost-based query planner with selectivity estimation
- `DiskANNService.cs` - P/Invoke bridge to Rust FFI (`diskann-garnet/` submodule)

DiskANN submodules: `DiskANN/` (rust core), `CDB-DiskANN/` (Cosmos DB variant), `diskann-garnet` are (Rust FFI wrapper). Build the Rust FFI with `cargo build --release` from `diskann-garnet in DiskANN'.

### Local DiskANN Development

By default, `diskann-garnet` pulls its DiskANN dependencies from crates.io and the built native library comes from a NuGet package. For local development against source:

**1. Use local DiskANN Rust source** (skip crates.io publish):
Add `[patch.crates-io]` to `diskann/Cargo.toml`:
```toml
[patch.crates-io]
diskann = { path = "../DiskANN/diskann" }
diskann-providers = { path = "../DiskANN/diskann-providers" }
diskann-vector = { path = "../DiskANN/diskann-vector" }
```
Ensure the version in `[dependencies]` matches the local crate versions, then `cargo update` and `cargo build --release`.

**2. Use locally built native library in Garnet** (skip NuGet package):
```bash
# Build the Rust FFI crate
cd diskann-garnet && cargo build --release

# Build/run/test Garnet with local override
dotnet build -p:DiskANNGarnetLocal=true
dotnet run --project main/GarnetServer -p:DiskANNGarnetLocal=true
dotnet test test/Garnet.test -p:DiskANNGarnetLocal=true
```
The `DiskANNGarnetLocal` MSBuild property (defined in `libs/server/Garnet.server.csproj`) copies `diskann_garnet.dll`/`libdiskann_garnet.so` from `diskann-garnet/target/release/` into the build output, overriding the NuGet-provided binary.

## Build Configuration

- Solution file: `Garnet.slnx`
- Target frameworks: `net8.0` and `net10.0`
- Warnings treated as errors (`TreatWarningsAsErrors=true`)
- Unsafe blocks enabled
- Assembly signing with `Garnet.snk`
- NuGet versions centralized in `Directory.Packages.props`
- Project version in `Version.props`
- File header required: `Copyright (c) Microsoft Corporation. Licensed under the MIT license.`

## CI/CD

GitHub Actions (`.github/workflows/ci.yml`) runs on push to main/dev and PRs:
- Matrix: Windows + Linux, net8.0 + net10.0, Debug + Release
- Validates code formatting, builds, runs Garnet and Tsavorite tests
- 45-minute timeout per job

## Reference Branch: `filter-index-exp-submodule`

This branch builds on top of the team's `vectorApiPoC` branch. The bulk of Vector Set implementation (commands, DiskANN FFI, storage, cluster, locking, etc.) was done by other team members. Below are only **Haiyang's commits** on this branch, which focus on the **filter/query engine** and **post-filter** work.

### Haiyang's Commits (10 commits)

#### Post-Filter Implementation (`f8284c7b55`)
First working version of post-filter for vector search — tokenizer, parser, expression AST, and evaluator.
- `libs/server/Resp/Vector/VectorFilterTokenizer.cs` — lexer for filter expressions (113 lines)
- `libs/server/Resp/Vector/VectorFilterParser.cs` — recursive descent parser (214 lines)
- `libs/server/Resp/Vector/VectorFilterExpression.cs` — AST node types (45 lines)
- `libs/server/Resp/Vector/VectorFilterEvaluator.cs` — evaluates filter expressions against attributes (129 lines)
- `libs/server/Resp/Vector/VectorManager.cs` — integration of filter into VSIM flow (115 lines added)
- `test/Garnet.test/RespVectorSetTests.cs` — filter tests (110 lines added)

#### Minor Fixes (`1673d1192e`, `da4decbcc3`)
- Small fixes in `VectorFilterTokenizer.cs` and `VectorManager.cs`

#### Filter Query Plan Doc (`51ac472ec7`)
- `docs/filter-query.md` — design doc for filter query planning (272 lines)

#### Query Engine Infrastructure (`abe7100ac9` — Phase 2/3)
Cost-based query planner with selectivity estimation, attribute indexes, and filter bitmaps.
- `libs/server/Resp/Vector/Filter/` — moved and reorganized filter files into subfolder
- `libs/server/Resp/Vector/QueryEngine/AttributeIndexManager.cs` — manages hash and range indexes (307 lines)
- `libs/server/Resp/Vector/QueryEngine/ExecutionCost.cs` — cost model for query plans (57 lines)
- `libs/server/Resp/Vector/QueryEngine/ExecutionStrategy.cs` — strategy enum/types (30 lines)
- `libs/server/Resp/Vector/QueryEngine/FilterBitmap.cs` — bitmap for pre-filter candidate sets (211 lines)
- `libs/server/Resp/Vector/QueryEngine/FilterBitmapBuilder.cs` — builds bitmaps from expressions (212 lines)
- `libs/server/Resp/Vector/QueryEngine/HashIndex.cs` — hash-based attribute index (188 lines)
- `libs/server/Resp/Vector/QueryEngine/IAttributeIndex.cs` — index interface (95 lines)
- `libs/server/Resp/Vector/QueryEngine/QueryPlan.cs` — plan representation (56 lines)
- `libs/server/Resp/Vector/QueryEngine/RangeIndex.cs` — range-based attribute index (268 lines)
- `libs/server/Resp/Vector/QueryEngine/SelectivityEstimator.cs` — cardinality/selectivity estimation (193 lines)
- `libs/server/Resp/Vector/QueryEngine/VectorQueryEngine.cs` — orchestrator (172 lines)
- `libs/server/Resp/Vector/QueryEngine/VectorQueryPlanner.cs` — cost-based planner (165 lines)
- `libs/server/Resp/Vector/QueryEngine/VectorQueryResult.cs` — result type (52 lines)
- `libs/server/Resp/Vector/VectorManager.QueryEngine.cs` — VectorManager integration for query engine (309 lines)
- `docs/VectorFilterArchitecture.md` — architecture doc (309 lines)
- `test/Garnet.test/VectorQueryEngineTests.cs` — comprehensive tests (1174 lines)

#### Local Submodule Commits (`7782afdf37`, `eab60f9a60`, `4e26fc97b3`, `6adb6f2e72`, `2b5e72b706`)
- Submodule pointer updates for local DiskANN development

### Uncommitted Changes
- `docs/VectorFilterArchitecture.md` — updates to filter architecture doc
- `libs/server/Garnet.server.csproj` — build config changes
- `libs/server/Resp/Vector/DiskANNService.cs` — DiskANN service changes
- `libs/server/Resp/Vector/VectorManager.QueryEngine.cs` — query engine integration updates
- `docs/filter-query.md` — removed (content merged into architecture doc)

### Playground Tools

**`playground/pinvoke-bench/`** — P/Invoke benchmark measuring FFI call overhead:
- `rust-lib/src/lib.rs` — Rust native library source
- `rust-lib/Cargo.toml`, `rust-lib/Cargo.lock` — Rust project config
- `csharp-bench/Program.cs` — C# BenchmarkDotNet harness
- `csharp-bench/PInvokeBench.csproj` — C# project
- `README.md` — setup and results documentation
- `Directory.Build.props`, `Directory.Packages.props` — isolated build config (not part of main solution)

**`playground/vector-filter-sample/`** — Vector filter demo:
- `Program.cs` — C# client demonstrating vector filter queries against Garnet
- `redis_client_demo.py` — Python Redis client demo for the same
- `VectorFilterSample.csproj` — C# project
- `README.md` — usage instructions

**`playground/VectorSearchBench/`** — Integration benchmark comparing Garnet vector search vs Redis ground-truth:
- `Program.cs` — CLI entry point (CommandLineParser)
- `BenchOptions.cs` — all CLI flags
- `Types.cs` — `IWorkload`, `ITargetClient`, `LatencyStats`, `WorkloadPhaseResult`
- `RedisTargetClient.cs` — StackExchange.Redis adapter for both Garnet and Redis endpoints
- `VectorSearchWorkload.cs` — `vector-search-filter` workload: synthetic VADD load + 3 VSIM query phases (unfiltered, `.year > 2000`, `.genre == "action"`)
- `BenchmarkRunner.cs` — orchestration, warmup, report (p50/p95/p99 latency + Recall@K)
- `VectorSearchBench.csproj` — net8.0 project

**Running the bench:**
```bash
# Terminal 1: start Garnet with vector support
dotnet run --project main/GarnetServer -- --port 6380 --enable-vector-set-preview

# Terminal 2: start Redis 8 (needed for --redis-mode vsim)
docker run -d --name redis-bench -p 6379:6379 redis:8.0.0
# Or Redis Stack (needed for --redis-mode hnsw)
# docker run -d --name redis-bench -p 6379:6379 redis/redis-stack:latest

# Terminal 3: run benchmark
dotnet run --project playground/VectorSearchBench -- \
  --garnet-port 6380 --redis-port 6379 \
  --num-vectors 5000 --dimensions 128 --num-queries 200 --top-k 10
```

Key flags: `--skip-load` reuse existing data, `--seed` for reproducibility, `--workload vsf` alias, `--id-prefix "item:"` for realistic IDs, `--ef 500` Garnet search exploration factor, `--redis-mode vsim` for apples-to-apples comparison.

**Redis modes:**
- `--redis-mode bruteforce` (default): client-side brute-force cosine scan as exact ground-truth. Works with any Redis version.
- `--redis-mode hnsw`: uses RediSearch `FT.CREATE`/`FT.SEARCH` with HNSW index. Requires `redis/redis-stack:latest`. Both Garnet and Redis show recall vs brute-force ground-truth.
- `--redis-mode vsim`: uses Redis 8 `VADD`/`VSIM` — same commands as Garnet for apples-to-apples comparison. Requires `redis:8.0.0`. Uses `VSIM TRUTH` (exact linear scan) for ground-truth.

**Recall definition:** `|target_topK ∩ exactTopK| / K` averaged across all queries.
In brute-force mode, Redis IS the ground-truth so only Garnet shows recall. In HNSW/VSIM modes, both show recall.

### YFCC Workload (`--data-dir`)

The YFCC workload benchmarks Garnet and Redis 8 on filtered vector search using real-world data from the YFCC10M dataset. It is activated by passing `--data-dir <path>` (which auto-sets `--workload yfcc`).

**Dataset:** YFCC10M — 1M (or 10M) Flickr image embeddings, uint8, 192 dimensions. Each vector has JSON attributes: `year`, `month`, `camera`, `country` (country may be absent).

**Data files required** (in the `--data-dir` directory):

| File | Description |
|------|-------------|
| `base.1M.u8bin` | Base vectors: `[u32 count][u32 dims]` header + `count×dims` raw bytes |
| `base.1M.label.jsonl` | One JSON per line: `{"doc_id": N, "year": "2010", "month": "May", "camera": "Panasonic", "country": "US"}` |
| `single_high_query_10k.u8bin` | Query vectors for unfiltered + single-high phase |
| `single_high_query_10k.label.jsonl` | Query filters: `{"query_id": N, "filter": {"year": {"$eq": "2009"}}}` |
| `single_medium_query_10k.u8bin` | Query vectors for single-medium phase |
| `single_medium_query_10k.label.jsonl` | Filters (single equality, medium-frequency values) |
| `single_low_query_10k.u8bin` | Query vectors for single-low phase |
| `single_low_query_10k.label.jsonl` | Filters (single equality, rare values) |
| `multiple_medium_query_10k.u8bin` | Query vectors for multi-medium phase |
| `multiple_medium_query_10k.label.jsonl` | Filters (AND of two predicates, medium selectivity) |
| `query.10k.u8bin` | Query vectors for multi-low phase |
| `multiple_low_query_10k.label.jsonl` | Filters (AND of two predicates, rare values) |
| `yfcc-config.json` | Phase configuration (maps phase names to file paths) |

**yfcc-config.json** maps phase names to their data files. It is searched in: `--yfcc-config` path, then `--data-dir`, then executable directory, then CWD. A copy is also kept in `playground/VectorSearchBench/yfcc-config.json`.

**Vector format differences:** Garnet uses `XB8` (raw uint8 binary) for both `VADD` and `VSIM`. Redis 8 only supports `FP32`/`VALUES`, so the benchmark converts uint8→float32 for the Redis path.

**Ingestion commands:**
```
# Garnet — native uint8
VADD bench:vset XB8 <192_raw_bytes> <id> SETATTR <json>

# Redis 8 — uint8 converted to float32
VADD bench:vset:redis FP32 <768_fp32_bytes> <id> SETATTR <json>
```

**Query command (both servers):**
```
VSIM <key> XB8|FP32 <vec_bytes> COUNT <k> WITHSCORES EF <ef> FILTER "<expr>"
```
Filter expressions use dot-syntax: `.year == "2009"`, `.camera == "NIKON" && .year == "2006"`. The same syntax works on both Garnet and Redis 8.

**Ground truth:** Client-side brute-force cosine similarity with filter applied in-memory. Both Garnet and Redis recall are measured against this same ground truth. `VSIM TRUTH` is not used because it does not support `FILTER`.

**Query phases and measured selectivity** (at 100K base vectors, 200 queries each):

| Phase | Filter example | Avg selectivity | Avg matching records |
|-------|----------------|-----------------|----------------------|
| unfiltered | (none) | 100% | 100,000 |
| single-high | `.year == "2012"` | 15.45% | 15,448 |
| single-medium | `.country == "ES"` | 1.28% | 1,281 |
| single-low | `.camera == "EPSON"` | 0.02% | 16 |
| multi-medium | `.camera == "NIKON" && .year == "2006"` | 1.48% | 1,483 |
| multi-low | `.country == "CA" && .year == "2005"` | 0.02% | 24 |

Selectivity = fraction of base records passing the filter, averaged across the 200 query filters in each phase.

**Running the YFCC benchmark:**
```bash
# Terminal 1: Garnet
dotnet run --project main/GarnetServer -- --port 6380 --enable-vector-set-preview

# Terminal 2: Redis 8
docker run -d --name redis8 -p 6379:6379 redis:8.0.0

# Terminal 3: benchmark
dotnet run --project playground/VectorSearchBench -- \
  --data-dir "<path-to-yfcc-data>" \
  --garnet-port 6380 --redis-port 6379 \
  --yfcc-count 100000 --num-queries 200 --top-k 10 --ef 200
```

Key YFCC flags: `--yfcc-count 100000` limits to first 100K vectors (0 = all), `--ef 200` sets search exploration factor, `--skip-load` reuses already-loaded data, `--yfcc-config <path>` explicit config file path.

**Benchmark report:** See `playground/VectorSearchBench/YFCC-Benchmark-Report.md` for detailed results including latency, recall, and analysis of Garnet's post-filter recall degradation on narrow filters.

### How Element IDs Flow Through the Vector Search Stack

Garnet vector set element IDs are **variable-length byte strings** (arbitrary user-provided names like `"item:42"` or `"my-vector"`). They are NOT fixed-size. Here's how they flow:

**1. RESP → C# (insert path):** The user sends `VADD key VALUES <dim> <floats...> <element-name>`. The element name is an arbitrary RESP bulk string, passed as `ReadOnlySpan<byte>` to `DiskANNService.Insert()` which forwards `id_data`/`id_len` to the Rust FFI `insert()`.

**2. Rust side — `GarnetId`** (`diskann-garnet/src/garnet.rs:464-504`): A wrapper around `Box<[u8]>` that prepends a **4-byte u32 length prefix** before the actual ID bytes. Created via `GarnetId::from(id_bytes)` which allocates `4 + id_len` bytes. `Deref` returns only the ID bytes (skipping the prefix). The prefix exists because Garnet storage callbacks require 4 preceding writable bytes for key operations.

**3. Rust side — search output** (`diskann-garnet/src/lib.rs:91-163`): `SearchResults` implements `SearchOutputBuffer<GarnetId>`. The `set()` method (line 109) writes results into a flat `ids: &mut [u8]` byte buffer using the same length-prefixed format:
```
[u32 len_1][id_bytes_1][u32 len_2][id_bytes_2]...
```
Each entry occupies `4 + id.len()` bytes. The `set()` method walks all previous entries sequentially (lines 119-125) to compute the byte offset for entry `i`, then writes the new entry. **It only bounds-checks the distance index** (`i >= self.dists.len()` at line 115), **not** whether the byte offset exceeds the ids buffer.

**4. C# side — buffer allocation** (`libs/server/Resp/Vector/VectorManager.cs:53, 532-539`):
```csharp
private const int MinimumSpacePerId = sizeof(int) + 8; // = 12 bytes
```
The ids buffer is allocated as `count * 12` bytes. This assumes each ID is at most 8 bytes. The initial stack allocation in `RespServerSessionVectors.cs:715` is `DefaultResultSetSize * DefaultIdSize + DefaultResultSetSize * sizeof(int)` = `64 * 8 + 64 * 4 = 768` bytes.

**5. C# side — reading results back** (`VectorManager.cs:919-971`): The `ApplyPostFilter` method reads the length-prefixed format: `BinaryPrimitives.ReadInt32LittleEndian(idsSpan[pos..])` to get each ID's length, then slices `pos + 4` to read the ID bytes.

### Known Bug: DiskANN Search Output Buffer Overflow

**Status:** Unfixed — documented for future work.

**Symptom:** VSIM search panics in the Rust native library with "range end index N out of range for slice of length M" when element IDs are too long relative to the top-k count.

**Root cause:** Because IDs are **variable-length**, the total bytes needed in the output buffer depends on the actual IDs returned by DiskANN's graph search, which are not known in advance. The C# side allocates `count * 12` bytes (assuming max 8-byte IDs), and `SearchResults::set()` in `lib.rs:109-136` does not bounds-check the ids byte buffer before writing. If the sum of `(4 + id_len)` across all results exceeds the buffer, Rust panics.

**Example:** For top-k=10 with IDs like `"item:4999"` (9 bytes each):
- Buffer allocated: `10 * 12 = 120 bytes`
- Bytes needed: `10 * (4 + 9) = 130 bytes` → overflow

**NuGet version notes:**
- v1.0.20/1.0.21: The pre-built NuGet DLL appears to use `output_distances_len * 8` internally for the IDs buffer (ignoring the actual `output_ids_len` parameter), giving only `top_k * 8` bytes — even smaller than what C# allocates.
- v1.0.22/1.0.23: ABI change causes crash on VADD insert (0xC0000005 access violation). Not usable with current C# code.

**Current workaround:** Use NuGet v1.0.20 (pinned in `Directory.Packages.props`) and keep element IDs short (numeric-only, e.g., `$"{i}"` instead of `$"item:{i}"`). The benchmark `VectorSearchWorkload.cs:323` uses this workaround.

**Proper fix:** Either (a) add a bounds check in `SearchResults::set()` before writing to `self.ids` and return an error/truncate instead of panicking, or (b) increase `MinimumSpacePerId` in `VectorManager.cs` to accommodate realistic ID lengths, or (c) both.

### Suggested PR Decomposition (Haiyang's work only)
1. **Post-filter basics** — VectorFilterTokenizer, Parser, Expression, Evaluator + VectorManager integration + tests
2. **Query engine infrastructure** — QueryEngine/ folder (indexes, planner, cost model, bitmaps, selectivity) + VectorManager.QueryEngine + tests
3. **Docs** — VectorFilterArchitecture.md, DiskANN-Filter-Architecture.md
4. **Playground tools** — pinvoke-bench, vector-filter-sample, VectorSearchBench
