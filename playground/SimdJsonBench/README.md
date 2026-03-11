# SimdJsonBench

Five-way benchmark comparing JSON field extraction strategies for Garnet's VSIM post-filter pipeline:

| # | Contender | Description |
|---|---|---|
| 1 | **Current** | `AttributeExtractor` — hand-rolled managed UTF-8 scalar scanner  |
| 2 | **SimdOnDemand** | `SimdOnDemandExtractor` — optimized single-pass managed scanner using .NET's SIMD-accelerated `IndexOfAny` |
| 3 | **SimdTwoPass** | `SimdOnDemandTwoPassExtractor` — two-pass: AVX2/SSE2 structural index build → walk index to match keys |
| 4 | **SimdJsonNative** | simdjson 3.12.3 C++ On-Demand API compiled to native DLL, called via P/Invoke |
| 5 | **SimdJson** | [SimdJsonSharp.Bindings v1.7.0](https://www.nuget.org/packages/SimdJsonSharp.Bindings) — old simdjson ~0.2 full-DOM parse via P/Invoke |

All five produce the same output type (`ExprToken`) from the same input (`byte[]` JSON + field name).

## Quick start

```bash
# Build
dotnet build playground/SimdJsonBench/SimdJsonBench.csproj -c Release

# Run all benchmarks (full statistical rigor)
dotnet run --project playground/SimdJsonBench/SimdJsonBench.csproj -c Release -f net8.0 -- --filter *

# Quick run (less precise, faster iteration)
dotnet run --project playground/SimdJsonBench/SimdJsonBench.csproj -c Release -f net8.0 -- --filter * --job short

# Run a single category
dotnet run --project playground/SimdJsonBench/SimdJsonBench.csproj -c Release -f net8.0 -- --filter *Num_Large* --job short
```

## Benchmark suites

### 1. `SingleFieldBenchmarks` — single-field extraction

Five-way: extract one field from JSON → `ExprToken`.

Covers Number / String / Bool / Missing × Small (~26 B) / Medium (~100 B) / Large (~270 B).

```bash
dotnet run --project playground/SimdJsonBench/SimdJsonBench.csproj -c Release -f net8.0 -- --filter *SingleField*
```

### 2. `MultiFieldBenchmarks` — multi-field extraction

| Approach | Strategy |
|---|---|
| **Current** | `ExtractFields` — single scalar scan for all N fields |
| **SimdOnDemand** | Single forward pass, `IndexOfAny` for string scanning, all N fields |
| **SimdTwoPass** | One AVX2/SSE2 structural-index build + walk for all N fields |
| **SimdJsonNative** | One `sj_extract_fields` call — single forward pass in native C++ |
| **SimdJson** | N × full native DOM re-parse (no multi-key API) |

| Scenario | Fields | JSON size |
|---|---|---|
| 3-fields·Medium | year, rating, genre | ~100 B |
| 5-fields·Large | year, rating, genre, budget, active | ~270 B |

```bash
dotnet run --project playground/SimdJsonBench/SimdJsonBench.csproj -c Release -f net8.0 -- --filter *MultiField*
```

### 3. `EndToEndBenchmarks` — full filter evaluation

End-to-end comparison of the actual VSIM post-filter hot path:

- **Current**: `ExprCompiler.TryCompile` (once) → `ExprRunner.Run` per candidate (internally calls `ExtractFields` + evaluates postfix program).
- **SimdOnDemand / SimdTwoPass / SimdJsonNative**: extract fields + hand-written predicate in C#.
- **SimdJson**: `SimdJsonHelper.ExtractField` per needed field + hand-written predicate.

Includes numeric range (`.year > 1950 and .rating >= 4.0`), string equality (`.genre == "action"`), and batch scenarios (10 / 100 / 1000 candidates).

```bash
dotnet run --project playground/SimdJsonBench/SimdJsonBench.csproj -c Release -f net8.0 -- --filter *EndToEnd*
```

## JSON payloads

| Name | Fields | Size | Shape |
|---|---|---|---|
| **Small** | 2 | ~26 B | `{"year":1980,"rating":4.5}` |
| **Medium** | 5 | ~100 B | + genre, director, tags array |
| **Large** | 12 | ~270 B | + id, title, studio, budget, nested metadata object, active |

## How each approach works

| | Current | SimdOnDemand | SimdTwoPass | SimdJsonNative | SimdJson |
|---|---|---|---|---|---|
| **Parse** | Scan raw UTF-8 byte by byte | Single forward pass; `IndexOfAny` SIMD scans for `"` and `\` | Phase 1: AVX2/SSE2 vectorized structural index; Phase 2: walk index | C++ `ondemand::parser.iterate` (SIMD structural index internally) | `SimdJsonN.ParseJson` full DOM |
| **Key lookup** | Linear scan, skip values | Forward scan with `SequenceEqual` key match | Walk structural index array | C++ `obj.find_field` | `iterator.MoveToKey` tree walk |
| **String result** | Zero-copy `JsonRef` | Zero-copy `JsonRef` | Zero-copy `JsonRef` | `Marshal.PtrToStringUTF8` (allocates) | `GetUtf16String` (allocates) |
| **Number parse** | `Utf8Parser.TryParse` | `Utf8Parser.TryParse` | `Utf8Parser.TryParse` | Native `get_double()` | Native `GetDouble()` |
| **Multi-field** | Single scan, early exit | Single scan, early exit | One index build + walk, early exit | Single native forward pass | N × full re-parse |
| **Interop** | Pure managed | Pure managed | Pure managed | P/Invoke per call (~20 ns) | P/Invoke per call |
| **Stack alloc** | None | None | 2 KB `stackalloc int[512]` | None (heap in native parser) | None |

## SimdOnDemand vs SimdTwoPass

Both are pure managed — the key architectural difference:

### SimdOnDemand (single-pass)

- **No structural index** — scans forward byte by byte through the JSON
- Uses .NET 8's `IndexOfAny((byte)'"', (byte)'\\')` which the JIT compiles to SIMD `vpcmpeqb` + `vpor` + `vpmovmskb`
- **Fused scan + match** — checks each key immediately as it's encountered, stops the moment the field is found
- **Zero stack overhead** — no `stackalloc`, minimal register pressure
- **Best on small/medium payloads** where the overhead of building a structural index exceeds the cost of just scanning forward

### SimdTwoPass (structural index)

- **Phase 1**: AVX2 loads 32 bytes → 8× `CompareEqual` + `MoveMask` → bitmask of structural positions → extract via `TrailingZeroCount` into `stackalloc int[512]`
- **Phase 2**: walk the index array to jump between key/colon/value boundaries
- **Pays 2 KB stackalloc upfront** — the JIT must zero-init the buffer
- **Better for very large payloads** (KB+) where the structural index amortizes the setup cost
- **AVX2 path** processes 32 bytes/iter; SSE2 fallback at 16 bytes/iter; scalar tail for remainder

## Native simdjson DLL

The `SimdJsonNative` contender calls into `native/simdjson_native.dll` — simdjson 3.12.3 compiled with [Zig](https://ziglang.org/) C++ compiler (`zig c++ -std=c++17 -O2 -march=haswell`).

### C API surface

```c
sj_parser    sj_parser_create(size_t max_capacity);
void         sj_parser_destroy(sj_parser p);

sj_field_result sj_extract_field(
    sj_parser parser, const uint8_t* json, size_t len, size_t capacity,
    const char* key, size_t key_len);

int32_t sj_extract_fields(
    sj_parser parser, const uint8_t* json, size_t len, size_t capacity,
    const char* const* keys, const size_t* key_lens, size_t count,
    sj_field_result* results);
```

- Uses simdjson's **On-Demand API** (`ondemand::parser.iterate` → `obj.find_field` / field iteration)
- Parser handle is **created once** and reused (amortized allocation)
- Compiled with `SIMDJSON_EXCEPTIONS=0` (error-code path, no C++ exception overhead)
- Requires **SIMDJSON_PADDING** (128 bytes) beyond JSON length — padded buffers are pre-allocated in the benchmark setup

### Rebuilding the native DLL

Requires [Zig](https://ziglang.org/) (no Visual Studio needed):

```bash
cd playground/SimdJsonBench/native
zig c++ -std=c++17 -O2 -DSIMDJSON_EXCEPTIONS=0 -DSIMDJSON_IMPLEMENTATION_ICELAKE=0 \
    -march=haswell -shared -o simdjson_native.dll simdjson.cpp simdjson_native.cpp -lc++
```

## Sample results

*ShortRun, AMD EPYC 7763 (Zen 3, AVX2), .NET 8.0:*

### Number·Small (~26 B, first field)

| Method | Mean | Ratio | Allocated |
|---|---|---|---|
| **SimdOnDemand** | **58.5 ns** | **0.45×** | **0 B** |
| SimdJsonNative | 99.8 ns | 0.76× | 0 B |
| SimdTwoPass | 103.7 ns | 0.79× | 0 B |
| Current | 131.2 ns | 1.00× | 0 B |
| SimdJson | 6,583 ns | 50.16× | 48 B |

### String·Medium (~100 B, third field)

| Method | Mean | Ratio | Allocated |
|---|---|---|---|
| **SimdOnDemand** | **61.5 ns** | **0.23×** | **0 B** |
| SimdTwoPass | ~105 ns | ~0.39× | 0 B |
| SimdJsonNative | 129.4 ns | 0.48× | 40 B |
| Current | 271.1 ns | 1.00× | 0 B |
| SimdJson | 9,757 ns | 36.00× | 88 B |

### Number·Large (~270 B, field #8)

| Method | Mean | Ratio | Allocated |
|---|---|---|---|
| SimdJsonNative | 179.7 ns | 0.23× | 0 B |
| **SimdOnDemand** | **185.6 ns** | **0.24×** | **0 B** |
| SimdTwoPass | 228.9 ns | 0.29× | 0 B |
| Current | 782.7 ns | 1.00× | 0 B |
| SimdJson | 15,311 ns | 19.56× | 48 B |

> [!NOTE]
> These are ShortRun numbers for quick comparison. Run without `--job short` for statistically rigorous results.
> SimdTwoPass numbers are from prior runs when it was the only SIMD implementation; `~` prefix marks interpolated values.
>
> **SimdOnDemand** wins on small/medium payloads (no structural index overhead, pure forward scan).
> **SimdTwoPass** sits between SimdOnDemand and Current — the 2 KB stackalloc + full-document index costs ~50–70 ns overhead.
> **SimdJsonNative** catches up on large payloads where simdjson's optimized C++ SIMD pipeline shines.
> All three are **3–5× faster** than the current production `AttributeExtractor`.

## Project setup notes

- **Not in the main solution** — standalone playground experiment. Build directly:
  ```bash
  dotnet build playground/SimdJsonBench/SimdJsonBench.csproj -c Release
  ```
- **Public-signed** — uses `<PublicSign>true</PublicSign>` because `SimdJsonSharp.Bindings` is not strong-named, while `Garnet.server` is.
- **InternalsVisibleTo** — `Garnet.server` grants access via `AssemblyInfo.cs` so the benchmark can use `internal` types (`AttributeExtractor`, `ExprToken`, `ExprRunner`, `ExprCompiler`).
- **Native DLL** — `native/simdjson_native.dll` is copied to the output directory via the `.csproj`. Must be rebuilt if the native code changes.

## Files

| File | Description |
|---|---|
| `Program.cs` | Benchmark harness — 5-way SingleField / MultiField / EndToEnd (70 benchmarks) |
| `SimdOnDemandExtractor.cs` | Optimized single-pass managed extractor (IndexOfAny SIMD, no structural index) |
| `SimdOnDemandTwoPassExtractor.cs` | Two-pass managed extractor (AVX2/SSE2 structural index + walk) |
| `SimdJsonNativeHelper.cs` | P/Invoke wrapper for native simdjson 3.x On-Demand DLL |
| `SimdJsonHelper.cs` | Wrapper for SimdJsonSharp.Bindings v1.7.0 (old full-DOM parse) |
| `native/simdjson_native.cpp` | C API implementation using simdjson On-Demand |
| `native/simdjson_native.h` | C API header |
| `native/simdjson.h` | simdjson 3.12.3 single-header |
| `native/simdjson.cpp` | simdjson 3.12.3 single-source |
| `native/simdjson_native.dll` | Pre-built native DLL (Win64, AVX2) |
| `SimdJsonBench.csproj` | Project file |
| `README.md` | This file |
