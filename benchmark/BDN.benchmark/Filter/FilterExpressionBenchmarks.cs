// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Binary;
using System.Text;
using BenchmarkDotNet.Attributes;
using Garnet.server;

namespace BDN.benchmark.Filter
{
    // ════════════════════════════════════════════════════════════════════════
    //  1. COMPILATION  (one-time cost per VSIM query)
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>Compile filter string → postfix program.</summary>
    [MemoryDiagnoser]
    public class FilterCompileBenchmarks
    {
        private byte[] _comparison;
        private byte[] _logicalAnd;
        private byte[] _stringEq;
        private byte[] _arithmetic;
        private byte[] _containment;
        private byte[] _combined;

        [GlobalSetup]
        public void Setup()
        {
            _comparison = ".year > 1950"u8.ToArray();
            _logicalAnd = ".year > 1950 and .rating >= 4.0"u8.ToArray();
            _stringEq = ".genre == \"action\""u8.ToArray();
            _arithmetic = "(.year - 2000) ** 2 < 100"u8.ToArray();
            _containment = "\"classic\" in .tags"u8.ToArray();
            _combined = ".rating * 2 > 8 and (.year >= 1980 or \"modern\" in .tags) and .genre == \"action\""u8.ToArray();
        }

        private static void Compile(byte[] filter)
        {
            Span<ExprToken> instrBuf = stackalloc ExprToken[128];
            Span<ExprToken> tuplePoolBuf = stackalloc ExprToken[64];
            Span<ExprToken> tokensBuf = stackalloc ExprToken[128];
            Span<ExprToken> opsStackBuf = stackalloc ExprToken[128];
            ExprCompiler.TryCompile(filter, instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf, out _, out _);
        }

        [Benchmark(Description = "Comparison (.year > N)")]
        public void Comparison() => Compile(_comparison);

        [Benchmark(Description = "Logical AND (2 clauses)")]
        public void LogicalAnd() => Compile(_logicalAnd);

        [Benchmark(Description = "String equality")]
        public void StringEq() => Compile(_stringEq);

        [Benchmark(Description = "Arithmetic + power")]
        public void Arithmetic() => Compile(_arithmetic);

        [Benchmark(Description = "Containment (in)")]
        public void Containment() => Compile(_containment);

        [Benchmark(Description = "Combined (all ops)")]
        public void Combined() => Compile(_combined);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  2. FIELD EXTRACTION  (per candidate, per selector)
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Extract a single field from raw JSON bytes.
    /// Parameterized by JSON size: Small (2 fields), Medium (5), Large (12 + nested obj).
    /// </summary>
    [MemoryDiagnoser]
    public class FilterExtractBenchmarks
    {
        // Small:  {"year":1980,"rating":4.5}
        // Medium: {"year":1980,"rating":4.5,"genre":"action","director":"Spielberg","tags":["classic","popular"]}
        // Large:  12 fields including nested object and 3-element array
        private byte[] _small;
        private byte[] _medium;
        private byte[] _large;

        [GlobalSetup]
        public void Setup()
        {
            _small = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5}");
            _medium = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"tags\":[\"classic\",\"popular\"]}");
            _large = Encoding.UTF8.GetBytes("{\"id\":12345,\"title\":\"Test Movie\",\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"studio\":\"Universal\",\"budget\":50000000,\"tags\":[\"classic\",\"popular\",\"award-winning\"],\"metadata\":{\"source\":\"imdb\",\"verified\":true},\"active\":true}");
        }

        // --- Number fields (zero-alloc) ---
        [Benchmark(Description = "Number · Small JSON (1st field)")]
        public void Num_Small() => AttributeExtractor.ExtractField(_small, "year"u8);

        [Benchmark(Description = "Number · Medium JSON (2nd field)")]
        public void Num_Medium() => AttributeExtractor.ExtractField(_medium, "rating"u8);

        [Benchmark(Description = "Number · Large JSON (skip 8 fields)")]
        public void Num_Large() => AttributeExtractor.ExtractField(_large, "budget"u8);

        // --- String fields (zero-alloc) ---
        [Benchmark(Description = "String · Medium JSON")]
        public void Str_Medium() => AttributeExtractor.ExtractField(_medium, "genre"u8);

        [Benchmark(Description = "String · Large JSON (skip 5)")]
        public void Str_Large() => AttributeExtractor.ExtractField(_large, "director"u8);

        // --- Array fields (zero-alloc with runtime pool) ---
        [Benchmark(Description = "Array[2] · Medium JSON")]
        public void Arr_Medium() => AttributeExtractor.ExtractField(_medium, "tags"u8);

        [Benchmark(Description = "Array[3] · Large JSON")]
        public void Arr_Large() => AttributeExtractor.ExtractField(_large, "tags"u8);

        // --- Boolean (zero-alloc) ---
        [Benchmark(Description = "Boolean · Large JSON (skip nested obj)")]
        public void Bool_Large() => AttributeExtractor.ExtractField(_large, "active"u8);

        // --- Missing field (zero-alloc) ---
        [Benchmark(Description = "Missing · Small JSON")]
        public void Miss_Small() => AttributeExtractor.ExtractField(_small, "missing"u8);

        [Benchmark(Description = "Missing · Medium JSON")]
        public void Miss_Medium() => AttributeExtractor.ExtractField(_medium, "missing"u8);

        [Benchmark(Description = "Missing · Large JSON")]
        public void Miss_Large() => AttributeExtractor.ExtractField(_large, "missing"u8);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  3. EXECUTION BY EXPRESSION TYPE  (compile-once, run per candidate)
    //     Fixed JSON: Medium (5 fields, includes array)
    //     Ordered: most frequent → least frequent real-world query patterns
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Run pre-compiled filters against medium JSON.
    /// Ordered from most common to least common real-world usage patterns.
    /// </summary>
    [MemoryDiagnoser]
    public class FilterRunByExprBenchmarks
    {
        private byte[] _comparisonFilter;
        private byte[] _logicalAndFilter;
        private byte[] _stringEqFilter;
        private byte[] _containsArrayFilter;
        private byte[] _logicalOrFilter;
        private byte[] _notFilter;
        private byte[] _stringNeqFilter;
        private byte[] _arithmeticFilter;
        private byte[] _powerFilter;
        private byte[] _containsStringFilter;
        private byte[] _combinedFilter;
        private byte[] _json;

        [GlobalSetup]
        public void Setup()
        {
            _comparisonFilter = ".year > 1950"u8.ToArray();
            _logicalAndFilter = ".year > 1950 and .rating >= 4.0"u8.ToArray();
            _stringEqFilter = ".genre == \"action\""u8.ToArray();
            _containsArrayFilter = "\"classic\" in .tags"u8.ToArray();
            _logicalOrFilter = ".year < 1960 or .rating > 4.0"u8.ToArray();
            _notFilter = "not (.genre == \"drama\")"u8.ToArray();
            _stringNeqFilter = ".genre != \"drama\""u8.ToArray();
            _arithmeticFilter = ".rating * 2 > 8"u8.ToArray();
            _powerFilter = "(.year - 2000) ** 2 < 100"u8.ToArray();
            _containsStringFilter = "\"act\" in .genre"u8.ToArray();
            _combinedFilter = ".rating * 2 > 8 and (.year >= 1980 or \"modern\" in .tags) and .genre == \"action\""u8.ToArray();
            _json = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"tags\":[\"classic\",\"popular\"]}");
        }

        /// <summary>
        /// Compile filter, build ExprProgram, extract fields, and evaluate — all on the stack.
        /// </summary>
        private static bool RunFilter(byte[] filterBytes, byte[] json)
        {
            Span<ExprToken> instrBuf = stackalloc ExprToken[128];
            Span<ExprToken> tuplePoolBuf = stackalloc ExprToken[64];
            Span<ExprToken> tokensBuf = stackalloc ExprToken[128];
            Span<ExprToken> opsStackBuf = stackalloc ExprToken[128];
            var instrCount = ExprCompiler.TryCompile(filterBytes, instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf, out var tupleCount, out _);
            if (instrCount < 0) return false;

            Span<ExprToken> runtimePoolBuf = stackalloc ExprToken[64];
            var program = new ExprProgram
            {
                Instructions = instrBuf[..instrCount],
                Length = instrCount,
                TuplePool = tuplePoolBuf[..tupleCount],
                TuplePoolLength = tupleCount,
                RuntimePool = runtimePoolBuf,
                RuntimePoolLength = 0,
            };

            Span<(int Start, int Length)> selectorBuf = stackalloc (int, int)[32];
            var selectorCount = VectorManager.GetSelectorRanges(program.Instructions, program.Length, filterBytes, selectorBuf);
            var selectorRanges = selectorBuf[..selectorCount];

            Span<ExprToken> extractedFields = stackalloc ExprToken[selectorCount > 0 ? selectorCount : 1];
            program.ResetRuntimePool();
            AttributeExtractor.ExtractFields(json, filterBytes, selectorRanges, extractedFields, ref program);

            Span<ExprToken> stackBuf = stackalloc ExprToken[16];
            var stack = new ExprStack(stackBuf);
            return ExprRunner.Run(ref program, json, filterBytes, selectorRanges, extractedFields, ref stack);
        }

        // ── Common: range / categorical ──────────────────────────────────

        [Benchmark(Description = "1. .year > N  (range)")]
        public bool Comparison() => RunFilter(_comparisonFilter, _json);

        [Benchmark(Description = "2. .year > N and .rating >= M  (multi-range)")]
        public bool LogicalAnd() => RunFilter(_logicalAndFilter, _json);

        [Benchmark(Description = "3. .genre == \"action\"  (category)")]
        public bool StringEq() => RunFilter(_stringEqFilter, _json);

        [Benchmark(Description = "4. \"x\" in .tags  (tag search)")]
        public bool InArray() => RunFilter(_containsArrayFilter, _json);

        // ── Moderate: logical combinations ───────────────────────────────

        [Benchmark(Description = "5. A or B  (logical OR)")]
        public bool LogicalOr() => RunFilter(_logicalOrFilter, _json);

        [Benchmark(Description = "6. not (A)  (exclusion)")]
        public bool Not() => RunFilter(_notFilter, _json);

        [Benchmark(Description = "7. .genre != \"drama\"  (not-equal)")]
        public bool StringNeq() => RunFilter(_stringNeqFilter, _json);

        // ── Less common: computed / advanced ─────────────────────────────

        [Benchmark(Description = "8. .rating * 2 > 8  (arithmetic)")]
        public bool Arithmetic() => RunFilter(_arithmeticFilter, _json);

        [Benchmark(Description = "9. (.year-2000)**2 < 100  (power)")]
        public bool Power() => RunFilter(_powerFilter, _json);

        [Benchmark(Description = "10. \"act\" in .genre  (substring)")]
        public bool InString() => RunFilter(_containsStringFilter, _json);

        // ── Realistic combined ───────────────────────────────────────────

        [Benchmark(Description = "11. Combined (all ops)")]
        public bool Combined() => RunFilter(_combinedFilter, _json);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  4. EXECUTION BY JSON COMPLEXITY  (fixed filter, varying JSON)
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Same filter run against small / medium / large JSON.
    /// Shows how JSON size affects extraction + evaluation time.
    /// </summary>
    [MemoryDiagnoser]
    public class FilterRunByJsonBenchmarks
    {
        private byte[] _numericFilterBytes;
        private byte[] _arrayFilterBytes;

        private byte[] _small;   // 2 fields, no array
        private byte[] _medium;  // 5 fields, 2-element array
        private byte[] _large;   // 12 fields, 3-element array, nested object

        [GlobalSetup]
        public void Setup()
        {
            _numericFilterBytes = ".year > 1950 and .rating >= 4.0"u8.ToArray();
            _arrayFilterBytes = "\"classic\" in .tags"u8.ToArray();

            _small = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5}");
            _medium = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"tags\":[\"classic\",\"popular\"]}");
            _large = Encoding.UTF8.GetBytes("{\"id\":12345,\"title\":\"Test Movie\",\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"studio\":\"Universal\",\"budget\":50000000,\"tags\":[\"classic\",\"popular\",\"award-winning\"],\"metadata\":{\"source\":\"imdb\",\"verified\":true},\"active\":true}");
        }

        private static bool RunFilter(byte[] filterBytes, byte[] json)
        {
            Span<ExprToken> instrBuf = stackalloc ExprToken[128];
            Span<ExprToken> tuplePoolBuf = stackalloc ExprToken[64];
            Span<ExprToken> tokensBuf = stackalloc ExprToken[128];
            Span<ExprToken> opsStackBuf = stackalloc ExprToken[128];
            var instrCount = ExprCompiler.TryCompile(filterBytes, instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf, out var tupleCount, out _);
            if (instrCount < 0) return false;

            Span<ExprToken> runtimePoolBuf = stackalloc ExprToken[64];
            var program = new ExprProgram
            {
                Instructions = instrBuf[..instrCount],
                Length = instrCount,
                TuplePool = tuplePoolBuf[..tupleCount],
                TuplePoolLength = tupleCount,
                RuntimePool = runtimePoolBuf,
                RuntimePoolLength = 0,
            };

            Span<(int Start, int Length)> selectorBuf = stackalloc (int, int)[32];
            var selectorCount = VectorManager.GetSelectorRanges(program.Instructions, program.Length, filterBytes, selectorBuf);
            var selectorRanges = selectorBuf[..selectorCount];

            Span<ExprToken> extractedFields = stackalloc ExprToken[selectorCount > 0 ? selectorCount : 1];
            program.ResetRuntimePool();
            AttributeExtractor.ExtractFields(json, filterBytes, selectorRanges, extractedFields, ref program);

            Span<ExprToken> stackBuf = stackalloc ExprToken[16];
            var stack = new ExprStack(stackBuf);
            return ExprRunner.Run(ref program, json, filterBytes, selectorRanges, extractedFields, ref stack);
        }

        // --- Numeric filter ---
        [Benchmark(Description = "Numeric AND · Small JSON")]
        public bool Numeric_Small() => RunFilter(_numericFilterBytes, _small);

        [Benchmark(Description = "Numeric AND · Medium JSON")]
        public bool Numeric_Medium() => RunFilter(_numericFilterBytes, _medium);

        [Benchmark(Description = "Numeric AND · Large JSON")]
        public bool Numeric_Large() => RunFilter(_numericFilterBytes, _large);

        // --- Array filter ---
        [Benchmark(Description = "in .tags · Small JSON (no tags → false)")]
        public bool Array_Small() => RunFilter(_arrayFilterBytes, _small);

        [Benchmark(Description = "in .tags · Medium JSON (2 elem)")]
        public bool Array_Medium() => RunFilter(_arrayFilterBytes, _medium);

        [Benchmark(Description = "in .tags · Large JSON (3 elem)")]
        public bool Array_Large() => RunFilter(_arrayFilterBytes, _large);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  5. BATCH  (compile once, run N candidates)
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Simulate real VSIM post-filtering: compile once, evaluate N candidates.
    /// Shows total allocation and throughput at scale.
    /// </summary>
    [MemoryDiagnoser]
    public class FilterBatchBenchmarks
    {
        private byte[] _numericAndFilter;
        private byte[] _combinedFilter;
        private byte[] _small;
        private byte[] _medium;

        [GlobalSetup]
        public void Setup()
        {
            _numericAndFilter = ".year > 1950 and .rating >= 4.0"u8.ToArray();
            _combinedFilter = ".rating * 2 > 8 and (.year >= 1980 or \"modern\" in .tags) and .genre == \"action\""u8.ToArray();
            _small = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5}");
            _medium = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"tags\":[\"classic\",\"popular\"]}");
        }

        private static int RunBatch(byte[] filterBytes, byte[] small, byte[] medium, int N)
        {
            Span<ExprToken> instrBuf = stackalloc ExprToken[128];
            Span<ExprToken> tuplePoolBuf = stackalloc ExprToken[64];
            Span<ExprToken> tokensBuf = stackalloc ExprToken[128];
            Span<ExprToken> opsStackBuf = stackalloc ExprToken[128];
            var instrCount = ExprCompiler.TryCompile(filterBytes, instrBuf, tuplePoolBuf, tokensBuf, opsStackBuf, out var tupleCount, out _);
            if (instrCount < 0) return 0;

            Span<ExprToken> runtimePoolBuf = stackalloc ExprToken[64];
            var program = new ExprProgram
            {
                Instructions = instrBuf[..instrCount],
                Length = instrCount,
                TuplePool = tuplePoolBuf[..tupleCount],
                TuplePoolLength = tupleCount,
                RuntimePool = runtimePoolBuf,
                RuntimePoolLength = 0,
            };

            Span<(int Start, int Length)> selectorBuf = stackalloc (int, int)[32];
            var selectorCount = VectorManager.GetSelectorRanges(program.Instructions, program.Length, filterBytes, selectorBuf);
            var selectorRanges = selectorBuf[..selectorCount];

            Span<ExprToken> extractedFields = stackalloc ExprToken[selectorCount > 0 ? selectorCount : 1];
            Span<ExprToken> stackBuf = stackalloc ExprToken[16];
            var stack = new ExprStack(stackBuf);

            var matched = 0;
            for (var i = 0; i < N; i++)
            {
                var json = (i % 3 == 0) ? small : medium;
                program.ResetRuntimePool();
                AttributeExtractor.ExtractFields(json, filterBytes, selectorRanges, extractedFields, ref program);
                if (ExprRunner.Run(ref program, json, filterBytes, selectorRanges, extractedFields, ref stack))
                    matched++;
            }
            return matched;
        }

        [Benchmark(Description = "Numeric AND · N candidates (zero-alloc)")]
        [Arguments(10)]
        [Arguments(100)]
        [Arguments(1000)]
        public int NumericAnd(int N) => RunBatch(_numericAndFilter, _small, _medium, N);

        [Benchmark(Description = "Combined + array · N candidates")]
        [Arguments(10)]
        [Arguments(100)]
        [Arguments(1000)]
        public int Combined(int N) => RunBatch(_combinedFilter, _small, _medium, N);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  6. END-TO-END ApplyPostFilter  (compile + extract + evaluate N candidates)
    //     Exercises the full pipeline including length-prefixed attribute span layout
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// End-to-end benchmark of <see cref="VectorManager.ApplyPostFilter"/>.
    /// Builds a realistic length-prefixed attribute span (as produced by VSIM),
    /// then calls ApplyPostFilter which compiles the filter, extracts fields,
    /// and evaluates each candidate.
    /// </summary>
    [MemoryDiagnoser]
    public class FilterApplyPostFilterBenchmarks
    {
        // Filters of varying complexity
        private byte[] _numericFilter;
        private byte[] _stringFilter;
        private byte[] _arrayFilter;
        private byte[] _combinedFilter;

        // Pre-built length-prefixed attribute spans for different candidate counts
        private byte[] _attrs10;
        private byte[] _attrs100;
        private byte[] _attrs1000;

        // Bitmap buffers (ceil(N/8) bytes)
        private byte[] _bitmap10;
        private byte[] _bitmap100;
        private byte[] _bitmap1000;

        // Scratch buffer for ApplyPostFilter
        private ScratchBufferBuilder _scratchBufferBuilder;

        [GlobalSetup]
        public void Setup()
        {
            _scratchBufferBuilder = new ScratchBufferBuilder();

            _numericFilter = ".year > 1950 and .rating >= 4.0"u8.ToArray();
            _stringFilter = ".genre == \"action\""u8.ToArray();
            _arrayFilter = "\"classic\" in .tags"u8.ToArray();
            _combinedFilter = ".rating * 2 > 8 and (.year >= 1980 or \"classic\" in .tags) and .genre == \"action\""u8.ToArray();

            // Build diverse JSON candidates — mix of matching and non-matching
            var candidates = new[]
            {
                // Matches numeric+string+combined: year>1950, rating>=4.0, genre=action
                Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"tags\":[\"classic\",\"popular\"]}"),
                // Matches numeric only: year>1950, rating>=4.0, genre=drama
                Encoding.UTF8.GetBytes("{\"year\":2005,\"rating\":4.2,\"genre\":\"drama\",\"director\":\"Nolan\",\"tags\":[\"modern\"]}"),
                // Doesn't match numeric: year<1950
                Encoding.UTF8.GetBytes("{\"year\":1940,\"rating\":3.8,\"genre\":\"noir\",\"director\":\"Wilder\"}"),
                // Matches all: year>1950, rating>=4.0, genre=action, has classic tag
                Encoding.UTF8.GetBytes("{\"year\":1999,\"rating\":4.9,\"genre\":\"action\",\"director\":\"Wachowski\",\"tags\":[\"classic\",\"scifi\",\"popular\"]}"),
                // Large JSON (12 fields) — matches numeric
                Encoding.UTF8.GetBytes("{\"id\":12345,\"title\":\"Test Movie\",\"year\":2010,\"rating\":4.1,\"genre\":\"comedy\",\"director\":\"Anderson\",\"studio\":\"Fox\",\"budget\":50000000,\"tags\":[\"indie\"],\"metadata\":{\"source\":\"imdb\",\"verified\":true},\"active\":true}"),
                // Small JSON — doesn't match (missing fields)
                Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5}"),
                // Large JSON — matches combined
                Encoding.UTF8.GetBytes("{\"id\":99,\"title\":\"Action Hero\",\"year\":2020,\"rating\":4.8,\"genre\":\"action\",\"director\":\"Bay\",\"studio\":\"Paramount\",\"budget\":200000000,\"tags\":[\"classic\",\"blockbuster\"],\"metadata\":{\"source\":\"rotten\"},\"active\":true}"),
            };

            _attrs10 = BuildAttributeSpan(candidates, 10);
            _attrs100 = BuildAttributeSpan(candidates, 100);
            _attrs1000 = BuildAttributeSpan(candidates, 1000);

            _bitmap10 = new byte[(10 + 7) / 8];
            _bitmap100 = new byte[(100 + 7) / 8];
            _bitmap1000 = new byte[(1000 + 7) / 8];
        }

        /// <summary>
        /// Build a length-prefixed attribute span: for each candidate, write [int32 len][json bytes].
        /// Cycles through the candidate array to fill N entries.
        /// </summary>
        private static byte[] BuildAttributeSpan(byte[][] candidates, int count)
        {
            // Calculate total size
            var totalSize = 0;
            for (var i = 0; i < count; i++)
                totalSize += sizeof(int) + candidates[i % candidates.Length].Length;

            var result = new byte[totalSize];
            var offset = 0;
            for (var i = 0; i < count; i++)
            {
                var json = candidates[i % candidates.Length];
                BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(offset), json.Length);
                offset += sizeof(int);
                json.CopyTo(result, offset);
                offset += json.Length;
            }
            return result;
        }

        // ── Numeric filter: .year > 1950 and .rating >= 4.0 ────────────

        [Benchmark(Description = "Numeric AND · 10 candidates")]
        public int Numeric_10() => VectorManager.ApplyPostFilter(_numericFilter, 10, _attrs10, _bitmap10, _scratchBufferBuilder);

        [Benchmark(Description = "Numeric AND · 100 candidates")]
        public int Numeric_100() => VectorManager.ApplyPostFilter(_numericFilter, 100, _attrs100, _bitmap100, _scratchBufferBuilder);

        [Benchmark(Description = "Numeric AND · 1000 candidates")]
        public int Numeric_1000() => VectorManager.ApplyPostFilter(_numericFilter, 1000, _attrs1000, _bitmap1000, _scratchBufferBuilder);

        // ── String filter: .genre == "action" ───────────────────────────

        [Benchmark(Description = "String EQ · 10 candidates")]
        public int String_10() => VectorManager.ApplyPostFilter(_stringFilter, 10, _attrs10, _bitmap10, _scratchBufferBuilder);

        [Benchmark(Description = "String EQ · 100 candidates")]
        public int String_100() => VectorManager.ApplyPostFilter(_stringFilter, 100, _attrs100, _bitmap100, _scratchBufferBuilder);

        [Benchmark(Description = "String EQ · 1000 candidates")]
        public int String_1000() => VectorManager.ApplyPostFilter(_stringFilter, 1000, _attrs1000, _bitmap1000, _scratchBufferBuilder);

        // ── Array filter: "classic" in .tags ────────────────────────────

        [Benchmark(Description = "Array IN · 10 candidates")]
        public int Array_10() => VectorManager.ApplyPostFilter(_arrayFilter, 10, _attrs10, _bitmap10, _scratchBufferBuilder);

        [Benchmark(Description = "Array IN · 100 candidates")]
        public int Array_100() => VectorManager.ApplyPostFilter(_arrayFilter, 100, _attrs100, _bitmap100, _scratchBufferBuilder);

        [Benchmark(Description = "Array IN · 1000 candidates")]
        public int Array_1000() => VectorManager.ApplyPostFilter(_arrayFilter, 1000, _attrs1000, _bitmap1000, _scratchBufferBuilder);

        // ── Combined filter: rating*2>8 and (year>=1980 or "classic" in .tags) and genre=="action"

        [Benchmark(Description = "Combined · 10 candidates")]
        public int Combined_10() => VectorManager.ApplyPostFilter(_combinedFilter, 10, _attrs10, _bitmap10, _scratchBufferBuilder);

        [Benchmark(Description = "Combined · 100 candidates")]
        public int Combined_100() => VectorManager.ApplyPostFilter(_combinedFilter, 100, _attrs100, _bitmap100, _scratchBufferBuilder);

        [Benchmark(Description = "Combined · 1000 candidates")]
        public int Combined_1000() => VectorManager.ApplyPostFilter(_combinedFilter, 1000, _attrs1000, _bitmap1000, _scratchBufferBuilder);
    }
}