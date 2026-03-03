// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using BenchmarkDotNet.Attributes;
using Garnet.server.Vector.Filter;

namespace BDN.benchmark.Filter
{
    // ════════════════════════════════════════════════════════════════════════
    //  1. COMPILATION  (one-time cost per VSIM query)
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>Compile filter string → postfix program. Always allocates (List, Stack, ExprToken[]).</summary>
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

        [Benchmark(Description = "Comparison (.year > N)")]
        public void Comparison() => ExprCompiler.TryCompile(_comparison, out _);

        [Benchmark(Description = "Logical AND (2 clauses)")]
        public void LogicalAnd() => ExprCompiler.TryCompile(_logicalAnd, out _);

        [Benchmark(Description = "String equality")]
        public void StringEq() => ExprCompiler.TryCompile(_stringEq, out _);

        [Benchmark(Description = "Arithmetic + power")]
        public void Arithmetic() => ExprCompiler.TryCompile(_arithmetic, out _);

        [Benchmark(Description = "Containment (in)")]
        public void Containment() => ExprCompiler.TryCompile(_containment, out _);

        [Benchmark(Description = "Combined (all ops)")]
        public void Combined() => ExprCompiler.TryCompile(_combined, out _);
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
        public void Num_Small() => AttributeExtractor.ExtractField(_small, "year");

        [Benchmark(Description = "Number · Medium JSON (2nd field)")]
        public void Num_Medium() => AttributeExtractor.ExtractField(_medium, "rating");

        [Benchmark(Description = "Number · Large JSON (skip 8 fields)")]
        public void Num_Large() => AttributeExtractor.ExtractField(_large, "budget");

        // --- String fields (zero-alloc for non-escaped) ---
        [Benchmark(Description = "String · Medium JSON")]
        public void Str_Medium() => AttributeExtractor.ExtractField(_medium, "genre");

        [Benchmark(Description = "String · Large JSON (skip 5)")]
        public void Str_Large() => AttributeExtractor.ExtractField(_large, "director");

        // --- Array fields (ALLOCATES ExprToken[count]) ---
        [Benchmark(Description = "Array[2] · Medium JSON → alloc")]
        public void Arr_Medium() => AttributeExtractor.ExtractField(_medium, "tags");

        [Benchmark(Description = "Array[3] · Large JSON → alloc")]
        public void Arr_Large() => AttributeExtractor.ExtractField(_large, "tags");

        // --- Boolean (zero-alloc) ---
        [Benchmark(Description = "Boolean · Large JSON (skip nested obj)")]
        public void Bool_Large() => AttributeExtractor.ExtractField(_large, "active");

        // --- Missing field (zero-alloc) ---
        [Benchmark(Description = "Missing · Small JSON")]
        public void Miss_Small() => AttributeExtractor.ExtractField(_small, "missing");

        [Benchmark(Description = "Missing · Medium JSON")]
        public void Miss_Medium() => AttributeExtractor.ExtractField(_medium, "missing");

        [Benchmark(Description = "Missing · Large JSON")]
        public void Miss_Large() => AttributeExtractor.ExtractField(_large, "missing");
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
        // --- Common: range / categorical filters ---
        private ExprProgram _comparison;        // .year > 1950
        private ExprProgram _logicalAnd;        // .year > 1950 and .rating >= 4.0
        private ExprProgram _stringEq;          // .genre == "action"
        private ExprProgram _containsArray;     // "classic" in .tags

        // --- Moderate: logical combinations ---
        private ExprProgram _logicalOr;         // .year < 1960 or .rating > 4.0
        private ExprProgram _not;               // not (.genre == "drama")
        private ExprProgram _stringNeq;         // .genre != "drama"

        // --- Less common: computed / advanced ---
        private ExprProgram _arithmetic;        // .rating * 2 > 8
        private ExprProgram _power;             // (.year - 2000) ** 2 < 100
        private ExprProgram _containsString;    // "act" in .genre  (substring)

        // --- Realistic combined ---
        private ExprProgram _combined;          // all ops together

        private byte[] _json;
        private Stack<ExprToken> _stack;

        [GlobalSetup]
        public void Setup()
        {
            _comparison = ExprCompiler.TryCompile(".year > 1950"u8, out _);
            _logicalAnd = ExprCompiler.TryCompile(".year > 1950 and .rating >= 4.0"u8, out _);
            _stringEq = ExprCompiler.TryCompile(".genre == \"action\""u8, out _);
            _containsArray = ExprCompiler.TryCompile("\"classic\" in .tags"u8, out _);
            _logicalOr = ExprCompiler.TryCompile(".year < 1960 or .rating > 4.0"u8, out _);
            _not = ExprCompiler.TryCompile("not (.genre == \"drama\")"u8, out _);
            _stringNeq = ExprCompiler.TryCompile(".genre != \"drama\""u8, out _);
            _arithmetic = ExprCompiler.TryCompile(".rating * 2 > 8"u8, out _);
            _power = ExprCompiler.TryCompile("(.year - 2000) ** 2 < 100"u8, out _);
            _containsString = ExprCompiler.TryCompile("\"act\" in .genre"u8, out _);
            _combined = ExprCompiler.TryCompile(".rating * 2 > 8 and (.year >= 1980 or \"modern\" in .tags) and .genre == \"action\""u8, out _);

            _json = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"tags\":[\"classic\",\"popular\"]}");
            _stack = ExprRunner.CreateStack();
        }

        // ── Common: range / categorical ──────────────────────────────────

        [Benchmark(Description = "1. .year > N  (range)")]
        public bool Comparison() => ExprRunner.Run(_comparison, _json, _stack);

        [Benchmark(Description = "2. .year > N and .rating >= M  (multi-range)")]
        public bool LogicalAnd() => ExprRunner.Run(_logicalAnd, _json, _stack);

        [Benchmark(Description = "3. .genre == \"action\"  (category)")]
        public bool StringEq() => ExprRunner.Run(_stringEq, _json, _stack);

        [Benchmark(Description = "4. \"x\" in .tags  (tag search) → ALLOC")]
        public bool InArray() => ExprRunner.Run(_containsArray, _json, _stack);

        // ── Moderate: logical combinations ───────────────────────────────

        [Benchmark(Description = "5. A or B  (logical OR)")]
        public bool LogicalOr() => ExprRunner.Run(_logicalOr, _json, _stack);

        [Benchmark(Description = "6. not (A)  (exclusion)")]
        public bool Not() => ExprRunner.Run(_not, _json, _stack);

        [Benchmark(Description = "7. .genre != \"drama\"  (not-equal)")]
        public bool StringNeq() => ExprRunner.Run(_stringNeq, _json, _stack);

        // ── Less common: computed / advanced ─────────────────────────────

        [Benchmark(Description = "8. .rating * 2 > 8  (arithmetic)")]
        public bool Arithmetic() => ExprRunner.Run(_arithmetic, _json, _stack);

        [Benchmark(Description = "9. (.year-2000)**2 < 100  (power)")]
        public bool Power() => ExprRunner.Run(_power, _json, _stack);

        [Benchmark(Description = "10. \"act\" in .genre  (substring)")]
        public bool InString() => ExprRunner.Run(_containsString, _json, _stack);

        // ── Realistic combined ───────────────────────────────────────────

        [Benchmark(Description = "11. Combined (all ops) → ALLOC")]
        public bool Combined() => ExprRunner.Run(_combined, _json, _stack);
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
        private ExprProgram _numericFilter;
        private ExprProgram _arrayFilter;

        private byte[] _small;   // 2 fields, no array
        private byte[] _medium;  // 5 fields, 2-element array
        private byte[] _large;   // 12 fields, 3-element array, nested object

        private Stack<ExprToken> _stack;

        [GlobalSetup]
        public void Setup()
        {
            _numericFilter = ExprCompiler.TryCompile(".year > 1950 and .rating >= 4.0"u8, out _);
            _arrayFilter = ExprCompiler.TryCompile("\"classic\" in .tags"u8, out _);

            _small = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5}");
            _medium = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"tags\":[\"classic\",\"popular\"]}");
            _large = Encoding.UTF8.GetBytes("{\"id\":12345,\"title\":\"Test Movie\",\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"studio\":\"Universal\",\"budget\":50000000,\"tags\":[\"classic\",\"popular\",\"award-winning\"],\"metadata\":{\"source\":\"imdb\",\"verified\":true},\"active\":true}");
            _stack = ExprRunner.CreateStack();
        }

        // --- Numeric filter (zero-alloc regardless of JSON size) ---
        [Benchmark(Description = "Numeric AND · Small JSON")]
        public bool Numeric_Small() => ExprRunner.Run(_numericFilter, _small, _stack);

        [Benchmark(Description = "Numeric AND · Medium JSON")]
        public bool Numeric_Medium() => ExprRunner.Run(_numericFilter, _medium, _stack);

        [Benchmark(Description = "Numeric AND · Large JSON")]
        public bool Numeric_Large() => ExprRunner.Run(_numericFilter, _large, _stack);

        // --- Array filter (allocates when array is found) ---
        [Benchmark(Description = "in .tags · Small JSON (no tags → false)")]
        public bool Array_Small() => ExprRunner.Run(_arrayFilter, _small, _stack);

        [Benchmark(Description = "in .tags · Medium JSON (2 elem) → alloc")]
        public bool Array_Medium() => ExprRunner.Run(_arrayFilter, _medium, _stack);

        [Benchmark(Description = "in .tags · Large JSON (3 elem) → alloc")]
        public bool Array_Large() => ExprRunner.Run(_arrayFilter, _large, _stack);
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
        private ExprProgram _numericAnd;
        private ExprProgram _combined;
        private byte[] _small;
        private byte[] _medium;
        private Stack<ExprToken> _stack;

        [GlobalSetup]
        public void Setup()
        {
            _numericAnd = ExprCompiler.TryCompile(".year > 1950 and .rating >= 4.0"u8, out _);
            _combined = ExprCompiler.TryCompile(".rating * 2 > 8 and (.year >= 1980 or \"modern\" in .tags) and .genre == \"action\""u8, out _);
            _small = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5}");
            _medium = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"tags\":[\"classic\",\"popular\"]}");
            _stack = ExprRunner.CreateStack();
        }

        [Benchmark(Description = "Numeric AND · N candidates (zero-alloc)")]
        [Arguments(10)]
        [Arguments(100)]
        [Arguments(1000)]
        public int NumericAnd(int N)
        {
            var matched = 0;
            for (var i = 0; i < N; i++)
            {
                var json = (i % 3 == 0) ? _small : _medium;
                if (ExprRunner.Run(_numericAnd, json, _stack)) matched++;
            }
            return matched;
        }

        [Benchmark(Description = "Combined + array · N candidates (allocs)")]
        [Arguments(10)]
        [Arguments(100)]
        [Arguments(1000)]
        public int Combined(int N)
        {
            var matched = 0;
            for (var i = 0; i < N; i++)
            {
                var json = (i % 3 == 0) ? _small : _medium;
                if (ExprRunner.Run(_combined, json, _stack)) matched++;
            }
            return matched;
        }
    }
}