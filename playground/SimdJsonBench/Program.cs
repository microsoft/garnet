// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#nullable enable

using System.Runtime.CompilerServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using Garnet.server.Vector.Filter;
using CurrentExtractor = SimdJsonBench.AttributeExtractor;

// ════════════════════════════════════════════════════════════════════════════
//  Entry point — run all benchmark classes.
//  Each class uses [GroupBenchmarksBy(ByCategory)] so BDN prints one
//  comparison table per category (e.g. "Number·Small", "Number·Large", etc.)
//  with Ratio and Alloc columns right in the summary output.
//
//  Usage:
//    dotnet run -c Release -f net8.0 -- --filter *               # all
//    dotnet run -c Release -f net8.0 -- --filter *SingleField*   # one class
//    dotnet run -c Release -f net8.0 -- --filter *Num_Large      # one category
//    Add --job short for quick (less precise) runs.
// ════════════════════════════════════════════════════════════════════════════

// Default to --filter * when no args given (avoids the interactive picker)
if (args.Length == 0)
    args = ["--filter", "*"];

BenchmarkSwitcher
    .FromTypes([typeof(SingleFieldBenchmarks), typeof(MultiFieldBenchmarks), typeof(EndToEndBenchmarks)])
    .Run(args);

// ════════════════════════════════════════════════════════════════════════════
//  Shared JSON payloads + pre-encoded UTF-8 field names
// ════════════════════════════════════════════════════════════════════════════

public static class JsonPayloads
{
    // Small: 2 fields, ~26 bytes
    public static readonly byte[] Small =
        Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5}");

    // Medium: 5 fields, ~100 bytes
    public static readonly byte[] Medium =
        Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"tags\":[\"classic\",\"popular\"]}");

    // Large: 12 fields including nested object and 3-element array, ~270 bytes
    public static readonly byte[] Large =
        Encoding.UTF8.GetBytes("{\"id\":12345,\"title\":\"Test Movie\",\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"studio\":\"Universal\",\"budget\":50000000,\"tags\":[\"classic\",\"popular\",\"award-winning\"],\"metadata\":{\"source\":\"imdb\",\"verified\":true},\"active\":true}");

    // Pre-encoded UTF-8 keys for SimdOnDemand (no null terminator needed)
    public static readonly byte[] KeyYear = "year"u8.ToArray();
    public static readonly byte[] KeyRating = "rating"u8.ToArray();
    public static readonly byte[] KeyBudget = "budget"u8.ToArray();
    public static readonly byte[] KeyGenre = "genre"u8.ToArray();
    public static readonly byte[] KeyDirector = "director"u8.ToArray();
    public static readonly byte[] KeyActive = "active"u8.ToArray();
    public static readonly byte[] KeyMissing = "missing"u8.ToArray();

    // Padded copies for native simdjson (requires SIMDJSON_PADDING = 128 extra bytes)
    public static readonly byte[] SmallPadded = SimdJsonNativeHelper.MakePadded(Small);
    public static readonly byte[] MediumPadded = SimdJsonNativeHelper.MakePadded(Medium);
    public static readonly byte[] LargePadded = SimdJsonNativeHelper.MakePadded(Large);
    public static readonly int SmallLen = Small.Length;
    public static readonly int MediumLen = Medium.Length;
    public static readonly int LargeLen = Large.Length;
}

// ════════════════════════════════════════════════════════════════════════════
//  1. SINGLE-FIELD extraction — three-way comparison
// ════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Five-way comparison: byte[] JSON + field name → ExprToken.
///
/// <b>Current</b>         — <see cref="AttributeExtractor.ExtractField"/> (managed scalar UTF-8 scanner).
/// <b>SimdOnDemand</b>    — <see cref="SimdOnDemandExtractor.ExtractField"/> (managed single-pass with SIMD IndexOfAny).
/// <b>SimdTwoPass</b>     — <see cref="SimdOnDemandTwoPassExtractor.ExtractField"/> (managed AVX2/SSE2 structural index + walk).
/// <b>SimdJsonNative</b>  — simdjson 3.x On-Demand API (native C++ DLL via P/Invoke).
/// <b>SimdJson</b>        — SimdJsonSharp.Bindings (native simdjson v0.2 full-DOM parse via P/Invoke).
/// </summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class SingleFieldBenchmarks
{
    private byte[] _small = null!;
    private byte[] _medium = null!;
    private byte[] _large = null!;

    // SimdJsonSharp needs null-terminated ASCII keys
    private byte[] _kYear = null!;
    private byte[] _kRating = null!;
    private byte[] _kBudget = null!;
    private byte[] _kGenre = null!;
    private byte[] _kDirector = null!;
    private byte[] _kActive = null!;
    private byte[] _kMissing = null!;

    [GlobalSetup]
    public void Setup()
    {
        _small = JsonPayloads.Small;
        _medium = JsonPayloads.Medium;
        _large = JsonPayloads.Large;

        _kYear = SimdJsonHelper.NullTerminated("year");
        _kRating = SimdJsonHelper.NullTerminated("rating");
        _kBudget = SimdJsonHelper.NullTerminated("budget");
        _kGenre = SimdJsonHelper.NullTerminated("genre");
        _kDirector = SimdJsonHelper.NullTerminated("director");
        _kActive = SimdJsonHelper.NullTerminated("active");
        _kMissing = SimdJsonHelper.NullTerminated("missing");
    }

    // ── Number ───────────────────────────────────────────────────────────

    [BenchmarkCategory("Number·Small"), Benchmark(Baseline = true)]
    public void Current_Num_Small() => Consume(CurrentExtractor.ExtractField(_small, "year"));

    [BenchmarkCategory("Number·Small"), Benchmark]
    public void SimdOnDemand_Num_Small() => Consume(SimdOnDemandExtractor.ExtractField(_small, JsonPayloads.KeyYear));
    [BenchmarkCategory("Number·Small"), Benchmark]
    public void SimdTwoPass_Num_Small() => Consume(SimdOnDemandTwoPassExtractor.ExtractField(_small, JsonPayloads.KeyYear));

    [BenchmarkCategory("Number·Small"), Benchmark]
    public void SimdJson_Num_Small() => Consume(SimdJsonHelper.ExtractField(_small, _kYear));

    [BenchmarkCategory("Number·Small"), Benchmark]
    public void SimdJsonNative_Num_Small() => Consume(SimdJsonNativeHelper.ExtractField(JsonPayloads.SmallPadded, JsonPayloads.SmallLen, JsonPayloads.KeyYear));

    [BenchmarkCategory("Number·Medium"), Benchmark(Baseline = true)]
    public void Current_Num_Medium() => Consume(CurrentExtractor.ExtractField(_medium, "rating"));

    [BenchmarkCategory("Number·Medium"), Benchmark]
    public void SimdOnDemand_Num_Medium() => Consume(SimdOnDemandExtractor.ExtractField(_medium, JsonPayloads.KeyRating));
    [BenchmarkCategory("Number·Medium"), Benchmark]
    public void SimdTwoPass_Num_Medium() => Consume(SimdOnDemandTwoPassExtractor.ExtractField(_medium, JsonPayloads.KeyRating));

    [BenchmarkCategory("Number·Medium"), Benchmark]
    public void SimdJson_Num_Medium() => Consume(SimdJsonHelper.ExtractField(_medium, _kRating));

    [BenchmarkCategory("Number·Medium"), Benchmark]
    public void SimdJsonNative_Num_Medium() => Consume(SimdJsonNativeHelper.ExtractField(JsonPayloads.MediumPadded, JsonPayloads.MediumLen, JsonPayloads.KeyRating));

    [BenchmarkCategory("Number·Large"), Benchmark(Baseline = true)]
    public void Current_Num_Large() => Consume(CurrentExtractor.ExtractField(_large, "budget"));

    [BenchmarkCategory("Number·Large"), Benchmark]
    public void SimdOnDemand_Num_Large() => Consume(SimdOnDemandExtractor.ExtractField(_large, JsonPayloads.KeyBudget));
    [BenchmarkCategory("Number·Large"), Benchmark]
    public void SimdTwoPass_Num_Large() => Consume(SimdOnDemandTwoPassExtractor.ExtractField(_large, JsonPayloads.KeyBudget));

    [BenchmarkCategory("Number·Large"), Benchmark]
    public void SimdJson_Num_Large() => Consume(SimdJsonHelper.ExtractField(_large, _kBudget));

    [BenchmarkCategory("Number·Large"), Benchmark]
    public void SimdJsonNative_Num_Large() => Consume(SimdJsonNativeHelper.ExtractField(JsonPayloads.LargePadded, JsonPayloads.LargeLen, JsonPayloads.KeyBudget));

    // ── String ───────────────────────────────────────────────────────────

    [BenchmarkCategory("String·Medium"), Benchmark(Baseline = true)]
    public void Current_Str_Medium() => Consume(CurrentExtractor.ExtractField(_medium, "genre"));

    [BenchmarkCategory("String·Medium"), Benchmark]
    public void SimdOnDemand_Str_Medium() => Consume(SimdOnDemandExtractor.ExtractField(_medium, JsonPayloads.KeyGenre));
    [BenchmarkCategory("String·Medium"), Benchmark]
    public void SimdTwoPass_Str_Medium() => Consume(SimdOnDemandTwoPassExtractor.ExtractField(_medium, JsonPayloads.KeyGenre));

    [BenchmarkCategory("String·Medium"), Benchmark]
    public void SimdJson_Str_Medium() => Consume(SimdJsonHelper.ExtractField(_medium, _kGenre));

    [BenchmarkCategory("String·Medium"), Benchmark]
    public void SimdJsonNative_Str_Medium() => Consume(SimdJsonNativeHelper.ExtractField(JsonPayloads.MediumPadded, JsonPayloads.MediumLen, JsonPayloads.KeyGenre));

    [BenchmarkCategory("String·Large"), Benchmark(Baseline = true)]
    public void Current_Str_Large() => Consume(CurrentExtractor.ExtractField(_large, "director"));

    [BenchmarkCategory("String·Large"), Benchmark]
    public void SimdOnDemand_Str_Large() => Consume(SimdOnDemandExtractor.ExtractField(_large, JsonPayloads.KeyDirector));
    [BenchmarkCategory("String·Large"), Benchmark]
    public void SimdTwoPass_Str_Large() => Consume(SimdOnDemandTwoPassExtractor.ExtractField(_large, JsonPayloads.KeyDirector));

    [BenchmarkCategory("String·Large"), Benchmark]
    public void SimdJson_Str_Large() => Consume(SimdJsonHelper.ExtractField(_large, _kDirector));

    [BenchmarkCategory("String·Large"), Benchmark]
    public void SimdJsonNative_Str_Large() => Consume(SimdJsonNativeHelper.ExtractField(JsonPayloads.LargePadded, JsonPayloads.LargeLen, JsonPayloads.KeyDirector));

    // ── Bool ─────────────────────────────────────────────────────────────

    [BenchmarkCategory("Bool·Large"), Benchmark(Baseline = true)]
    public void Current_Bool_Large() => Consume(CurrentExtractor.ExtractField(_large, "active"));

    [BenchmarkCategory("Bool·Large"), Benchmark]
    public void SimdOnDemand_Bool_Large() => Consume(SimdOnDemandExtractor.ExtractField(_large, JsonPayloads.KeyActive));
    [BenchmarkCategory("Bool·Large"), Benchmark]
    public void SimdTwoPass_Bool_Large() => Consume(SimdOnDemandTwoPassExtractor.ExtractField(_large, JsonPayloads.KeyActive));

    [BenchmarkCategory("Bool·Large"), Benchmark]
    public void SimdJson_Bool_Large() => Consume(SimdJsonHelper.ExtractField(_large, _kActive));

    [BenchmarkCategory("Bool·Large"), Benchmark]
    public void SimdJsonNative_Bool_Large() => Consume(SimdJsonNativeHelper.ExtractField(JsonPayloads.LargePadded, JsonPayloads.LargeLen, JsonPayloads.KeyActive));

    // ── Missing key ──────────────────────────────────────────────────────

    [BenchmarkCategory("Missing·Small"), Benchmark(Baseline = true)]
    public void Current_Miss_Small() => Consume(CurrentExtractor.ExtractField(_small, "missing"));

    [BenchmarkCategory("Missing·Small"), Benchmark]
    public void SimdOnDemand_Miss_Small() => Consume(SimdOnDemandExtractor.ExtractField(_small, JsonPayloads.KeyMissing));
    [BenchmarkCategory("Missing·Small"), Benchmark]
    public void SimdTwoPass_Miss_Small() => Consume(SimdOnDemandTwoPassExtractor.ExtractField(_small, JsonPayloads.KeyMissing));

    [BenchmarkCategory("Missing·Small"), Benchmark]
    public void SimdJson_Miss_Small() => Consume(SimdJsonHelper.ExtractField(_small, _kMissing));

    [BenchmarkCategory("Missing·Small"), Benchmark]
    public void SimdJsonNative_Miss_Small() => Consume(SimdJsonNativeHelper.ExtractField(JsonPayloads.SmallPadded, JsonPayloads.SmallLen, JsonPayloads.KeyMissing));

    [BenchmarkCategory("Missing·Medium"), Benchmark(Baseline = true)]
    public void Current_Miss_Medium() => Consume(CurrentExtractor.ExtractField(_medium, "missing"));

    [BenchmarkCategory("Missing·Medium"), Benchmark]
    public void SimdOnDemand_Miss_Medium() => Consume(SimdOnDemandExtractor.ExtractField(_medium, JsonPayloads.KeyMissing));
    [BenchmarkCategory("Missing·Medium"), Benchmark]
    public void SimdTwoPass_Miss_Medium() => Consume(SimdOnDemandTwoPassExtractor.ExtractField(_medium, JsonPayloads.KeyMissing));

    [BenchmarkCategory("Missing·Medium"), Benchmark]
    public void SimdJson_Miss_Medium() => Consume(SimdJsonHelper.ExtractField(_medium, _kMissing));

    [BenchmarkCategory("Missing·Medium"), Benchmark]
    public void SimdJsonNative_Miss_Medium() => Consume(SimdJsonNativeHelper.ExtractField(JsonPayloads.MediumPadded, JsonPayloads.MediumLen, JsonPayloads.KeyMissing));

    [BenchmarkCategory("Missing·Large"), Benchmark(Baseline = true)]
    public void Current_Miss_Large() => Consume(CurrentExtractor.ExtractField(_large, "missing"));

    [BenchmarkCategory("Missing·Large"), Benchmark]
    public void SimdOnDemand_Miss_Large() => Consume(SimdOnDemandExtractor.ExtractField(_large, JsonPayloads.KeyMissing));
    [BenchmarkCategory("Missing·Large"), Benchmark]
    public void SimdTwoPass_Miss_Large() => Consume(SimdOnDemandTwoPassExtractor.ExtractField(_large, JsonPayloads.KeyMissing));

    [BenchmarkCategory("Missing·Large"), Benchmark]
    public void SimdJson_Miss_Large() => Consume(SimdJsonHelper.ExtractField(_large, _kMissing));

    [BenchmarkCategory("Missing·Large"), Benchmark]
    public void SimdJsonNative_Miss_Large() => Consume(SimdJsonNativeHelper.ExtractField(JsonPayloads.LargePadded, JsonPayloads.LargeLen, JsonPayloads.KeyMissing));

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void Consume(ExprToken _) { }
}

// ════════════════════════════════════════════════════════════════════════════
//  2. MULTI-FIELD extraction — three-way comparison
// ════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Three-way multi-field comparison:
///
/// <b>Current</b>      — single-pass <see cref="AttributeExtractor.ExtractFields"/> (one scan for N fields).
/// <b>SimdOnDemand</b> — single structural-index pass + on-demand walk for N fields.
/// <b>SimdJson</b>     — N × (ParseJson + MoveToKey) — one full native re-parse per field.
/// </summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class MultiFieldBenchmarks
{
    private byte[] _medium = null!;
    private byte[] _large = null!;

    // AttributeExtractor
    private string[] _mediumFields = null!;
    private string[] _largeFields = null!;
    private ExprToken[] _mediumResults = null!;
    private ExprToken[] _largeResults = null!;

    // SimdOnDemand (UTF-8 byte keys)
    private byte[][] _mediumKeysUtf8 = null!;
    private byte[][] _largeKeysUtf8 = null!;
    private ExprToken[] _mediumOnDemandResults = null!;
    private ExprToken[] _largeOnDemandResults = null!;

    // SimdTwoPass (same UTF-8 byte keys, separate result arrays)
    private ExprToken[] _mediumTwoPassResults = null!;
    private ExprToken[] _largeTwoPassResults = null!;

    // SimdJsonSharp (null-terminated keys)
    private byte[][] _mediumSimdKeys = null!;
    private byte[][] _largeSimdKeys = null!;
    private ExprToken[] _mediumSimdResults = null!;
    private ExprToken[] _largeSimdResults = null!;

    // SimdJsonNative (UTF-8 byte keys, padded JSON)
    private ExprToken[] _mediumNativeResults = null!;
    private ExprToken[] _largeNativeResults = null!;

    [GlobalSetup]
    public void Setup()
    {
        _medium = JsonPayloads.Medium;
        _large = JsonPayloads.Large;

        _mediumFields = ["year", "rating", "genre"];
        _largeFields = ["year", "rating", "genre", "budget", "active"];

        _mediumResults = new ExprToken[_mediumFields.Length];
        _largeResults = new ExprToken[_largeFields.Length];

        _mediumKeysUtf8 = [JsonPayloads.KeyYear, JsonPayloads.KeyRating, JsonPayloads.KeyGenre];
        _largeKeysUtf8 = [JsonPayloads.KeyYear, JsonPayloads.KeyRating, JsonPayloads.KeyGenre, JsonPayloads.KeyBudget, JsonPayloads.KeyActive];

        _mediumOnDemandResults = new ExprToken[_mediumFields.Length];
        _largeOnDemandResults = new ExprToken[_largeFields.Length];

        _mediumTwoPassResults = new ExprToken[_mediumFields.Length];
        _largeTwoPassResults = new ExprToken[_largeFields.Length];

        _mediumSimdKeys = Array.ConvertAll(_mediumFields, SimdJsonHelper.NullTerminated);
        _largeSimdKeys = Array.ConvertAll(_largeFields, SimdJsonHelper.NullTerminated);

        _mediumSimdResults = new ExprToken[_mediumFields.Length];
        _largeSimdResults = new ExprToken[_largeFields.Length];

        _mediumNativeResults = new ExprToken[_mediumFields.Length];
        _largeNativeResults = new ExprToken[_largeFields.Length];
    }

    // ── Medium JSON, 3 fields ────────────────────────────────────────────

    [BenchmarkCategory("3-fields·Medium"), Benchmark(Baseline = true)]
    public int Current_3F_Medium()
        => CurrentExtractor.ExtractFields(_medium, _mediumFields, _mediumResults);

    [BenchmarkCategory("3-fields·Medium"), Benchmark]
    public int SimdOnDemand_3F_Medium()
        => SimdOnDemandExtractor.ExtractFields(_medium, _mediumKeysUtf8, _mediumOnDemandResults);

    [BenchmarkCategory("3-fields·Medium"), Benchmark]
    public int SimdTwoPass_3F_Medium()
        => SimdOnDemandTwoPassExtractor.ExtractFields(_medium, _mediumKeysUtf8, _mediumTwoPassResults);

    [BenchmarkCategory("3-fields·Medium"), Benchmark]
    public int SimdJson_3F_Medium()
        => SimdJsonHelper.ExtractFields(_medium, _mediumSimdKeys, _mediumSimdResults);

    [BenchmarkCategory("3-fields·Medium"), Benchmark]
    public int SimdJsonNative_3F_Medium()
        => SimdJsonNativeHelper.ExtractFields(JsonPayloads.MediumPadded, JsonPayloads.MediumLen, _mediumKeysUtf8, _mediumNativeResults);

    // ── Large JSON, 5 fields ─────────────────────────────────────────────

    [BenchmarkCategory("5-fields·Large"), Benchmark(Baseline = true)]
    public int Current_5F_Large()
        => CurrentExtractor.ExtractFields(_large, _largeFields, _largeResults);

    [BenchmarkCategory("5-fields·Large"), Benchmark]
    public int SimdOnDemand_5F_Large()
        => SimdOnDemandExtractor.ExtractFields(_large, _largeKeysUtf8, _largeOnDemandResults);

    [BenchmarkCategory("5-fields·Large"), Benchmark]
    public int SimdTwoPass_5F_Large()
        => SimdOnDemandTwoPassExtractor.ExtractFields(_large, _largeKeysUtf8, _largeTwoPassResults);

    [BenchmarkCategory("5-fields·Large"), Benchmark]
    public int SimdJson_5F_Large()
        => SimdJsonHelper.ExtractFields(_large, _largeSimdKeys, _largeSimdResults);

    [BenchmarkCategory("5-fields·Large"), Benchmark]
    public int SimdJsonNative_5F_Large()
        => SimdJsonNativeHelper.ExtractFields(JsonPayloads.LargePadded, JsonPayloads.LargeLen, _largeKeysUtf8, _largeNativeResults);
}

// ════════════════════════════════════════════════════════════════════════════
//  3. END-TO-END filter evaluation — three-way comparison
// ════════════════════════════════════════════════════════════════════════════

/// <summary>
/// End-to-end filter evaluation — the actual VSIM post-filter hot path.
///
/// <b>Current</b>      — ExprRunner.Run (ExtractFields + postfix VM).
/// <b>SimdOnDemand</b> — SimdOnDemandExtractor + hand-written predicate.
/// <b>SimdJson</b>     — SimdJsonSharp + hand-written predicate.
/// </summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class EndToEndBenchmarks
{
    private ExprProgram _numericFilter;
    private ExprProgram _stringFilter;
    private byte[] _medium = null!;
    private Stack<ExprToken> _stack = null!;

    // SimdJsonSharp keys
    private byte[] _kYear = null!;
    private byte[] _kRating = null!;
    private byte[] _kGenre = null!;

    [GlobalSetup]
    public void Setup()
    {
        _numericFilter = ExprCompiler.TryCompile(".year > 1950 and .rating >= 4.0"u8, out _);
        _stringFilter = ExprCompiler.TryCompile(".genre == \"action\""u8, out _);

        _medium = JsonPayloads.Medium;
        _stack = ExprRunner.CreateStack();

        _kYear = SimdJsonHelper.NullTerminated("year");
        _kRating = SimdJsonHelper.NullTerminated("rating");
        _kGenre = SimdJsonHelper.NullTerminated("genre");
    }

    // ── Numeric range ────────────────────────────────────────────────────

    [BenchmarkCategory("NumericRange"), Benchmark(Baseline = true)]
    public bool Current_NumRange()
        => ExprRunner.Run(_numericFilter, _medium, _stack);

    [BenchmarkCategory("NumericRange"), Benchmark]
    public bool SimdOnDemand_NumRange()
    {
        var year = SimdOnDemandExtractor.ExtractField(_medium, JsonPayloads.KeyYear);
        var rating = SimdOnDemandExtractor.ExtractField(_medium, JsonPayloads.KeyRating);
        return year.Num > 1950 && rating.Num >= 4.0;
    }

    [BenchmarkCategory("NumericRange"), Benchmark]
    public bool SimdTwoPass_NumRange()
    {
        var year = SimdOnDemandTwoPassExtractor.ExtractField(_medium, JsonPayloads.KeyYear);
        var rating = SimdOnDemandTwoPassExtractor.ExtractField(_medium, JsonPayloads.KeyRating);
        return year.Num > 1950 && rating.Num >= 4.0;
    }

    [BenchmarkCategory("NumericRange"), Benchmark]
    public bool SimdJson_NumRange()
    {
        var year = SimdJsonHelper.ExtractField(_medium, _kYear);
        var rating = SimdJsonHelper.ExtractField(_medium, _kRating);
        return year.Num > 1950 && rating.Num >= 4.0;
    }

    [BenchmarkCategory("NumericRange"), Benchmark]
    public bool SimdJsonNative_NumRange()
    {
        var year = SimdJsonNativeHelper.ExtractField(JsonPayloads.MediumPadded, JsonPayloads.MediumLen, JsonPayloads.KeyYear);
        var rating = SimdJsonNativeHelper.ExtractField(JsonPayloads.MediumPadded, JsonPayloads.MediumLen, JsonPayloads.KeyRating);
        return year.Num > 1950 && rating.Num >= 4.0;
    }

    // ── String equality ──────────────────────────────────────────────────

    [BenchmarkCategory("StringEq"), Benchmark(Baseline = true)]
    public bool Current_StrEq()
        => ExprRunner.Run(_stringFilter, _medium, _stack);

    [BenchmarkCategory("StringEq"), Benchmark]
    public bool SimdOnDemand_StrEq()
    {
        var genre = SimdOnDemandExtractor.ExtractField(_medium, JsonPayloads.KeyGenre);
        return genre.Str == "action";
    }

    [BenchmarkCategory("StringEq"), Benchmark]
    public bool SimdTwoPass_StrEq()
    {
        var genre = SimdOnDemandTwoPassExtractor.ExtractField(_medium, JsonPayloads.KeyGenre);
        return genre.Str == "action";
    }

    [BenchmarkCategory("StringEq"), Benchmark]
    public bool SimdJson_StrEq()
    {
        var genre = SimdJsonHelper.ExtractField(_medium, _kGenre);
        return genre.Str == "action";
    }

    [BenchmarkCategory("StringEq"), Benchmark]
    public bool SimdJsonNative_StrEq()
    {
        var genre = SimdJsonNativeHelper.ExtractField(JsonPayloads.MediumPadded, JsonPayloads.MediumLen, JsonPayloads.KeyGenre);
        return genre.Str == "action";
    }

    // ── Batch: N candidates ──────────────────────────────────────────────

    [BenchmarkCategory("Batch"), Benchmark(Baseline = true)]
    [Arguments(10)]
    [Arguments(100)]
    [Arguments(1000)]
    public int Current_Batch(int N)
    {
        var matched = 0;
        for (var i = 0; i < N; i++)
            if (ExprRunner.Run(_numericFilter, _medium, _stack)) matched++;
        return matched;
    }

    [BenchmarkCategory("Batch"), Benchmark]
    [Arguments(10)]
    [Arguments(100)]
    [Arguments(1000)]
    public int SimdOnDemand_Batch(int N)
    {
        var matched = 0;
        for (var i = 0; i < N; i++)
        {
            var year = SimdOnDemandExtractor.ExtractField(_medium, JsonPayloads.KeyYear);
            var rating = SimdOnDemandExtractor.ExtractField(_medium, JsonPayloads.KeyRating);
            if (year.Num > 1950 && rating.Num >= 4.0) matched++;
        }
        return matched;
    }

    [BenchmarkCategory("Batch"), Benchmark]
    [Arguments(10)]
    [Arguments(100)]
    [Arguments(1000)]
    public int SimdTwoPass_Batch(int N)
    {
        var matched = 0;
        for (var i = 0; i < N; i++)
        {
            var year = SimdOnDemandTwoPassExtractor.ExtractField(_medium, JsonPayloads.KeyYear);
            var rating = SimdOnDemandTwoPassExtractor.ExtractField(_medium, JsonPayloads.KeyRating);
            if (year.Num > 1950 && rating.Num >= 4.0) matched++;
        }
        return matched;
    }

    [BenchmarkCategory("Batch"), Benchmark]
    [Arguments(10)]
    [Arguments(100)]
    [Arguments(1000)]
    public int SimdJson_Batch(int N)
    {
        var matched = 0;
        for (var i = 0; i < N; i++)
        {
            var year = SimdJsonHelper.ExtractField(_medium, _kYear);
            var rating = SimdJsonHelper.ExtractField(_medium, _kRating);
            if (year.Num > 1950 && rating.Num >= 4.0) matched++;
        }
        return matched;
    }

    [BenchmarkCategory("Batch"), Benchmark]
    [Arguments(10)]
    [Arguments(100)]
    [Arguments(1000)]
    public int SimdJsonNative_Batch(int N)
    {
        var matched = 0;
        for (var i = 0; i < N; i++)
        {
            var year = SimdJsonNativeHelper.ExtractField(JsonPayloads.MediumPadded, JsonPayloads.MediumLen, JsonPayloads.KeyYear);
            var rating = SimdJsonNativeHelper.ExtractField(JsonPayloads.MediumPadded, JsonPayloads.MediumLen, JsonPayloads.KeyRating);
            if (year.Num > 1950 && rating.Num >= 4.0) matched++;
        }
        return matched;
    }
}
