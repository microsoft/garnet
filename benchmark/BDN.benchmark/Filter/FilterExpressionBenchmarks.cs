// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Text;
using BenchmarkDotNet.Attributes;
using Garnet.server.Vector.Filter;

namespace BDN.benchmark.Filter
{
    /// <summary>
    /// Benchmarks for the vector filter expression engine:
    /// - ExprCompiler: compile filter string → postfix program
    /// - ExprRunner: execute compiled program against JSON attributes
    /// - AttributeExtractor: extract fields from raw JSON bytes
    /// - End-to-end: compile + run for realistic filter scenarios
    ///
    /// These benchmarks measure the hot path of VSIM ... FILTER post-processing.
    /// </summary>
    [MemoryDiagnoser]
    public class FilterExpressionBenchmarks
    {
        // ======================== Shared test data ========================

        // Simple filter: single comparison
        private byte[] _simpleFilterBytes;
        private ExprProgram _simpleProgram;

        // Medium filter: two comparisons joined by AND
        private byte[] _mediumFilterBytes;
        private ExprProgram _mediumProgram;

        // Complex filter: multiple ops, arithmetic, containment, string equality
        private byte[] _complexFilterBytes;
        private ExprProgram _complexProgram;

        // JSON attribute payloads (varying sizes)
        private byte[] _smallJson;    // 2 fields
        private byte[] _mediumJson;   // 5 fields
        private byte[] _largeJson;    // 10+ fields, array, nested object to skip

        // Reusable evaluation stack
        private Stack<ExprToken> _stack;

        [GlobalSetup]
        public void Setup()
        {
            // Filter expressions (UTF-8)
            _simpleFilterBytes = Encoding.UTF8.GetBytes(".year > 1950");
            _mediumFilterBytes = Encoding.UTF8.GetBytes(".year > 1950 and .rating >= 4.0");
            _complexFilterBytes = Encoding.UTF8.GetBytes(".rating * 2 > 8 and (.year >= 1980 or \"modern\" in .tags) and .genre == \"action\"");

            // Compile once for run benchmarks
            _simpleProgram = ExprCompiler.TryCompile(_simpleFilterBytes, out _);
            _mediumProgram = ExprCompiler.TryCompile(_mediumFilterBytes, out _);
            _complexProgram = ExprCompiler.TryCompile(_complexFilterBytes, out _);

            // JSON payloads
            _smallJson = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5}");
            _mediumJson = Encoding.UTF8.GetBytes("{\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"tags\":[\"classic\",\"popular\"]}");
            _largeJson = Encoding.UTF8.GetBytes("{\"id\":12345,\"title\":\"Test Movie\",\"year\":1980,\"rating\":4.5,\"genre\":\"action\",\"director\":\"Spielberg\",\"studio\":\"Universal\",\"budget\":50000000,\"tags\":[\"classic\",\"popular\",\"award-winning\"],\"metadata\":{\"source\":\"imdb\",\"verified\":true},\"active\":true}");

            _stack = ExprRunner.CreateStack();
        }

        // ======================== Compilation benchmarks ========================

        [Benchmark]
        public void Compile_Simple()
            => ExprCompiler.TryCompile(_simpleFilterBytes, out _);

        [Benchmark]
        public void Compile_Medium()
            => ExprCompiler.TryCompile(_mediumFilterBytes, out _);

        [Benchmark]
        public void Compile_Complex()
            => ExprCompiler.TryCompile(_complexFilterBytes, out _);

        // ======================== Execution benchmarks (compile once, run many) ========================

        [Benchmark]
        public bool Run_Simple_SmallJson()
            => ExprRunner.Run(_simpleProgram, _smallJson, _stack);

        [Benchmark]
        public bool Run_Simple_MediumJson()
            => ExprRunner.Run(_simpleProgram, _mediumJson, _stack);

        [Benchmark]
        public bool Run_Medium_MediumJson()
            => ExprRunner.Run(_mediumProgram, _mediumJson, _stack);

        [Benchmark]
        public bool Run_Complex_MediumJson()
            => ExprRunner.Run(_complexProgram, _mediumJson, _stack);

        [Benchmark]
        public bool Run_Complex_LargeJson()
            => ExprRunner.Run(_complexProgram, _largeJson, _stack);

        // ======================== Field extraction benchmarks ========================

        [Benchmark]
        public void Extract_FirstField()
            => AttributeExtractor.ExtractField(_smallJson, "year");

        [Benchmark]
        public void Extract_LastField_MediumJson()
            => AttributeExtractor.ExtractField(_mediumJson, "tags");

        [Benchmark]
        public void Extract_SkipNestedObject()
            => AttributeExtractor.ExtractField(_largeJson, "active");

        [Benchmark]
        public void Extract_MissingField()
            => AttributeExtractor.ExtractField(_mediumJson, "nonexistent");

        // ======================== End-to-end benchmarks (compile + run) ========================

        [Benchmark]
        public bool EndToEnd_Simple()
        {
            var program = ExprCompiler.TryCompile(_simpleFilterBytes, out _);
            return ExprRunner.Run(program, _mediumJson, _stack);
        }

        [Benchmark]
        public bool EndToEnd_Complex()
        {
            var program = ExprCompiler.TryCompile(_complexFilterBytes, out _);
            return ExprRunner.Run(program, _mediumJson, _stack);
        }

        // ======================== Batch simulation (N candidates) ========================

        [Benchmark]
        [Arguments(10)]
        [Arguments(100)]
        [Arguments(1000)]
        public int RunBatch_Medium(int count)
        {
            var matched = 0;
            for (var i = 0; i < count; i++)
            {
                // Alternate between matching and non-matching JSON to simulate realistic filtering
                var json = (i % 3 == 0) ? _smallJson : _mediumJson;
                if (ExprRunner.Run(_mediumProgram, json, _stack))
                    matched++;
            }
            return matched;
        }

        [Benchmark]
        [Arguments(10)]
        [Arguments(100)]
        [Arguments(1000)]
        public int RunBatch_Complex(int count)
        {
            var matched = 0;
            for (var i = 0; i < count; i++)
            {
                var json = (i % 3 == 0) ? _smallJson : _mediumJson;
                if (ExprRunner.Run(_complexProgram, json, _stack))
                    matched++;
            }
            return matched;
        }
    }
}