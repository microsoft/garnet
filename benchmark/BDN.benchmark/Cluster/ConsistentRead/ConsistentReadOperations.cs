// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;

namespace BDN.benchmark.Cluster.ConsistentRead
{
    /// <summary>
    /// Benchmarks for GET operations exercising the consistent read path
    /// in cluster mode with multilog enabled.
    /// </summary>
    [MemoryDiagnoser]
    public unsafe class ConsistentReadOperations
    {
        /// <summary>
        /// Benchmark parameters
        /// </summary>
        [ParamsSource(nameof(ConsistentReadParamsProvider))]
        public ConsistentReadParams Params { get; set; }

        /// <summary>
        /// Parameters provider
        /// </summary>
        public IEnumerable<ConsistentReadParams> ConsistentReadParamsProvider()
        {
            yield return new(false, false);   // SingleLog (baseline)
            yield return new(true, false);    // MultiLog+Primary
            yield return new(true, true);     // MultiLog+Replica (exercises consistent read)
        }

        ConsistentReadContext cc;

        /// <summary>
        /// Global setup
        /// </summary>
        [GlobalSetup]
        public void GlobalSetup()
        {
            cc = new ConsistentReadContext();
            cc.SetupInstance(Params);
        }

        /// <summary>
        /// Global cleanup
        /// </summary>
        [GlobalCleanup]
        public void GlobalCleanup()
        {
            cc.Dispose();
        }

        /// <summary>
        /// GET benchmark — 100 sequential GETs per invocation.
        /// Exercises: sublog idx calc, sketch lookup, inProgress lock, ResetTimeoutCts, SwitchActiveDatabaseSession.
        /// </summary>
        [Benchmark]
        public void Get()
        {
            cc.Consume(cc.getRequest.ptr, cc.getRequest.buffer.Length);
        }

        /// <summary>
        /// MGET benchmark — single MGET with 100 keys per invocation.
        /// Exercises: batch consistent read path (BeforeConsistentReadKeyBatch).
        /// </summary>
        [Benchmark]
        public void MGet()
        {
            cc.Consume(cc.mgetRequest.ptr, cc.mgetRequest.buffer.Length);
        }
    }
}