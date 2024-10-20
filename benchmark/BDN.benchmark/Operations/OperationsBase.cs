// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Embedded.perftest;
using Garnet.server;
using Garnet.server.Auth.Settings;

namespace BDN.benchmark.Operations
{
    /// <summary>
    /// Base class for operations benchmarks
    /// </summary>
    public abstract unsafe class OperationsBase
    {
        /// <summary>
        /// Batch size per method invocation
        /// With a batchSize of 100, we have a convenient conversion of latency to throughput:
        ///   5 us = 20 Mops/sec
        ///  10 us = 10 Mops/sec
        ///  20 us =  5 Mops/sec
        ///  25 us =  4 Mops/sec
        /// 100 us =  1 Mops/sec
        /// </summary>
        const int batchSize = 100;
        internal EmbeddedRespServer server;
        internal RespServerSession session;

        [ParamsSource(nameof(OperationParamsProvider))]
        public OperationParams Params { get; set; }

        public IEnumerable<OperationParams> OperationParamsProvider()
        {
            yield return new(false, false);
            yield return new(true, false);
            yield return new(false, true);
        }

        /// <summary>
        /// Setup
        /// </summary>
        [GlobalSetup]
        public virtual void GlobalSetup()
        {
            var opts = new GarnetServerOptions
            {
                QuietMode = true
            };
            if (Params.useAof)
            {
                opts.EnableAOF = true;
                opts.UseAofNullDevice = true;
                opts.MainMemoryReplication = true;
                opts.CommitFrequencyMs = -1;
                opts.AofPageSize = "128m";
                opts.AofMemorySize = "256m";
            }

            string aclFile = null;
            try
            {
                if (Params.useACLs)
                {
                    aclFile = Path.GetTempFileName();
                    File.WriteAllText(aclFile, @"user default on nopass -@all +ping +set +get +setex +incr +decr +incrby +decrby");
                    opts.AuthSettings = new AclAuthenticationPasswordSettings(aclFile);
                }
                server = new EmbeddedRespServer(opts);
            }
            finally
            {
                if (aclFile != null)
                    File.Delete(aclFile);
            }

            session = server.GetRespSession();
        }

        /// <summary>
        /// Cleanup
        /// </summary>
        [GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            session.Dispose();
            server.Dispose();
        }

        protected void SetupOperation(ref byte[] requestBuffer, ref byte* requestBufferPointer, ReadOnlySpan<byte> operation)
        {
            requestBuffer = GC.AllocateArray<byte>(operation.Length * batchSize, pinned: true);
            requestBufferPointer = (byte*)Unsafe.AsPointer(ref requestBuffer[0]);
            for (int i = 0; i < batchSize; i++)
                operation.CopyTo(new Span<byte>(requestBuffer).Slice(i * operation.Length));
        }

        protected void SlowConsumeMessage(ReadOnlySpan<byte> message)
        {
            var buffer = GC.AllocateArray<byte>(message.Length, pinned: true);
            var bufferPointer = (byte*)Unsafe.AsPointer(ref buffer[0]);
            message.CopyTo(new Span<byte>(buffer));
            _ = session.TryConsumeMessages(bufferPointer, buffer.Length);
        }
    }
}