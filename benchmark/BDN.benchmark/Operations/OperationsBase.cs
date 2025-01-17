// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using Embedded.server;
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
        /// Parameters
        /// </summary>
        [ParamsSource(nameof(OperationParamsProvider))]
        public OperationParams Params { get; set; }

        /// <summary>
        /// Operation parameters provider
        /// </summary>
        public IEnumerable<OperationParams> OperationParamsProvider()
        {
            yield return new(false, false);
            yield return new(true, false);
            yield return new(false, true);
        }

        /// <summary>
        /// Batch size per method invocation
        /// With a batchSize of 100, we have a convenient conversion of latency to throughput:
        ///   5 us = 20 Mops/sec
        ///  10 us = 10 Mops/sec
        ///  20 us =  5 Mops/sec
        ///  25 us =  4 Mops/sec
        /// 100 us =  1 Mops/sec
        /// </summary>
        internal const int batchSize = 100;
        internal EmbeddedRespServer server;
        internal RespServerSession session;
        internal RespServerSession subscribeSession;

        /// <summary>
        /// Setup
        /// </summary>
        [GlobalSetup]
        public virtual void GlobalSetup()
        {
            var opts = new GarnetServerOptions
            {
                QuietMode = true,
                EnableLua = true,
                DisablePubSub = true,
                LuaOptions = new(LuaMemoryManagementMode.Native, ""),
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
                    File.WriteAllText(aclFile, @"user default on nopass -@all +ping +set +get +setex +incr +decr +incrby +decrby +zadd +zrem +lpush +lpop +sadd +srem +hset +hdel +publish +subscribe +@custom");
                    opts.AuthSettings = new AclAuthenticationPasswordSettings(aclFile);
                }

                server = new EmbeddedRespServer(opts, null, new GarnetServerEmbedded());
                session = server.GetRespSession();
            }
            finally
            {
                if (aclFile != null)
                    File.Delete(aclFile);
            }
        }

        /// <summary>
        /// Cleanup
        /// </summary>
        [GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            session.Dispose();
            subscribeSession?.Dispose();
            server.Dispose();
        }

        protected void Send(Request request)
        {
            _ = session.TryConsumeMessages(request.bufferPtr, request.buffer.Length);
        }

        protected unsafe void SetupOperation(ref Request request, ReadOnlySpan<byte> operation, int batchSize = batchSize)
        {
            request.buffer = GC.AllocateArray<byte>(operation.Length * batchSize, pinned: true);
            request.bufferPtr = (byte*)Unsafe.AsPointer(ref request.buffer[0]);
            for (int i = 0; i < batchSize; i++)
                operation.CopyTo(new Span<byte>(request.buffer).Slice(i * operation.Length));
        }

        protected unsafe void SetupOperation(ref Request request, string operation, int batchSize = batchSize)
        {
            request.buffer = GC.AllocateUninitializedArray<byte>(operation.Length * batchSize, pinned: true);
            for (var i = 0; i < batchSize; i++)
            {
                var start = i * operation.Length;
                Encoding.UTF8.GetBytes(operation, request.buffer.AsSpan().Slice(start, operation.Length));
            }
            request.bufferPtr = (byte*)Unsafe.AsPointer(ref request.buffer[0]);
        }

        protected unsafe void SetupOperation(ref Request request, List<byte> operationBytes)
        {
            request.buffer = GC.AllocateUninitializedArray<byte>(operationBytes.Count, pinned: true);
            operationBytes.CopyTo(request.buffer);
            request.bufferPtr = (byte*)Unsafe.AsPointer(ref request.buffer[0]);
        }

        protected void SlowConsumeMessage(ReadOnlySpan<byte> message)
        {
            Request request = default;
            SetupOperation(ref request, message, 1);
            Send(request);
        }
    }
}