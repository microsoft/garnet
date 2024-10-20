// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Embedded.perftest;
using Garnet.server;
using Garnet.server.Auth.Settings;

namespace BDN.benchmark.Operations
{
    public abstract unsafe class OperationsBase
    {
        protected EmbeddedRespServer server;
        internal RespServerSession session;
        protected IAuthenticationSettings authSettings = null;
        protected const int batchSize = 128;
        protected bool useAof = false;
        protected bool useACLs = false;

        [GlobalSetup]
        public virtual void GlobalSetup()
        {
            var opts = new GarnetServerOptions
            {
                QuietMode = true
            };
            if (useAof)
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
                if (useACLs)
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