﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
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

        protected void Setup()
        {
            var opts = new GarnetServerOptions();
            opts.QuietMode = true;
            opts.AuthSettings = authSettings;
            server = new EmbeddedRespServer(opts);
            session = server.GetRespSession();
        }

        protected void Cleanup()
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