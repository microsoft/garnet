// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;

namespace Garnet.server
{
    public class ReadSessionWaiter
    {
        public ManualResetEventSlim eventSlim;
        public ReplicaReadSessionContext rrsc;

        readonly CancellationTokenSource cts = new();
        SingleWriterMultiReaderLock _disposed = new();

        public void Wait(TimeSpan timeout)
        {
            var acquireLock = false;
            try
            {
                acquireLock = _disposed.TryReadLock();
                if (!acquireLock)
                    throw new GarnetException("Failed to acquire ReadSessionWaiter lock!");
                _ = eventSlim.Wait((int)timeout.TotalMilliseconds, cts.Token);
            }
            finally
            {
                if (acquireLock)
                    _disposed.ReadUnlock();
            }
        }

        public void Set()
            => eventSlim.Set();

        public void Reset()
            => eventSlim.Reset();

        public void Dispose()
        {
            cts.Cancel();
            _disposed.WriteLock();
            cts?.Dispose();
        }
    }
}