// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// A <see cref="ManualResetEventSlim"/> that is triggered based on a count hitting or advancing above 0 rather than
    /// explicit Set and Reset calls.
    /// </summary>
    /// <remarks>
    /// Akin to a <see cref="CountdownEvent"/>, but allows for count to go back up after hitting 0.
    /// </remarks>
    public struct CountingEventSlim : IDisposable
    {
        private const int HoldForSetValue = int.MinValue / 2;

        private readonly ManualResetEventSlim resetEvent;
        private int count = 0;

        private CountingEventSlim(ManualResetEventSlim resetEvent)
        {
            this.resetEvent = resetEvent;
        }

        /// <summary>
        /// Increment the internal count.
        /// 
        /// Any caller to <see cref="Wait"/> after this call returns but before a paired call to <see cref="Decrement"/> will block.
        /// </summary>
        public void Increment()
        {
            while (true)
            {
                var addRes = Interlocked.Increment(ref count);
                if (addRes <= 0)
                {
                    // Some thread is about to Set, undo and wait

                    _ = Interlocked.Decrement(ref count);
                    _ = Thread.Yield();
                }
                else
                {
                    resetEvent.Reset();
                    break;
                }
            }
        }

        /// <summary>
        /// Decrement the internal count.
        /// 
        /// If this is the last outstanding <see cref="Decrement"/> paired to a completed <see cref="Increment"/>, threads blocked in <see cref="Wait"/> will be unblocked.
        /// </summary>
        public void Decrement()
        {
            var decrRes = Interlocked.Decrement(ref count);
            Debug.Assert(decrRes >= 0, "Decrement fell below 0, implies unbalanced calls to Increment and Decrement");

            if (decrRes == 0 && Interlocked.CompareExchange(ref count, HoldForSetValue, 0) == 0)
            {
                resetEvent.Set();

                var unlockRes = Interlocked.Add(ref count, -HoldForSetValue);
                Debug.Assert(unlockRes >= 0, "Unlock resulted in incoherent count");
            }
        }

        /// <summary>
        /// Block until the internal count hits 0.
        /// 
        /// Returns true if wait was successful, false otherwise.
        /// </summary>
        /// <param name="millisecondsTimeout">Timeout for wait operation.  -1 (the default) waits indefinitely, 0 returns immediately.</param>
        public readonly bool Wait(int millisecondsTimeout = -1)
        => resetEvent.Wait(millisecondsTimeout);

        /// <summary>
        /// Create a new <see cref="CountingEventSlim"/>.
        /// </summary>
        public static CountingEventSlim Create()
        => new(new(true));

        /// <inheritdoc/>
        public readonly void Dispose()
        => resetEvent.Dispose();
    }
}
