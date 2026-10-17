// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Garnet.server.ACL;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Shares <see cref="ScriptExecutor"/>s between sessions.
    /// </summary>
    /// <remarks>
    /// A session rents an executor for one EVAL, EVALSHA or SCRIPT LOAD and returns it afterwards, so the
    /// number of executors (and the Lua states and storage sessions behind them) follows the number of
    /// scripts running at once, not the number of connections. Idle executors sit in a small slot array.
    /// A thread starts looking at the slot its id hashes to and returns the executor there, so a thread
    /// that runs scripts back to back keeps reusing one executor and its cache-resident working set.
    /// </remarks>
    internal sealed class ScriptExecutorPool : IDisposable
    {
        readonly StoreWrapper storeWrapper;
        readonly ILogger logger;
        readonly ScriptExecutor[] idle;
        readonly ConcurrentDictionary<ScriptExecutor, byte> live = new();
        volatile bool disposed;

        public ScriptExecutorPool(StoreWrapper storeWrapper, ILogger logger = null)
        {
            this.storeWrapper = storeWrapper;
            this.logger = logger;
            idle = new ScriptExecutor[Math.Max(16, Environment.ProcessorCount)];
        }

        /// <summary>
        /// Largest scratch buffer held by an idle executor. For tests.
        /// </summary>
        internal int LargestIdleScratchBufferCapacityForTests
        {
            get
            {
                var largest = 0;
                for (var i = 0; i < idle.Length; i++)
                {
                    var executor = Volatile.Read(ref idle[i]);
                    if (executor != null)
                        largest = Math.Max(largest, executor.ScratchBufferCapacityForTests);
                }

                return largest;
            }
        }

        /// <summary>
        /// Number of idle executors currently held. For tests.
        /// </summary>
        internal int IdleCount
        {
            get
            {
                var count = 0;
                for (var i = 0; i < idle.Length; i++)
                {
                    if (Volatile.Read(ref idle[i]) != null)
                        count++;
                }

                return count;
            }
        }

        /// <summary>
        /// Rents an executor that runs commands as <paramref name="userHandle"/>, creating one if none is idle.
        /// Returns <c>null</c> once the pool is disposed.
        /// </summary>
        /// <remarks>Must be paired with <see cref="Return"/>, from the same thread.</remarks>
        internal ScriptExecutor Rent(UserHandle userHandle)
        {
            var start = StartSlot();
            var ix = start;
            for (var i = 0; i < idle.Length; i++)
            {
                var candidate = Volatile.Read(ref idle[ix]);
                if (candidate != null && Interlocked.CompareExchange(ref idle[ix], null, candidate) == candidate)
                {
                    candidate.SetUserHandle(userHandle);
                    return candidate;
                }

                if (++ix == idle.Length)
                    ix = 0;
            }

            if (disposed)
                return null;

            var executor = new ScriptExecutor(storeWrapper, storeWrapper.luaTimeoutManager, logger);
            _ = live.TryAdd(executor, 0);
            executor.SetUserHandle(userHandle);
            return executor;
        }

        /// <summary>
        /// Returns an executor obtained from <see cref="Rent"/>. It is disposed instead of kept if the pool
        /// is full or has been disposed.
        /// </summary>
        internal void Return(ScriptExecutor executor)
        {
            executor.OnReturned();

            if (disposed)
            {
                Discard(executor);
                return;
            }

            var ix = StartSlot();
            for (var i = 0; i < idle.Length; i++)
            {
                if (Interlocked.CompareExchange(ref idle[ix], executor, null) == null)
                {
                    // A dispose that swept this slot before the executor landed in it has not seen it
                    if (disposed && Interlocked.CompareExchange(ref idle[ix], null, executor) == executor)
                        Discard(executor);

                    return;
                }

                if (++ix == idle.Length)
                    ix = 0;
            }

            Discard(executor);
        }

        /// <summary>
        /// Swap the database sessions of every idle executor, after the databases themselves were swapped.
        /// </summary>
        /// <remarks>
        /// Only called while no other session is active, so no executor is rented.
        /// </remarks>
        internal void TrySwapDatabaseSessions(int dbId1, int dbId2)
        {
            for (var i = 0; i < idle.Length; i++)
            {
                _ = Volatile.Read(ref idle[i])?.TrySwapDatabaseSessions(dbId1, dbId2);
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            disposed = true;

            for (var i = 0; i < idle.Length; i++)
            {
                var executor = Interlocked.Exchange(ref idle[i], null);
                if (executor != null)
                    Discard(executor);
            }
        }

        /// <summary>
        /// The sessions that run script commands, one per executor. The metrics monitor reads them as it
        /// does network sessions, so commands run by scripts are accounted for while the executors live.
        /// </summary>
        internal IEnumerable<RespServerSession> Processors() => live.Keys.Select(static executor => executor.Processor);

        void Discard(ScriptExecutor executor)
        {
            _ = live.TryRemove(executor, out _);
            executor.Dispose();
        }

        int StartSlot() => (int)((uint)Environment.CurrentManagedThreadId % (uint)idle.Length);
    }
}