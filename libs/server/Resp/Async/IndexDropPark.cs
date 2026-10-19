// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    // Not declared unsafe: an async method body cannot sit in an unsafe context, and the unsafe modifier
    // on a partial type declaration applies only to the part that carries it.
    internal sealed partial class RespServerSession
    {
        /// <summary>
        /// Reusable wait for a session parked until the DiskANN index behind a Vector Set has been dropped, released when
        /// this session is torn down.
        /// </summary>
        /// <remarks>
        /// Created on first use, and deliberately not disposed, for the same reasons as <see cref="SessionDelay"/>.
        /// </remarks>
        IndexDropWait IndexDropWait
        {
            get
            {
                var wait = indexDropWait;
                if (wait is null)
                {
                    wait = new IndexDropWait();
                    var existing = Interlocked.CompareExchange(ref indexDropWait, wait, null);
                    if (existing is not null)
                        wait = existing;
                    else if (sessionTornDown)
                        wait.Cancel();
                }
                return wait;
            }
        }

        IndexDropWait indexDropWait;

        /// <summary>
        /// Called by the Vector Set manager when the command being run has to recreate the DiskANN index of a key whose
        /// earlier index is still to be dropped, with no Vector Set lock held. Parks the session until the drop task has
        /// dropped it, and runs the command again from its parse when the session resumes.
        /// </summary>
        /// <param name="vectorManager">The Vector Set manager whose drop task drops the index.</param>
        /// <param name="storageSession">The storage session the command is running on.</param>
        /// <param name="key">The key of the Vector Set.</param>
        /// <param name="keyHash">The hash of <paramref name="key"/>.</param>
        /// <remarks>
        /// Parking unwinds the command with <see cref="LockContendedException"/>, as <see cref="OnLockContended"/> does.
        /// Returns, so that the caller waits on its thread, if the session cannot park, if the command is not a Vector Set
        /// command that has yet to change anything, or if the drop is found done once the session is listed for it.
        /// </remarks>
        internal unsafe void ParkUntilIndexDropped(VectorManager vectorManager, StorageSession storageSession, ReadOnlySpan<byte> key, long keyHash)
        {
            if (!restartPoint.IsArmed
                || !restartPoint.Command.IsVectorSetCommand()
                || !CanSuspend
                || DenyBlocking
                || recvBufferPtr == null
                || storageSession.PendingOperationCount != 0)
                return;

            var wait = IndexDropWait;
            wait.Register(vectorManager.indexDropSignal, keyHash);
            if (!vectorManager.DropRequested(key))
            {
                wait.Withdraw();
                return;
            }

            lockContendedRaised = 3;
            throw lockContended ??= new LockContendedException();
        }

        /// <summary>
        /// Parks the session on the drop its command found pending, once the command has unwound to <see cref="ConsumeCore"/>
        /// and been rewound to its parse.
        /// </summary>
        void ParkForIndexDrop()
        {
            ValueTask body;
            using (BeginAsyncCommand(retainBatchScope: false))
                body = WaitForIndexDropAsync();

            _ = CompleteAsyncCommand(body);
            Debug.Assert(asyncSuspended, "An index drop wait completed without suspending");
        }

        [AsyncMethodBuilder(typeof(RespAsyncMethodBuilder))]
        async ValueTask WaitForIndexDropAsync()
            // Suspends even when the drop was done before the session parked: the command has already unwound, and runs
            // again only when the session resumes
            => await new AlwaysSuspend(IndexDropWait.WaitAsync());
    }
}