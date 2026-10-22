// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics.CodeAnalysis;

namespace Garnet.server
{
    /// <summary>
    /// What a record replayed by <see cref="AofProcessor.TryProcessAofRecordWithoutWaiting"/> has to wait on before it
    /// can be replayed.
    /// </summary>
    public enum ReplayWait : byte
    {
        /// <summary>The record was replayed.</summary>
        None,

        /// <summary>
        /// The record has to wait for the Vector Set operations queued before it, which <see cref="AofProcessor.WaitForVectorOperationsToCompleteAsync"/>
        /// waits for without holding a thread.
        /// </summary>
        VectorOperations,

        /// <summary>
        /// The record can wait on something that only a thread of its own may block on: the other replay participants, a
        /// checkpoint, an import being finalized, or the drop of an index.
        /// </summary>
        Thread,
    }

    /// <summary>
    /// Thrown by a wait that a record reaches while it is replayed on a thread that must not wait (see
    /// <see cref="StorageSession.replayMustNotWait"/>), in place of the wait. It is thrown only before the record has
    /// changed anything, so the record can be replayed again from its start once the wait is over.
    /// </summary>
    internal sealed class ReplayMustWaitException : Exception
    {
        /// <summary>What the record has to wait on.</summary>
        internal ReplayWait Wait { get; }

        /// <summary>
        /// Whether the record waits on the drop of the index of the key with hash <see cref="IndexDropKeyHash"/>.
        /// </summary>
        internal bool IsIndexDrop { get; }

        /// <summary>The hash of the key whose index the record waits to be dropped, if <see cref="IsIndexDrop"/>.</summary>
        internal long IndexDropKeyHash { get; }

        ReplayMustWaitException(ReplayWait wait, bool isIndexDrop, long indexDropKeyHash)
            : base($"Replay has to wait on {wait}")
        {
            Wait = wait;
            IsIndexDrop = isIndexDrop;
            IndexDropKeyHash = indexDropKeyHash;
        }

        /// <summary>The record has to wait for the Vector Set operations queued before it.</summary>
        [DoesNotReturn]
        internal static void ThrowForVectorOperations() => throw new ReplayMustWaitException(ReplayWait.VectorOperations, false, 0);

        /// <summary>The record can wait on something that only a thread of its own may block on.</summary>
        [DoesNotReturn]
        internal static void ThrowForThread() => throw new ReplayMustWaitException(ReplayWait.Thread, false, 0);

        /// <summary>The record has to wait for the drop of the index of the key with hash <paramref name="keyHash"/>.</summary>
        /// <param name="keyHash">The hash of the key.</param>
        [DoesNotReturn]
        internal static void ThrowForIndexDrop(long keyHash) => throw new ReplayMustWaitException(ReplayWait.Thread, true, keyHash);
    }
}