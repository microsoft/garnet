// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Passes every call to another <see cref="ILogCommitManager"/>, and lets a test wait until a commit's metadata has been
    /// passed on. In fast-commit mode a commit completes before its commit-metadata file is written, so a test that reads,
    /// removes or recovers from commit-metadata files waits for them first.
    /// </summary>
    /// <remarks>A commit is identified by its number and <see cref="TsavoriteLogRecoveryInfo.UntilAddress"/>, because commits
    /// that follow a recovery to an earlier commit reuse the numbers of the commits it did not restore.</remarks>
    /// <param name="inner">Commit manager that does the work; it is owned by the caller</param>
    internal sealed class NotifyingLogCommitManager(ILogCommitManager inner) : ILogCommitManager
    {
        readonly ConcurrentDictionary<(long CommitNum, long UntilAddress), TaskCompletionSource> committed = new();

        TaskCompletionSource Committed(long commitNum, long untilAddress)
            => committed.GetOrAdd((commitNum, untilAddress), _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        /// <summary>
        /// Wait until the metadata of the commit with <paramref name="commitNum"/> and <paramref name="untilAddress"/> has been
        /// passed to the inner commit manager. The log passes commits' metadata on one at a time, in commit order, so this also
        /// covers every earlier commit.
        /// </summary>
        public void WaitForMetadata(long commitNum, long untilAddress)
            => ClassicAssert.IsTrue(Committed(commitNum, untilAddress).Task.Wait(TimeSpan.FromSeconds(30)),
                $"The metadata of commit {commitNum} until address {untilAddress} was not written");

        /// <inheritdoc />
        public void Commit(long beginAddress, long untilAddress, byte[] commitMetadata, long commitNum, bool forceWriteMetadata)
        {
            inner.Commit(beginAddress, untilAddress, commitMetadata, commitNum, forceWriteMetadata);
            _ = Committed(commitNum, untilAddress).TrySetResult();
        }

        /// <inheritdoc />
        public byte[] GetCommitMetadata(long commitNum) => inner.GetCommitMetadata(commitNum);

        /// <inheritdoc />
        public IEnumerable<long> ListCommits() => inner.ListCommits();

        /// <inheritdoc />
        public void RemoveCommit(long commitNum) => inner.RemoveCommit(commitNum);

        /// <inheritdoc />
        public void RemoveAllCommits() => inner.RemoveAllCommits();

        /// <inheritdoc />
        public void OnRecovery(long commitNum) => inner.OnRecovery(commitNum);

        /// <inheritdoc />
        public void Dispose()
        {
        }
    }
}