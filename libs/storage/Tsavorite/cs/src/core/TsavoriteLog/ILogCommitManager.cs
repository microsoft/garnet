// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Tsavorite.core
{
    /// <summary>
    /// TsavoriteLog commit manager
    /// </summary>
    public interface ILogCommitManager : IDisposable
    {
        /// <summary>
        /// Perform (synchronous) commit with specified metadata
        /// </summary>
        /// <param name="beginAddress">Committed begin address (for information only, not necessary to persist)</param>
        /// <param name="untilAddress">Address committed until (for information only, not necessary to persist)</param>
        /// <param name="commitMetadata">Commit metadata - should be persisted</param>
        /// <param name="commitNum">commit num</param>
        /// <param name="forceWriteMetadata">force writing of metadata in case of fast commit</param>
        void Commit(long beginAddress, long untilAddress, byte[] commitMetadata, long commitNum, bool forceWriteMetadata);

        /// <summary>
        /// Return commit metadata
        /// </summary>
        /// <param name="commitNum"></param>
        /// <returns></returns>
        byte[] GetCommitMetadata(long commitNum);

        /// <summary>
        /// Get list of commits, in order of usage preference
        /// </summary>
        /// <returns></returns>
        public IEnumerable<long> ListCommits();

        /// <summary>
        /// Remove the given commit, if present. Should only be invoked if PreciseCommitNumRecoverySupport returns true
        /// </summary>
        /// <param name="commitNum">commit num to remove</param>
        public void RemoveCommit(long commitNum);

        /// <summary>
        /// Remove all TsavoriteLog commits from this manager
        /// </summary>
        public void RemoveAllCommits();

        /// <summary>
        /// Initiatize manager on recovery (e.g., deleting other commits)
        /// </summary>
        /// <param name="commitNum">Commit number</param>
        public void OnRecovery(long commitNum);
    }
}