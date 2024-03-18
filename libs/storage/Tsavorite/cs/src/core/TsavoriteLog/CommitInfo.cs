// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable 0162

using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Info contained in task associated with commit
    /// </summary>
    public struct CommitInfo
    {
        /// <summary>
        /// From address of commit range
        /// </summary>
        public long FromAddress;

        /// <summary>
        /// Until address of commit range
        /// </summary>
        public long UntilAddress;

        /// <summary>
        /// Error code (0 = success)
        /// </summary>
        public uint ErrorCode;
    }

    /// <summary>
    /// Linked list (chain) of commit info
    /// </summary>
    public struct LinkedCommitInfo
    {
        /// <summary>
        /// Commit info
        /// </summary>
        public CommitInfo CommitInfo;

        /// <summary>
        /// Next task in commit chain
        /// </summary>
        public Task<LinkedCommitInfo> NextTask;
    }
}