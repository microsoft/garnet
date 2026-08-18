// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Exception thrown when commit fails
    /// </summary>
    public sealed class CommitFailureException : TsavoriteException
    {
        /// <summary>
        /// Commit info and next commit task in chain
        /// </summary>
        public LinkedCommitInfo LinkedCommitInfo { get; private set; }

        internal CommitFailureException(LinkedCommitInfo linkedCommitInfo, string message)
            : base(message)
            => LinkedCommitInfo = linkedCommitInfo;

        internal CommitFailureException(LinkedCommitInfo linkedCommitInfo, string message, Exception innerException)
            : base(message, innerException)
            => LinkedCommitInfo = linkedCommitInfo;
    }
}