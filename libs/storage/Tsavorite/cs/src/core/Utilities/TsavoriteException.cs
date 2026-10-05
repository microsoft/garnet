// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Tsavorite exception base type
    /// </summary>
    public class TsavoriteException : Exception
    {
        /// <summary>
        /// Throw Tsavorite exception
        /// </summary>
        public TsavoriteException()
        {
        }

        /// <summary>
        /// Throw Tsavorite exception with message
        /// </summary>
        /// <param name="message"></param>
        public TsavoriteException(string message) : base(message)
        {
        }

        /// <summary>
        /// Throw Tsavorite exception with message and inner exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public TsavoriteException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// Tsavorite IO exception type with message and inner exception
    /// </summary>
    public class TsavoriteIOException : TsavoriteException
    {
        /// <summary>
        /// Throw Tsavorite exception
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public TsavoriteIOException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    /// <summary>
    /// TsavoriteNoHybridLog exception type with message and inner exception.
    /// </summary>
    public class TsavoriteNoHybridLogException : TsavoriteException
    {
        /// <summary>
        /// Number of HybridLog checkpoint tokens present on disk when recovery scanned for one. Zero means nothing
        /// was ever checkpointed to this location, which distinguishes a fresh start from a set of rejected tokens.
        /// </summary>
        public int CandidateTokenCount { get; }

        /// <summary>
        /// Number of the <see cref="CandidateTokenCount"/> tokens whose metadata could not be read. This reflects
        /// on-disk state only when no other caller can read metadata through the same checkpoint manager, as during
        /// Garnet startup recovery; a concurrent reader elsewhere can cause transient read failures.
        /// </summary>
        public int UnreadableTokenCount { get; }

        /// <summary>
        /// Throw Tsavorite exception
        /// </summary>
        /// <param name="message"></param>
        public TsavoriteNoHybridLogException(string message) : base(message)
        {
        }

        /// <summary>
        /// Throw Tsavorite exception, reporting the checkpoint tokens that were scanned and rejected
        /// </summary>
        /// <param name="message"></param>
        /// <param name="candidateTokenCount">Number of HybridLog checkpoint tokens present on disk</param>
        /// <param name="unreadableTokenCount">Number of those tokens whose metadata could not be read</param>
        public TsavoriteNoHybridLogException(string message, int candidateTokenCount, int unreadableTokenCount) : base(message)
        {
            CandidateTokenCount = candidateTokenCount;
            UnreadableTokenCount = unreadableTokenCount;
        }
    }

    /// <summary>
    /// The store's log geometry does not match the geometry recorded in the checkpoint being recovered.
    /// </summary>
    /// <remarks>
    /// Distinct from the other recovery failures because it reports a configuration mistake rather than unreadable data.
    /// The checkpoint is intact and recovers correctly once the store is opened with the settings it was written with, so
    /// a host must not fall back to starting empty and discarding it.
    /// </remarks>
    public class TsavoriteLogGeometryMismatchException : TsavoriteException
    {
        /// <summary>
        /// Report a geometry mismatch between the store's settings and the recovered checkpoint.
        /// </summary>
        /// <param name="message"></param>
        public TsavoriteLogGeometryMismatchException(string message) : base(message)
        {
        }
    }
}