// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Callback functions for streaming snapshot iteration
    /// </summary>
    public interface IStreamingSnapshotIteratorFunctions<TKey, TValue>
    {
        /// <summary>Iteration is starting.</summary>
        /// <param name="checkpointToken">Checkpoint token</param>
        /// <param name="currentVersion">Current version of database</param>
        /// <param name="targetVersion">Target version of database</param>
        /// <returns>True to continue iteration, else false</returns>
        bool OnStart(Guid checkpointToken, long currentVersion, long targetVersion);

        /// <summary>Next record in the streaming snapshot.</summary>
        /// <param name="key">Reference to the current record's key</param>
        /// <param name="value">Reference to the current record's Value</param>
        /// <param name="recordMetadata">Record metadata, including <see cref="RecordInfo"/> and the current record's logical address</param>
        /// <param name="numberOfRecords">The number of records returned so far, not including the current one.</param>
        /// <returns>True to continue iteration, else false</returns>
        bool Reader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords);

        /// <summary>Iteration is complete.</summary>
        /// <param name="completed">If true, the iteration completed; else OnStart() or Reader() returned false to stop the iteration.</param>
        /// <param name="numberOfRecords">The number of records returned before the iteration stopped.</param>
        void OnStop(bool completed, long numberOfRecords);

        /// <summary>An exception was thrown on iteration (likely during <see name="Reader"/>.</summary>
        /// <param name="exception">The exception that was thrown.</param>
        /// <param name="numberOfRecords">The number of records returned before the exception.</param>
        void OnException(Exception exception, long numberOfRecords);
    }
}