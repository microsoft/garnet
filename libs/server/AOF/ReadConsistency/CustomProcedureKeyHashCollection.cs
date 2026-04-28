// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.server
{
    /// <summary>
    /// Used to track the hashes of the keys associated with a given proc in order to update their timestamps in the ReplicaTimestampTracker
    /// </summary>
    /// <param name="appendOnlyFile"></param>
    public class CustomProcedureKeyHashCollection(GarnetAppendOnlyFile appendOnlyFile)
    {
        readonly GarnetAppendOnlyFile appendOnlyFile = appendOnlyFile;
        readonly List<long> hashes = [];

        /// <summary>
        /// Add hash from key to track
        /// </summary>
        /// <param name="hash"></param>
        public void AddHash(long hash)
            => hashes.Add(hash);

        /// <summary>
        /// Update sequenceNumber for all keys in the collection
        /// </summary>
        /// <param name="sequenceNumber"></param>
        public void UpdateSequenceNumber(long sequenceNumber)
        {
            foreach (var hash in hashes)
                appendOnlyFile.readConsistencyManager.UpdateVirtualSublogKeySequenceNumber(hash, sequenceNumber);
        }
    }
}