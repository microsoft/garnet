// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;

namespace Garnet.client.GarnetClientAPI
{
    /// <summary>
    /// Describes a SortedSet [member, score] entry
    /// </summary>
    public readonly struct SortedSetPairCollection
    {
        readonly List<Memory<byte>> elements;

        /// <summary>
        /// Create a new collection for a sorted set
        /// </summary>
        public SortedSetPairCollection()
        {
            this.elements = new List<Memory<byte>>();
        }

        /// <summary>
        /// Adds a new entry to the collection
        /// </summary>
        /// <param name="member"></param>
        /// <param name="score"></param>
        public void AddSortedSetEntry(byte[] member, double score)
        {
            elements.Add(Encoding.ASCII.GetBytes(score.ToString()));
            elements.Add((Memory<byte>)member);
        }

        /// <summary>
        /// Gets all the entries
        /// </summary>
        public List<Memory<byte>> Elements => elements;
    }
}