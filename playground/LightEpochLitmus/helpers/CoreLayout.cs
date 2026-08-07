// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace Tsavorite.epoch.litmus
{
    /// <summary>Which cores the harness pins its threads to.</summary>
    internal readonly struct CoreLayout
    {
        internal int ReclaimerCore { get; init; }
        internal int ReaderCore { get; init; }
        internal int[] DisturberCores { get; init; }

        /// <summary>
        /// Reclaimer on processor 0, reader on 2, disturbers on 4, 6, 8 and so on. Stepping by two
        /// gives each thread its own physical core on an SMT machine, where sibling threads share a
        /// store buffer and would mask the reordering; the odd processors are only used once the
        /// even ones run out. Exactly <paramref name="disturberCount"/> disturbers are laid out,
        /// sharing cores if need be; false if the machine cannot even seat the reader and the
        /// reclaimer.
        /// </summary>
        internal static bool TrySelect(int disturberCount, out CoreLayout cores)
        {
            var logical = Environment.ProcessorCount;
            if (logical < 4)
            {
                cores = default;
                return false;
            }

            const int reclaimer = 0, reader = 2;

            // Every processor except the reader's and the reclaimer's, preferring their own
            // physical core.
            var pool = new List<int>();
            for (var core = 4; core < logical; core += 2)
                pool.Add(core);

            for (var core = 1; core < logical; core += 2)
                pool.Add(core);

            var disturbers = new int[disturberCount];
            for (var i = 0; i < disturberCount; i++)
                disturbers[i] = pool[i % pool.Count];

            cores = new CoreLayout { ReclaimerCore = reclaimer, ReaderCore = reader, DisturberCores = disturbers };
            return true;
        }

        public override string ToString()
            => $"reclaimer={ReclaimerCore} reader={ReaderCore} disturbers=[{string.Join(",", DisturberCores)}]";
    }
}