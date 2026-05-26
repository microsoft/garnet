// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.kvbench
{
    /// <summary>
    /// Pre-computed Zipf distribution constants, shared across all per-thread
    /// generators. Computing these is O(N) in the number of keys due to
    /// <see cref="Zeta"/> — doing it once on the main thread saves
    /// <c>O(N * threads)</c> Math.Pow calls.
    /// </summary>
    public sealed class ZipfConstants
    {
        public readonly long Size;
        public readonly double Theta;
        public readonly double ZetaN;
        public readonly double Alpha;
        public readonly double Cutoff2;
        public readonly double Eta;

        public ZipfConstants(long size, double theta)
        {
            Size = size;
            Theta = theta;
            ZetaN = Zeta(size, theta);
            Alpha = 1.0 / (1.0 - theta);
            Cutoff2 = Math.Pow(0.5, theta);
            var zeta2 = Zeta(2, theta);
            Eta = (1.0 - Math.Pow(2.0 / size, 1.0 - theta)) / (1.0 - zeta2 / ZetaN);
        }

        private static double Zeta(long count, double theta)
        {
            double zetaN = 0.0;
            for (long i = 1; i <= count; ++i)
                zetaN += 1.0 / Math.Pow(i, theta);
            return zetaN;
        }
    }

    /// <summary>
    /// Per-thread Zipf key sampler. Stores 5 doubles + a reference to a shared
    /// <see cref="ZipfConstants"/>; no per-instance setup cost.
    /// </summary>
    public struct ZipfGenerator
    {
        readonly long size;
        readonly double zetaN;
        readonly double alpha;
        readonly double cutoff2;
        readonly double eta;

        public ZipfGenerator(ZipfConstants constants)
        {
            size = constants.Size;
            zetaN = constants.ZetaN;
            alpha = constants.Alpha;
            cutoff2 = constants.Cutoff2;
            eta = constants.Eta;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Next(ref XoshiroRng rng)
        {
            // Use top 53 bits of a 64-bit draw for a uniform [0,1) double.
            double u = (rng.NextUInt64() >> 11) * (1.0 / (1UL << 53));
            double uz = u * zetaN;
            if (uz < 1.0) return 0;
            if (uz < 1.0 + cutoff2) return 1;
            return (long)(size * Math.Pow(eta * u - eta + 1.0, alpha));
        }
    }
}