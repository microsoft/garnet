// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;

namespace Resp.benchmark
{
    public class ZipfGenerator
    {
        public const double DefaultTheta = 0.99;

        static readonly ConcurrentDictionary<(int Size, double Theta), Lazy<ZipfConstants>> ConstantsCache = new();

        // Based on "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al., SIGMOD 1994.
        readonly RandomGenerator rng;
        private readonly int size;
        readonly double zetaN, alpha, cutoff2, eta;

        public ZipfGenerator(RandomGenerator rng, int size, double theta = DefaultTheta)
        {
            this.rng = rng;
            this.size = size;

            var constants = ConstantsCache.GetOrAdd(
                (size, theta),
                static key => new Lazy<ZipfConstants>(
                    () => CreateConstants(key.Size, key.Theta),
                    LazyThreadSafetyMode.ExecutionAndPublication)).Value;
            zetaN = constants.ZetaN;
            alpha = constants.Alpha;
            cutoff2 = constants.Cutoff2;
            eta = constants.Eta;
        }

        private static ZipfConstants CreateConstants(int size, double theta)
        {
            var zetaN = Zeta(size, theta);
            var alpha = 1.0 / (1.0 - theta);
            var cutoff2 = Math.Pow(0.5, theta);
            var zeta2 = Zeta(2, theta);
            var eta = (1.0 - Math.Pow(2.0 / size, 1.0 - theta)) / (1.0 - zeta2 / zetaN);
            return new ZipfConstants(zetaN, alpha, cutoff2, eta);
        }

        private static double Zeta(int count, double theta)
        {
            double zetaN = 0.0;
            for (var ii = 1; ii <= count; ii++)
                zetaN += 1.0 / Math.Pow(ii, theta);
            return zetaN;
        }

        public int Next()
        {
            double u = (double)rng.Generate64(int.MaxValue) / int.MaxValue;
            double uz = u * zetaN;
            if (uz < 1)
                return 0;
            if (uz < 1 + cutoff2)
                return 1;
            return (int)(size * Math.Pow(eta * u - eta + 1, alpha));
        }

        private readonly record struct ZipfConstants(double ZetaN, double Alpha, double Cutoff2, double Eta);
    }
}