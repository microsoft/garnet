// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Covers <see cref="Utility.GetHashCodeWithMix"/>, whose reason for existing is the avalanche property
    /// rather than the hash value itself. Callers reduce the result modulo a power of two, so a weak bit
    /// anywhere in the word silently biases the value they end up using.
    /// </summary>
    [TestFixture]
    public class UtilityTests : TestBase
    {
        /// <summary>
        /// Trials per input bit. Each cell of the avalanche matrix is a proportion over this many samples,
        /// so the standard error is sqrt(0.25 / trials) ~= 0.011. <see cref="MaxAcceptableBias"/> sits about
        /// nine standard errors out, which keeps the 8192 cells free of false failures while staying far
        /// below the 0.5 an unmixed multiply-accumulate produces.
        /// </summary>
        private const int Trials = 2048;

        private const int InputBytes = 16;

        /// <summary>
        /// Largest tolerated departure from the ideal 0.5 flip probability.
        /// </summary>
        private const double MaxAcceptableBias = 0.1;

        /// <summary>
        /// Flipping any single input bit must flip every output bit about half the time. Multiplication
        /// carries information only toward higher bits, so without the finalizer the low bits of the result
        /// depend on almost none of the input and this fails with a bias of exactly 0.5.
        /// </summary>
        [Test]
        public void MixAvalanchesEveryOutputBit()
        {
            var random = new Random(1);
            var flips = new int[InputBytes * 8, 64];
            var original = new byte[InputBytes];

            for (var trial = 0; trial < Trials; trial++)
            {
                random.NextBytes(original);
                var baseline = (ulong)Utility.GetHashCodeWithMix(original);

                for (var inputBit = 0; inputBit < InputBytes * 8; inputBit++)
                {
                    original[inputBit / 8] ^= (byte)(1 << (inputBit % 8));
                    var delta = baseline ^ (ulong)Utility.GetHashCodeWithMix(original);
                    original[inputBit / 8] ^= (byte)(1 << (inputBit % 8));

                    while (delta != 0)
                    {
                        var outputBit = System.Numerics.BitOperations.TrailingZeroCount(delta);
                        flips[inputBit, outputBit]++;
                        delta &= delta - 1;
                    }
                }
            }

            var worstBias = 0.0;
            var worstInputBit = 0;
            var worstOutputBit = 0;

            for (var inputBit = 0; inputBit < InputBytes * 8; inputBit++)
            {
                for (var outputBit = 0; outputBit < 64; outputBit++)
                {
                    var bias = Math.Abs(((double)flips[inputBit, outputBit] / Trials) - 0.5);
                    if (bias > worstBias)
                        (worstBias, worstInputBit, worstOutputBit) = (bias, inputBit, outputBit);
                }
            }

            Assert.That(worstBias, Is.LessThan(MaxAcceptableBias),
                $"Flipping input bit {worstInputBit} changes output bit {worstOutputBit} with probability " +
                $"{(double)flips[worstInputBit, worstOutputBit] / Trials:F3} rather than ~0.5, so that output " +
                $"bit does not carry the whole input and reducing the hash modulo a power of two is biased.");
        }

        /// <summary>
        /// Sequential inputs must spread evenly over the low bits, which are the bits callers take when
        /// reducing modulo a power of two. This is a weaker guard than <see cref="MixAvalanchesEveryOutputBit"/>
        /// - an unmixed multiply-accumulate also passes it - so it is here to pin the distribution property
        /// callers rely on, not to detect a missing finalizer.
        /// </summary>
        [Test]
        public void MixSpreadsSequentialInputsAcrossLowBits()
        {
            const int buckets = 512;
            const int samples = buckets * 32;

            var counts = new int[buckets];
            foreach (var value in Sequential(samples))
                counts[(int)((ulong)Utility.GetHashCodeWithMix(value) % buckets)]++;

            // Chi-square over a uniform expectation; 512 buckets at 32 per bucket has a 0.001 critical
            // value near 650, so a hash that leaks structure from the inputs lands far outside it.
            var expected = (double)samples / buckets;
            var chiSquare = 0.0;
            foreach (var count in counts)
                chiSquare += (count - expected) * (count - expected) / expected;

            Assert.That(chiSquare, Is.LessThan(650.0),
                $"Sequential inputs produced a chi-square of {chiSquare:F1} over {buckets} buckets, so the " +
                $"low bits still carry the structure of the input.");
        }

        /// <summary>
        /// The value has to be a pure function of the bytes. Callers depend on separate processes deriving
        /// the same result from the same input, which rules out anything seeded per process.
        /// </summary>
        [Test]
        public void MixIsDeterministic()
        {
            byte[] bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9];

            ClassicAssert.AreEqual(Utility.GetHashCodeWithMix(bytes), Utility.GetHashCodeWithMix(bytes));
            ClassicAssert.AreEqual(Utility.GetHashCodeWithMix(bytes), Utility.GetHashCodeWithMix(bytes.AsSpan()));
        }

        [Test]
        public void MixHandlesEmptyAndOddLengthInput()
        {
            Assert.DoesNotThrow(() => Utility.GetHashCodeWithMix([]));
            Assert.That(Utility.GetHashCodeWithMix([7]),
                Is.Not.EqualTo(Utility.GetHashCodeWithMix([7, 7])));
        }

        private static IEnumerable<byte[]> Sequential(int count)
        {
            for (var i = 0; i < count; i++)
                yield return BitConverter.GetBytes(i);
        }
    }
}