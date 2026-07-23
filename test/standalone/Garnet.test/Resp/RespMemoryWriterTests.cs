// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp
{
    /// <summary>
    /// Tests for RespMemoryWriter's output buffer growth logic.
    ///
    /// Regression coverage for https://github.com/microsoft/garnet/issues/1616, where
    /// HGETALL (and, in principle, any command that writes a large RESP response through
    /// RespMemoryWriter, such as HRANDFIELD WITHVALUES, SMEMBERS, or ZRANGE on large
    /// collections) could throw a confusing ArgumentOutOfRangeException/OverflowException
    /// once the output buffer's capacity, while doubling to accommodate the response,
    /// crossed certain boundaries near 2^30-2^31 bytes. The root cause was that the growth
    /// calculation was done with `int`/`uint` arithmetic that could wrap around to a value
    /// smaller than what had already been written, which then made a subsequent
    /// Buffer.MemoryCopy call fail.
    /// </summary>
    [TestFixture]
    public class RespMemoryWriterTests
    {
        /// <summary>
        /// For "ordinary" growth requests (i.e. the buffer already has more room than the
        /// hint requires), the buffer should simply double, and the result must never be
        /// smaller than the current capacity.
        /// </summary>
        [TestCase(0, 0, false, 1024)]        // below the 512 floor -> floor (512), then doubled
        [TestCase(100, 10, false, 1024)]     // below the 512 floor -> floor, then doubled
        [TestCase(768, 10, false, 1024)]     // non-power-of-two but still below the 1024 floor threshold -> floor, then doubled
        [TestCase(1024, 10, false, 2048)]    // at/above the floor threshold -> plain doubling
        [TestCase(0, 0, true, 16)]           // below the 8 floor -> floor, then doubled
        [TestCase(10, 2, true, 16)]
        public static void ComputeGrowth_OrdinaryGrowth_DoublesAndNeverShrinks(int currentLength, int extraLenHint, bool lowerMinimum, int expected)
        {
            var result = RespMemoryWriter.ComputeGrowth(currentLength, extraLenHint, lowerMinimum);

            ClassicAssert.AreEqual(expected, result);
            ClassicAssert.GreaterOrEqual(result, currentLength);
            ClassicAssert.GreaterOrEqual(result, extraLenHint);
        }

        /// <summary>
        /// The exact boundary the original bug report crossed: as an output buffer for a
        /// large HGETALL response keeps doubling, its capacity passes through and beyond
        /// 0x40000000 (2^30) and 0x80000000 (2^31). At no point should the computed
        /// capacity wrap around into a small or negative value - it must always be able to
        /// hold at least what's already been written (modeled here by currentLength) plus
        /// the incoming item (extraLenHint), up to the Array.MaxLength ceiling.
        /// </summary>
        [TestCase(0x3FFFFFFF)] // just below 2^30
        [TestCase(0x40000000)] // exactly 2^30 (the old special-cased value)
        [TestCase(0x40000001)] // just above 2^30 (previously NOT special-cased -> bug)
        [TestCase(0x60000000)] // 1.5 * 2^30 - not a power of two, reachable when the buffer's
                               // starting size isn't a power of two
        [TestCase(0x7FFFFFC6)] // Array.MaxLength - 1
        [TestCase(0x7FFFFFC7)] // exactly Array.MaxLength
        public static void ComputeGrowth_NearMaxCapacity_NeverShrinksOrOverflows(int currentLength)
        {
            var result = RespMemoryWriter.ComputeGrowth(currentLength, extraLenHint: 64, lowerMinimum: false);

            ClassicAssert.Positive(result, "Growth must never compute a non-positive capacity.");
            ClassicAssert.GreaterOrEqual((long)result, currentLength,
                "Growth must never shrink below what has already been allocated/written.");
            ClassicAssert.LessOrEqual(result, Array.MaxLength,
                "Growth must be clamped to Array.MaxLength, the hard ceiling for a single buffer.");
        }

        /// <summary>
        /// When a single item is larger than the current buffer (the `length &lt; extraLenHint`
        /// branch), the sum of the two must be computed without wraparound, even when that
        /// sum is itself close to or beyond Array.MaxLength.
        /// </summary>
        [TestCase(100, int.MaxValue - 1000)]
        [TestCase(1024, 0x7FFFFFC0)]
        public static void ComputeGrowth_LargeSingleItem_NeverShrinksOrOverflows(int currentLength, int extraLenHint)
        {
            var result = RespMemoryWriter.ComputeGrowth(currentLength, extraLenHint, lowerMinimum: false);

            ClassicAssert.Positive(result);
            ClassicAssert.LessOrEqual(result, Array.MaxLength);
        }

        /// <summary>
        /// Both the current buffer capacity AND the incoming single item are simultaneously
        /// large (each close to its individual maximum: <c>currentLength</c> near
        /// <see cref="Array.MaxLength"/>, <c>extraLenHint</c> near <see cref="int.MaxValue"/>).
        /// The <c>TestCase</c> values below are deliberately kept within <see cref="int"/>'s
        /// valid range, since a real <c>extraLenHint</c> always comes from an actual
        /// `int`-typed `Span&lt;byte&gt;.Length`/`string.Length` and can never exceed
        /// <see cref="int.MaxValue"/>. Note that with two genuinely legitimate (non-negative)
        /// <see cref="int"/> operands, their sum can reach at most
        /// <c>2 * int.MaxValue = 2^32 - 2</c>, which is one less than
        /// <see cref="uint.MaxValue"/> - so the old code's `(uint)extraLenHint + (uint)length`
        /// addition itself could never actually overflow uint32 for any input this method
        /// could legitimately be called with. The real old-code hazard was the *cast* of
        /// `RoundUpToPowerOf2`'s `uint` result down to `int` once the total exceeded 2^31,
        /// which is what this case (and <see cref="ComputeGrowth_LargeSingleItem_NeverShrinksOrOverflows"/>
        /// above) exercises with legitimate inputs.
        /// </summary>
        [TestCase(0x7FFFFFC6, 0x7FFFFF00)] // currentLength = Array.MaxLength - 1, extraLenHint = Array.MaxLength - 199 (still satisfiable)
        [TestCase(0x7FFFFFC7, 0x7FFFFF00)] // currentLength = Array.MaxLength, extraLenHint = Array.MaxLength - 199
        public static void ComputeGrowth_LargeLengthAndLargeSingleItem_NeverShrinksOrOverflows(int currentLength, int extraLenHint)
        {
            var result = RespMemoryWriter.ComputeGrowth(currentLength, extraLenHint, lowerMinimum: false);

            ClassicAssert.Positive(result, "Growth must never compute a non-positive capacity.");
            ClassicAssert.GreaterOrEqual((long)result, currentLength,
                "Growth must never shrink below what has already been allocated/written.");
            ClassicAssert.GreaterOrEqual((long)result, extraLenHint,
                "Growth must be large enough to actually hold the incoming item.");
            ClassicAssert.LessOrEqual(result, Array.MaxLength,
                "Growth must be clamped to Array.MaxLength, the hard ceiling for a single buffer.");
        }

        /// <summary>
        /// Regression coverage for the actual gap kevin-montrose identified in review: once the
        /// buffer is already sitting at the <see cref="Array.MaxLength"/> ceiling, growth must
        /// report "no further growth is possible" (i.e. return exactly <c>currentLength</c>, not
        /// something &gt; it) so that <c>ReallocateOutput</c>'s `length &lt;= previousLength` check
        /// can tell the difference between real progress and a plateaued buffer, and raise a
        /// clear, client-visible <see cref="GarnetException"/> (`disposeSession: false`) instead
        /// of either looping forever re-renting an identically-sized buffer or overflowing into
        /// an undersized one. This is what actually happens once a single RESP response (e.g. a
        /// sufficiently large HGETALL) cannot fit in one managed buffer, regardless of exactly
        /// which arithmetic step the pre-fix code failed on for the original report.
        /// </summary>
        [TestCase(0)]
        [TestCase(1)]
        [TestCase(1024)]
        [TestCase(0x7FFFFF00)]
        public static void ComputeGrowth_AtMaxCapacity_ReportsNoFurtherGrowthPossible(int extraLenHint)
        {
            var result = RespMemoryWriter.ComputeGrowth(Array.MaxLength, extraLenHint, lowerMinimum: false);

            ClassicAssert.AreEqual(Array.MaxLength, result,
                "Once already at the ceiling, ComputeGrowth must signal 'no growth occurred' " +
                "(result == currentLength) rather than something that looks like progress.");
        }

        /// <summary>
        /// Sanity check that repeatedly applying ComputeGrowth - modeling the buffer
        /// doubling many times over the course of writing a very large response, starting
        /// from a non-power-of-two initial capacity as the live server does - is monotonic
        /// and never produces a capacity smaller than a previous one, all the way up to the
        /// Array.MaxLength ceiling.
        /// </summary>
        [Test]
        public static void ComputeGrowth_RepeatedDoubling_IsMonotonicUpToMaxLength()
        {
            var length = 768; // deliberately not a power of two
            for (var i = 0; i < 100; i++)
            {
                var next = RespMemoryWriter.ComputeGrowth(length, extraLenHint: 64, lowerMinimum: false);

                ClassicAssert.GreaterOrEqual(next, length, $"Iteration {i}: capacity shrank from {length} to {next}.");
                ClassicAssert.LessOrEqual(next, Array.MaxLength);

                if (next == length)
                {
                    // We've reached (and stayed at) the ceiling; further growth is impossible
                    // and callers are expected to report a clear error rather than loop forever.
                    ClassicAssert.AreEqual(Array.MaxLength, next);
                    break;
                }

                length = next;
            }
        }
    }
}