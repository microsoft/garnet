// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Allure.NUnit;
using Garnet.common.Parsing;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp
{
    /// <summary>
    /// Tests which validate the fixes introduced for bugs found during fuzz testing do not regress.
    /// 
    /// Accordingly these are a bit of a grab bag, but they need to go somewhere.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    [Category("FUZZING")]
    public class RespParseFuzzRegressionTests : AllureTestBase
    {
        [Test]
        public void MakeUpperCaseAccessViolation()
        {
            const int Iterations = 1_000;

            // This intermittently causes an access violation, so we stress it a bunch.

            byte[] ex1 = [0x2A, 0x33, 0x0D, 0x0A, 0x24, 0x36, 0x0D, 0x0A, 0x6C, 0x78, 0x4A, 0x43, 0x34, 0x0D, 0xFF, 0x00, 0xFF, 0x87, 0x0D, 0x00, 0x87];
            byte[] ex2 = [0x2A, 0x31, 0x0D, 0x0A, 0x24, 0x37, 0x0D, 0x0A, 0x0D, 0x32, 0x0D, 0x6B, 0x6B, 0x6B, 0x6B, 0x6B, 0x6B, 0x6B, 0x6B];
            byte[] ex3 = [0x2A, 0x32, 0x0D, 0x0A, 0x24, 0x35, 0x0D, 0x0A, 0x42, 0x38, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0xFF, 0xFF, 0x00, 0x87];

            byte[][] allExamples = [ex1, ex2, ex3];

            var session = new RespServerSession();

            foreach (var ex in allExamples)
            {
                for (var iter = 0; iter < Iterations; iter++)
                {
                    // Memory must be pinned, and we need it to jitter around to reproduce
                    var copy = GC.AllocateUninitializedArray<byte>(ex.Length, pinned: true);
                    ex.AsSpan().CopyTo(copy);

                    _ = ClassicAssert.Throws<RespParsingException>(() => session.FuzzParseCommandBuffer(copy, out _));

                    GC.KeepAlive(copy);
                }
            }
        }

        [Test]
        public void FastParseArrayCommandLongCommandAssertFailure()
        {
            ReadOnlySpan<byte> example = [0x2A, 0x32, 0x0D, 0x0A, 0x24, 0x36, 0x25, 0x0D, 0x0A, 0x43, 0x34, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x87];

            // Memory must be pinned
            var copy = GC.AllocateUninitializedArray<byte>(example.Length, pinned: true);

            var session = new RespServerSession();

            // All values must correctly be rejected after the assert was removed
            for (var val = 0; val <= byte.MaxValue; val++)
            {
                if (val is >= '0' and <= '9')
                {
                    // Numbers are valid, and should be ignored
                    continue;
                }

                example.CopyTo(copy);
                copy[6] = (byte)val;

                _ = ClassicAssert.Throws<RespParsingException>(() => session.FuzzParseCommandBuffer(copy, out _));
            }

            GC.KeepAlive(copy);
        }

        [Test]
        public void FastParseArrayCommandShortCommandAssertFailure()
        {
            ReadOnlySpan<byte> example = [0x2A, 0x32, 0x30, 0x0D, 0x0A, 0x24, 0x3C, 0x0D, 0x0A, 0x24, 0x32, 0x0D, 0x0A];

            // Memory must be pinned
            var copy = GC.AllocateUninitializedArray<byte>(example.Length, pinned: true);

            var session = new RespServerSession();
            for (var val = 0; val <= byte.MaxValue; val++)
            {
                if (val is >= '1' and <= '9')
                {
                    // Numbers (but not a leading 0) are valid, and should be ignored
                    continue;
                }

                example.CopyTo(copy);
                copy[6] = (byte)val;

                // All values must correctly be rejected after the assert was removed
                _ = ClassicAssert.Throws<RespParsingException>(() => session.FuzzParseCommandBuffer(copy, out _));
            }

            GC.KeepAlive(copy);
        }

        [Test]
        public void FastParseArrayCommandArrayCountAssertFailure()
        {
            ReadOnlySpan<byte> example = [0x2A, 0x41, 0x0D, 0x0A, 0x24, 0x35, 0x0D, 0x0A, 0x78, 0x78, 0x78, 0x78, 0x78, 0x7C, 0x78];

            // Memory must be pinned
            var copy = GC.AllocateUninitializedArray<byte>(example.Length, pinned: true);

            var session = new RespServerSession();
            // All values must correctly be rejected after the assert was removed
            for (var val = 0; val <= byte.MaxValue; val++)
            {
                if (val is >= '1' and <= '9')
                {
                    // Numbers are valid, and should be ignored
                    continue;
                }

                example.CopyTo(copy);
                copy[1] = (byte)val;

                _ = ClassicAssert.Throws<RespParsingException>(() => session.FuzzParseCommandBuffer(copy, out _));
            }

            GC.KeepAlive(copy);
        }
    }
}