// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
    public class RespParseFuzzRegressionTests
    {
        [Test]
        [Category("FUZZING")]
        public void MakeUpperCaseAccessViolation()
        {
            const int Iterations = 1_000;

            // This intermittently causes an access violation.

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
                }
            }
        }
    }
}
