// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using Garnet.common;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class AsciiUtilsTests : TestBase
    {
        [TestCase("auth2", "AUTH2", true)]
        [TestCase("vector_set_contexts", "VECTOR_SET_CONTEXTS", true)]
        [TestCase("auth3", "AUTH2", false)]
        [TestCase("vector-set-contexts", "VECTOR_SET_CONTEXTS", false)]
        [TestCase("authR", "AUTH2", false)]
        public void UpperCaseComparisonHandlesNonAlphabeticCharacters(string left, string right, bool expected)
        {
            ReadOnlySpan<byte> leftBytes = Encoding.ASCII.GetBytes(left);
            var rightBytes = Encoding.ASCII.GetBytes(right);

            Assert.That(leftBytes.EqualsUpperCaseSpanIgnoringCase(rightBytes), Is.EqualTo(expected));
        }
    }
}