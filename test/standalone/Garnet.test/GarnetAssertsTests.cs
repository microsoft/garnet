// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class GarnetAssertsTests : TestBase
    {
        [Test]
        public void SuccessfulConditionSkipsInterpolation()
        {
            var evaluations = 0;
            string Message()
            {
                evaluations++;
                return "unused";
            }

            _ = Format(true, $"message: {Message()}");

            Assert.That(evaluations, Is.Zero);
        }

        [Test]
        public void FormatsMessageValuesAndAlignment()
        {
            var text = "value";
            var number = 42;
            ReadOnlySpan<char> span = "span";

            var message = Format(false, $"{text} {number} {number:X4} |{number,4}| |{number,-6:X4}| {span} |{span,6}| |{text,7}|");

            Assert.That(message, Is.EqualTo("value 42 002A |  42| |002A  | span |  span| |  value|"));
        }

        private static string Format(bool condition, [InterpolatedStringHandlerArgument(nameof(condition))] ref AssertMessageHandler message)
            => condition ? null : message.GetFormattedText();
    }
}