// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class ExpandableMapTests : TestBase
    {
        [Test]
        public void SupportsSingleEntryRange()
        {
            var map = new ExpandableMap<string>(1, 0, 0);

            Assert.That(map.TrySetValue(0, "value"), Is.True);
            Assert.That(map.TryGetValue(0, out var value), Is.True);
            Assert.That(value, Is.EqualTo("value"));
            Assert.That(map.TrySetValue(1, "outside"), Is.False);
        }
    }
}