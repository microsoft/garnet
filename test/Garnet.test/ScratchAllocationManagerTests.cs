// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    public class ScratchAllocationManagerTests
    {
        [Test]
        public void Test()
        {
            var sam = new ScratchAllocationManager();
            var as1 = sam.CreateArgSlice("Hello");
            var as2 = sam.CreateArgSlice("My name is Inigo Montoya. You killed my father. Prepare to die");
            ClassicAssert.IsTrue(sam.RewindScratchBuffer(ref as2));
            ClassicAssert.IsTrue(sam.RewindScratchBuffer(ref as1));
        }
    }
}
