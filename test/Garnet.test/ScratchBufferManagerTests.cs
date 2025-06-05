// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    public class ScratchBufferManagerTests
    {
        [Test]
        public void Test()
        {
            var sbm = new ScratchBufferManager();
            var as1 = sbm.CreateArgSlice("Hello");
            var as2 = sbm.CreateArgSlice("My name is Inigo Montoya. You killed my father. Prepare to die");
            ClassicAssert.IsTrue(sbm.RewindScratchBuffer(ref as2));
            ClassicAssert.IsTrue(sbm.RewindScratchBuffer(ref as1));
        }
    }
}
