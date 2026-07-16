// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Tests that RESP commands that _overwrite_ Vector Sets correctly cause them to be cleaned up.
    /// </summary>
    [TestFixture]
    public class VectorSetOverwriteTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = CreateGarnetServer(tryRecover: false);

            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Create a new GarnetServer instance with common parameters.
        /// </summary>
        private static GarnetServer CreateGarnetServer(bool tryRecover)
        => TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, tryRecover: tryRecover, enableVectorSetPreview: true, enableRangeIndexPreview: true);

        [Test]
        public void AllOverwritingCommandsCovered()
        {
            var toCheck = VectorSetWrongTypeTests.GetOverwritingCommands();

            var missing = new List<RespCommand>();
            foreach (var cmd in toCheck)
            {
                var mtd = GetType().GetMethod($"{cmd.ToString()}Async");
                if (mtd == null || mtd.GetCustomAttribute<TestAttribute>() == null)
                {
                    missing.Add(cmd);
                }
            }

            if (missing.Count > 0)
            {
                var missingCmds = string.Join(", ", missing.OrderBy(static x => x.ToString()));

                ClassicAssert.Fail($"Missing tests for {missing.Count:N0} commands: {missingCmds}");
            }
        }
    }
}
