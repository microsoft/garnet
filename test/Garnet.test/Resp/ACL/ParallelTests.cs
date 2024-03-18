// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.server.ACL;
using NUnit.Framework;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Tests that operate in parallel on the ACL
    /// </summary>
    [TestFixture]
    internal class ParallelTests : AclTest
    {
        /// <summary>
        /// Creates and starts the Garnet test server
        /// </summary>
        [SetUp]
        public virtual void Setup()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, useAcl: true);
            server.Start();
        }

        /// <summary>
        /// Tests that AUTH works in parallel without corrupting the server state
        /// </summary>
        [TestCase(128, 2048)]
        public async Task ParallelAuthTest(int degreeOfParallelism, int iterationsPerSession)
        {
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Add the test user and password
            var response = await c.ExecuteAsync("ACL", "SETUSER", TestUserA, "on", $">{DummyPassword}");
            Assert.IsTrue(response.StartsWith("OK"));

            // Run multiple sessions that stress AUTH
            Parallel.For(0, degreeOfParallelism, (t, state) =>
            {
                using var c = TestUtils.GetGarnetClientSession();
                c.Connect();

                for (uint i = 0; i < iterationsPerSession; i++)
                {
                    // Execute two AUTH commands - one that succeeds and one that fails
                    c.Execute("AUTH", TestUserA, DummyPassword);
                    c.Execute("AUTH", DummyPasswordB);
                }
            });
        }

        /// <summary>
        /// Tests that password hashing works in parallel
        /// </summary>
        [TestCase(128, 2048)]
        public void ParallelPasswordHashTest(int degreeOfParallelism, int iterationsPerSession)
        {
            // Run multiple sessions that stress password hashing
            Parallel.For(0, degreeOfParallelism, (t, state) =>
            {
                for (uint i = 0; i < iterationsPerSession; i++)
                {
                    ACLPassword.ACLPasswordFromString(DummyPassword);
                    ACLPassword.ACLPasswordFromString(DummyPasswordB);
                }
            });
        }
    }
}