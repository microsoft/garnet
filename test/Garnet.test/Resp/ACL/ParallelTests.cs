// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.server.ACL;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Tests that operate in parallel on the ACL
    /// </summary>
    [AllureNUnit]
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
            ClassicAssert.IsTrue(response.StartsWith("OK"));

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

        /// <summary>
        /// Tests that ACL SETUSER works in parallel without corrupting the user's ACL.
        ///
        /// Test launches multiple single-threaded clients that apply two simple ACL changes to the same user many times
        /// in parallel. Validates that ACL result after each execution is one of the possible valid responses.
        ///
        /// Race conditions are not deterministic so test uses repeat.
        ///
        /// </summary>
        [TestCase(128, 2048)]
        [Repeat(2)]
        public async Task ParallelAclSetUserTest(int degreeOfParallelism, int iterationsPerSession)
        {
            string activeUserWithGetCommand = $"ACL SETUSER {TestUserA} on >{DummyPassword} +get";
            string inactiveUserWithoutGetCommand = $"ACL SETUSER {TestUserA} off >{DummyPassword} -get";

            // This is a combination of the two commands above indicative of threading issues.
            string inactiveUserWithGet = $"user {TestUserA} off #{DummyPasswordHash} +get";

            // Use client with support for single thread.
            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();
            _ = await c.ExecuteAsync(activeUserWithGetCommand.Split(" "));

            await Parallel.ForAsync(0, degreeOfParallelism, async (t, state) =>
            {
                using var c = TestUtils.GetGarnetClientSession();
                c.Connect();

                for (uint i = 0; i < iterationsPerSession; i++)
                {
                    await c.ExecuteAsync(activeUserWithGetCommand.Split(" "));
                    await c.ExecuteAsync(inactiveUserWithoutGetCommand.Split(" "));

                    var aclListResponse = await c.ExecuteForArrayAsync("ACL", "LIST");

                    if (aclListResponse.Contains(inactiveUserWithGet))
                    {
                        string corruptedAcl = aclListResponse.First(line => line.Contains(TestUserA));
                        throw new AssertionException($"Invalid ACL: {corruptedAcl}");
                    }
                }
            });
        }

        /// <summary>
        /// Tests that ACL SETUSER works in parallel without fatal contention on user in ACL map.
        ///
        /// Test launches multiple single-threaded clients that apply the same ACL change to the same user. Creates race
        /// to become the first client to add the user to the ACL. Throws after initial insert into ACL if threading issues exist.
        ///
        /// Race conditions are not deterministic so test uses repeat.
        ///
        /// </summary>
        [TestCase(128)]
        [Repeat(5)]
        public async Task ParallelAclSetUserAvoidsMapContentionTest(int degreeOfParallelism)
        {
            string setUserCommand = $"ACL SETUSER {TestUserA} on >{DummyPassword}";

            await Parallel.ForAsync(0, degreeOfParallelism, async (t, state) =>
            {
                // Use client with support for single thread.
                using var c = TestUtils.GetGarnetClientSession();
                c.Connect();
                await c.ExecuteAsync(setUserCommand.Split(" "));
            });

            ClassicAssert.Pass();
        }
    }
}