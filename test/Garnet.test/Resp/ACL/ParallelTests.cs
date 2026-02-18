// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.client;
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
            var sharedEpoch = new LightEpoch();
            try
            {
                using var c = TestUtils.GetGarnetClient(epoch: sharedEpoch);
                await c.ConnectAsync();

                // Add the test user and password
                var response = await c.ExecuteForStringResultAsync("ACL", ["SETUSER", TestUserA, "on", $">{DummyPassword}"]);
                ClassicAssert.IsTrue(response.StartsWith("OK"));

                // Run multiple sessions that stress AUTH
                var timeout = TimeSpan.FromSeconds(120);
                await Parallel.ForAsync(0, degreeOfParallelism, async (t, token) =>
                {
                    using var c = TestUtils.GetGarnetClient(epoch: sharedEpoch);
                    await c.ConnectAsync(token);

                    var firstResponseTasks = new Task<string>[iterationsPerSession];
                    var secondResponseTasks = new Task<string>[iterationsPerSession];
                    for (uint i = 0; i < iterationsPerSession; i++)
                    {
                        // Execute two AUTH commands - one that succeeds and one that fails
                        firstResponseTasks[i] = c.ExecuteForStringResultAsync("AUTH", [TestUserA, DummyPassword]).WaitAsync(token);
                        secondResponseTasks[i] = c.ExecuteForStringResultAsync("AUTH", [TestUserA, DummyPasswordB]).WaitAsync(token);
                    }

                    try
                    {
                        await Task.WhenAll(firstResponseTasks.Concat(secondResponseTasks)).WaitAsync(token);
                    }
                    catch { }
                    foreach (var task in firstResponseTasks)
                    {
                        ClassicAssert.IsTrue(task.Result.StartsWith("OK"));
                    }
                    foreach (var task in secondResponseTasks)
                    {
                        ClassicAssert.IsTrue(task.IsFaulted);
                        ClassicAssert.IsTrue(task.Exception.InnerExceptions.Count == 1);
                        ClassicAssert.IsTrue(task.Exception.InnerExceptions[0].Message.StartsWith("WRONGPASS"));
                    }
                }).WaitAsync(timeout).ConfigureAwait(false);
            }
            finally
            {
                sharedEpoch.Dispose();
            }
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
            var sharedEpoch = new LightEpoch();
            try
            {
                string activeUserWithGetCommand = $"SETUSER {TestUserA} on >{DummyPassword} +get";
                string inactiveUserWithoutGetCommand = $"SETUSER {TestUserA} off >{DummyPassword} -get";

                // This is a combination of the two commands above indicative of threading issues.
                string inactiveUserWithGet = $"user {TestUserA} off #{DummyPasswordHash} +get";

                // Use client with support for single thread.
                using var c = TestUtils.GetGarnetClient(epoch: sharedEpoch);
                await c.ConnectAsync();
                var response = await c.ExecuteForStringResultAsync("ACL", activeUserWithGetCommand.Split(" "));
                ClassicAssert.IsTrue(response.StartsWith("OK"));

                var timeout = TimeSpan.FromSeconds(120);
                await Parallel.ForAsync(0, degreeOfParallelism, async (t, token) =>
                {
                    using var c = TestUtils.GetGarnetClient(epoch: sharedEpoch);
                    await c.ConnectAsync(token);

                    for (uint i = 0; i < iterationsPerSession; i++)
                    {
                        var response1 = await c.ExecuteForStringResultAsync("ACL", activeUserWithGetCommand.Split(" ")).WaitAsync(token);
                        ClassicAssert.IsTrue(response1.StartsWith("OK"));

                        var response2 = await c.ExecuteForStringResultAsync("ACL", inactiveUserWithoutGetCommand.Split(" ")).WaitAsync(token);
                        ClassicAssert.IsTrue(response2.StartsWith("OK"));

                        var aclListResponse = await c.ExecuteForStringArrayResultAsync("ACL", ["LIST"]).WaitAsync(token);

                        if (aclListResponse.Contains(inactiveUserWithGet))
                        {
                            string corruptedAcl = aclListResponse.First(line => line.Contains(TestUserA));
                            throw new AssertionException($"Invalid ACL: {corruptedAcl}");
                        }
                    }
                }).WaitAsync(timeout).ConfigureAwait(false);
            }
            finally
            {
                sharedEpoch.Dispose();
            }
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
        [TestCase(128, 2048)]
        [Repeat(2)]
        public async Task ParallelAclSetUserAvoidsMapContentionTest(int degreeOfParallelism, int iterationsPerSession)
        {
            var sharedEpoch = new LightEpoch();
            try
            {
                string setUserCommand = $"SETUSER {TestUserA} on >{DummyPassword}";

                var timeout = TimeSpan.FromSeconds(120);
                await Parallel.ForAsync(0, degreeOfParallelism, async (t, token) =>
                {
                    // Use client with support for single thread.
                    using var c = TestUtils.GetGarnetClient(epoch: sharedEpoch);
                    await c.ConnectAsync(token);
                    for (uint i = 0; i < iterationsPerSession; i++)
                    {
                        var response = await c.ExecuteForStringResultAsync("ACL", setUserCommand.Split(" ")).WaitAsync(token);
                        ClassicAssert.IsTrue(response.StartsWith("OK"));
                    }
                }).WaitAsync(timeout).ConfigureAwait(false);
            }
            finally
            {
                sharedEpoch.Dispose();
            }
        }
    }
}