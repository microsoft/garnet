// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;

namespace Garnet.test.Resp.ACL
{
    /// <summary>
    /// Base class used for all RESP ACL tests
    /// </summary>
    abstract class AclTest
    {
        /// <summary>
        /// Dummy password used by some of the tests.
        /// </summary>
        protected const string DummyPassword = "passw0rd";

        /// <summary>
        /// Matching expected password hash for DummyPassword
        /// </summary>
        protected const string DummyPasswordHash = "8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9";

        /// <summary>
        /// Second dummy password used by some of the tests.
        /// </summary>
        protected const string DummyPasswordB = "paSSw0rd";

        /// <summary>
        /// Non-default test user
        /// </summary>
        protected const string TestUserA = "testUser";

        /// <summary>
        /// Non-default test user
        /// </summary>
        protected const string TestUserB = "otherUser";

        /// <summary>
        /// Username for unknown user
        /// </summary>
        protected const string TestUserUnknown = "DoesNotExist";

        /// <summary>
        /// Garnet server instance to use in the tests.
        /// </summary>
        protected GarnetServer server = null;

        /// <summary>
        /// Creates working directory
        /// </summary>
        [SetUp]
        public virtual void BaseSetup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            Directory.CreateDirectory(TestUtils.MethodTestDir);
        }

        /// <summary>
        /// Cleans up any running working instances
        /// </summary>
        [TearDown]
        public virtual void BaseTearDown()
        {
            if (server != null)
            {
                server.Dispose();
                server = null;
            }

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }
    }
}