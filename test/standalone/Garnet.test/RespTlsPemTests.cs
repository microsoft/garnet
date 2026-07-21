// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Verifies that a GarnetServer configured with a PEM-encoded certificate (instead of the usual .pfx)
    /// serves RESP traffic over TLS correctly.
    /// </summary>
    [TestFixture]
    public class RespTlsPemTests : TestBase
    {
        GarnetServer server;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: true,
                tlsCertFileName: TestUtils.pemCertFile, tlsCertPassword: TestUtils.pemCertKeyFile);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void PemCertSingleSetGet()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(disablePubSub: true, useTLS: true));
            var db = redis.GetDatabase(0);

            const string origValue = "abcdefg";
            db.StringSet("mykey", origValue);

            string retValue = db.StringGet("mykey");

            ClassicAssert.AreEqual(origValue, retValue);
        }

        [Test]
        public async Task PemCertSingleSetGetGarnetClient()
        {
            using var db = TestUtils.GetGarnetClient(useTLS: true);
            db.Connect();

            const string origValue = "abcdefg";
            await db.StringSetAsync("mykey", origValue).ConfigureAwait(false);

            string retValue = await db.StringGetAsync("mykey").ConfigureAwait(false);

            ClassicAssert.AreEqual(origValue, retValue);
        }
    }
}