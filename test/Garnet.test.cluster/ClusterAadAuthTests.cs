// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using Allure.NUnit;
using Garnet.server.Auth.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using NUnit.Framework;
using NUnit.Framework.Legacy;


namespace Garnet.test.cluster
{

    [AllureNUnit]
    [TestFixture]
    [NonParallelizable]
    class ClusterAadAuthTests : AllureTestBase
    {
        ClusterTestContext context;

        readonly Dictionary<string, LogLevel> monitorTests = [];

        private const string issuer = "https://sts.windows.net/975f013f-7f24-47e8-a7d3-abc4752bf346/";

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests);
        }

        [TearDown]
        public void TearDown()
        {
            context.TearDown();
        }

        [Test, Order(1)]
        [Category("CLUSTER-AUTH"), CancelAfter(60000)]
        public void ValidateClusterAuthWithObjectId()
        {
            var nodes = 2;
            var audience = Guid.NewGuid().ToString();
            JwtTokenGenerator tokenGenerator = new JwtTokenGenerator(issuer, audience);

            var appId = Guid.NewGuid().ToString();
            var objId = Guid.NewGuid().ToString();
            var tokenClaims = new List<Claim>
            {
                new Claim("appidacr","1"),
                new Claim("appid", appId),
                new Claim("http://schemas.microsoft.com/identity/claims/objectidentifier",objId),
            };
            var authSettings = new AadAuthenticationSettings([appId], [audience], [issuer], new MockIssuerSigningTokenProvider(new List<SecurityKey> { tokenGenerator.SecurityKey }, context.logger), true);

            var token = tokenGenerator.CreateToken(tokenClaims, DateTime.Now.AddMinutes(10));
            ValidateConnectionsWithToken(objId, token, nodes, authSettings);
        }

        [Test, Order(2)]
        [Category("CLUSTER-AUTH"), CancelAfter(60000)]
        public void ValidateClusterAuthWithGroupOid()
        {
            var nodes = 2;
            var audience = Guid.NewGuid().ToString();
            JwtTokenGenerator tokenGenerator = new JwtTokenGenerator(issuer, audience);

            var appId = Guid.NewGuid().ToString();
            var objId = Guid.NewGuid().ToString();
            var groupIds = new List<string> { Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
            var tokenClaims = new List<Claim>
            {
                new Claim("appidacr","1"),
                new Claim("appid", appId),
                new Claim("http://schemas.microsoft.com/identity/claims/objectidentifier", objId),
                new Claim("groups", string.Join(',', groupIds)),
            };
            var authSettings = new AadAuthenticationSettings([appId], [audience], [issuer], new MockIssuerSigningTokenProvider(new List<SecurityKey> { tokenGenerator.SecurityKey }, context.logger), true);
            var token = tokenGenerator.CreateToken(tokenClaims, DateTime.Now.AddMinutes(10));
            ValidateConnectionsWithToken(groupIds.First(), token, nodes, authSettings);
        }

        private void ValidateConnectionsWithToken(string aclUsername, string token, int nodeCount, AadAuthenticationSettings authenticationSettings)
        {
            var userCredential = new ServerCredential { user = aclUsername, IsAdmin = true, IsClearText = true };
            var clientCredentials = new ServerCredential { user = aclUsername, password = token };
            context.GenerateCredentials([userCredential]);
            context.CreateInstances(nodeCount, useAcl: true, clusterCreds: clientCredentials, authenticationSettings: authenticationSettings);


            context.CreateConnection(useTLS: false, clientCreds: clientCredentials);

            for (int i = 0; i < nodeCount; i++)
            {
                context.clusterTestUtils.Authenticate(i, clientCredentials.user, clientCredentials.password, context.logger);
                context.clusterTestUtils.Meet(i, (i + 1) % nodeCount, context.logger);
                var ex = Assert.Throws<AssertionException>(() => context.clusterTestUtils.Authenticate(i, "randomUserId", clientCredentials.password, context.logger));
                ClassicAssert.AreEqual("WRONGPASS Invalid username/password combination", ex.Message);
            }

        }
    }
}