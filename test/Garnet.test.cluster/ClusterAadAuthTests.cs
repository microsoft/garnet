// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using Garnet.server.Auth;
using Garnet.server.Auth.Aad;
using Microsoft.IdentityModel.Tokens;
using NUnit.Framework;

namespace Garnet.test.cluster
{
    [TestFixture]
    [NonParallelizable]
    class ClusterAadAuthTests
    {
        ClusterTestContext context;

        readonly HashSet<string> monitorTests = [];

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
        [Category("CLUSTER-AUTH"), Timeout(60000)]
        public void ValidateClusterAuth()
        {
            var nodes = 2;
            var audience = Guid.NewGuid().ToString();
            JwtTokenGenerator tokenGenerator = new JwtTokenGenerator(audience);

            var appId = Guid.NewGuid().ToString();
            var objId = Guid.NewGuid().ToString();
            var tokenClaims = new List<Claim>
            {
                new Claim("appidacr","1"),
                new Claim("appid", appId),
                new Claim("http://schemas.microsoft.com/identity/claims/objectidentifier",objId),
            };
            var authSettings = new AadAuthenticationSettings([appId], [audience], [audience], IssuerSigningTokenProvider.Create(new List<SecurityKey> { tokenGenerator.SecurityKey }, context.logger));
            var token = tokenGenerator.CreateToken(tokenClaims, DateTime.Now.AddMinutes(10));
            // Generate default ACL file

            var userCredential = new ServerCredential { user = objId, IsAdmin = true, IsClearText = true };
            var clientCredentials = new ServerCredential { user = objId, password = token };
            context.GenerateCredentials([userCredential]);
            context.CreateInstances(nodes, useAcl: true, clusterCreds: clientCredentials, authenticationSettings: authSettings);


            context.CreateConnection(useTLS: false, clientCreds: clientCredentials);

            for (int i = 0; i < nodes; i++)
            {
                context.clusterTestUtils.Authenticate(i, clientCredentials.user, clientCredentials.password, context.logger);
                context.clusterTestUtils.Meet(i, (i + 1) % nodes, context.logger);
                var ex = Assert.Throws<AssertionException>(() => context.clusterTestUtils.Authenticate(i, "randomUserId", clientCredentials.password, context.logger));
                Assert.AreEqual("WRONGPASS Invalid username/password combination", ex.Message);
            }
        }
    }
}