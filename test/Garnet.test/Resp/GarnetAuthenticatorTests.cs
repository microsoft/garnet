// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.server;
using Garnet.server.Auth;
using Garnet.server.Auth.Settings;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.Resp
{
    /// <summary>
    /// Tests generic to all <see cref="IGarnetAuthenticator"/>s.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class GarnetAuthenticatorTests : AllureTestBase
    {
        private delegate bool AuthenticateDelegate(ReadOnlySpan<byte> password, ReadOnlySpan<byte> username);

        private sealed class MockAuthenticationSettings : IAuthenticationSettings
        {
            public Func<StoreWrapper, IGarnetAuthenticator> CreateAuthenticatorCallback { get; set; } = static sw => throw new NotImplementedException();

            public Action DisposeCallback { get; set; } = static () => { };

            public IGarnetAuthenticator CreateAuthenticator(StoreWrapper storeWrapper)
            => CreateAuthenticatorCallback(storeWrapper);

            public void Dispose()
            => DisposeCallback();
        }

        private sealed class MockAuthenticator : IGarnetAuthenticator
        {
            public bool IsAuthenticated { get; set; }

            public bool CanAuthenticate { get; set; }

            public bool HasACLSupport { get; set; }

            public AuthenticateDelegate AuthenticateCallback { get; set; } = static (pwd, un) => throw new NotImplementedException();

            public bool Authenticate(ReadOnlySpan<byte> password, ReadOnlySpan<byte> username)
            => AuthenticateCallback(password, username);
        }

        [Test]
        public async Task InvalidatingAuthorizationAsync()
        {
            MockAuthenticationSettings authSettings = new();
            MockAuthenticator auth = new();
            authSettings.CreateAuthenticatorCallback = sw => auth;

            auth.CanAuthenticate = true;
            auth.HasACLSupport = false;
            auth.IsAuthenticated = false;

            int authCalls = 0;

            auth.AuthenticateCallback =
                (p, u) =>
                {
                    if (authCalls == 0)
                    {
                        ClassicAssert.AreEqual("default", Encoding.UTF8.GetString(u));
                    }
                    else
                    {
                        ClassicAssert.AreEqual("foo", Encoding.UTF8.GetString(u));
                    }

                    authCalls++;

                    auth.IsAuthenticated = true;
                    return true;
                };

            using GarnetServer server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, authenticationSettings: authSettings);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            c.Connect();

            // Initial command runs under default user
            await c.ExecuteAsync("PING");
            ClassicAssert.AreEqual(1, authCalls);

            // Auth as proper user, should get another call
            await c.ExecuteAsync("AUTH", "foo", "bar");
            ClassicAssert.AreEqual(2, authCalls);

            await c.ExecuteAsync("PING");
            ClassicAssert.AreEqual(2, authCalls);

            // Command after auth invalidation fails as no auth
            auth.IsAuthenticated = false;
            try
            {
                await c.ExecuteAsync("PING");
                Assert.Fail("Should be denied, user is not authed");
            }
            catch (Exception e)
            {
                ClassicAssert.AreEqual("NOAUTH Authentication required.", e.Message);
            }

            await c.ExecuteAsync("AUTH", "foo", "bar");
            ClassicAssert.AreEqual(3, authCalls);
        }
    }
}