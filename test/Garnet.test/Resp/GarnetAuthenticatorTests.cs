// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading.Tasks;
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
    public class GarnetAuthenticatorTests
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

            var authCalls = 0;
            var authingAsFoo = false;
            var authedAsFoo = false;

            auth.AuthenticateCallback =
                (p, u) =>
                {
                    if (!authingAsFoo)
                    {
                        ClassicAssert.AreEqual("default", Encoding.UTF8.GetString(u));
                    }
                    else
                    {
                        ClassicAssert.AreEqual("foo", Encoding.UTF8.GetString(u));
                        authedAsFoo = true;
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
            _ = await c.ExecuteAsync("PING");

            // Auth as proper user, should get another call
            authingAsFoo = true;
            _ = await c.ExecuteAsync("AUTH", "foo", "bar");
            ClassicAssert.IsTrue(authedAsFoo);

            _ = await c.ExecuteAsync("PING");

            // Command after auth invalidation fails as no auth

            var oldAuthCalls = authCalls;
            auth.IsAuthenticated = false;
            try
            {
                _ = await c.ExecuteAsync("PING");
                Assert.Fail("Should be denied, user is not authed");
            }
            catch (Exception e)
            {
                ClassicAssert.AreEqual("NOAUTH Authentication required.", e.Message);
            }

            _ = await c.ExecuteAsync("AUTH", "foo", "bar");
            ClassicAssert.True(authCalls > oldAuthCalls);
        }
    }
}