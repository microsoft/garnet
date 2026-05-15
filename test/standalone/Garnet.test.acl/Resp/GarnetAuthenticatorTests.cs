// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
    [TestFixture]
    public class GarnetAuthenticatorTests : TestBase
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

            auth.AuthenticateCallback =
                (p, u) =>
                {
                    auth.IsAuthenticated = true;
                    return true;
                };

            using GarnetServer server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, authenticationSettings: authSettings);
            server.Start();

            using var c = TestUtils.GetGarnetClientSession();
            await c.ConnectAsync().ConfigureAwait(false);

            // Initial command should work
            _ = await c.ExecuteAsync("PING").ConfigureAwait(false);

            // Command after auth invalidation fails as no auth
            auth.IsAuthenticated = false;
            try
            {
                _ = await c.ExecuteAsync("PING").ConfigureAwait(false);
                Assert.Fail("Should be denied, user is not authed");
            }
            catch (Exception e)
            {
                ClassicAssert.AreEqual("NOAUTH Authentication required.", e.Message);
            }

            // Re-auth
            _ = await c.ExecuteAsync("AUTH", "bar").ConfigureAwait(false);
            
            // Should be authed again
            _ = await c.ExecuteAsync("PING").ConfigureAwait(false);
        }
    }
}