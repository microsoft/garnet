// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Security.Claims;
using System.Text;
using Garnet.common;
using Garnet.server.Auth;
using Microsoft.Extensions.Time.Testing;
using Microsoft.IdentityModel.Tokens;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Unit tests for <see cref="GarnetAadAuthenticator"/> covering the
    /// authorization window via a <see cref="FakeTimeProvider"/>-backed
    /// <see cref="CoarseTimeProvider"/>. Same code path as production —
    /// <see cref="GarnetAadAuthenticator.IsAuthenticated"/> always reads from
    /// the injected <see cref="CoarseTimeProvider"/>; advancing the fake's clock
    /// drives the very same cache-refresh callback the production cache uses.
    /// </summary>
    [TestFixture]
    public class GarnetAadAuthenticatorTests
    {
        // Microsoft Entra tenant ID — a public identifier (NOT a secret), used only to
        // construct the issuer URL for test-only JWTs signed by a locally-generated key.
        private const string Issuer = "https://sts.windows.net/975f013f-7f24-47e8-a7d3-abc4752bf346/";

        private static (GarnetAadAuthenticator authenticator, CoarseTimeProvider coarseTime, string token, string objId) BuildAuthenticator(
            FakeTimeProvider timeProvider,
            DateTime tokenExpiry,
            DateTime? tokenNotBefore = null)
        {
            var audience = Guid.NewGuid().ToString();
            var appId = Guid.NewGuid().ToString();
            var objId = Guid.NewGuid().ToString();
            var tokenGenerator = new JwtTokenGenerator(Issuer, audience);

            var claims = new List<Claim>
            {
                new Claim("appidacr", "1"),
                new Claim("appid", appId),
                new Claim("http://schemas.microsoft.com/identity/claims/objectidentifier", objId),
            };

            var token = new System.IdentityModel.Tokens.Jwt.JwtSecurityTokenHandler().WriteToken(
                new System.IdentityModel.Tokens.Jwt.JwtSecurityToken(
                    Issuer,
                    audience,
                    claims,
                    notBefore: tokenNotBefore,
                    expires: tokenExpiry,
                    signingCredentials: tokenGenerator.SigningCredentials));

            var coarseTime = CoarseTimeProvider.Create(timeProvider);
            var authenticator = new GarnetAadAuthenticator(
                authorizedAppIds: new HashSet<string> { appId },
                audiences: new HashSet<string> { audience },
                issuers: new HashSet<string> { Issuer },
                signingTokenProvider: new MockIssuerSigningTokenProvider(new List<SecurityKey> { tokenGenerator.SecurityKey }),
                validateUsername: true,
                logger: null,
                coarseTime: coarseTime);

            return (authenticator, coarseTime, token, objId);
        }

        [Test]
        public void DefaultCoarseTime_FallsBackToSystem()
        {
            // No coarseTime argument → must default to CoarseTimeProvider.System without throwing.
            var authenticator = new GarnetAadAuthenticator(
                authorizedAppIds: new HashSet<string> { "appid" },
                audiences: new HashSet<string> { "aud" },
                issuers: new HashSet<string> { Issuer },
                signingTokenProvider: new MockIssuerSigningTokenProvider(new List<SecurityKey>()),
                validateUsername: false,
                logger: null);

            // Pre-authentication: IsAuthenticated must be false (no token validated yet).
            ClassicAssert.IsFalse(authenticator.IsAuthenticated);
        }

        [Test]
        public void Authenticate_ReturnsTrue_WhenTokenInsideValidityWindow()
        {
            var now = DateTimeOffset.UtcNow;
            var timeProvider = new FakeTimeProvider(now);

            var (auth, coarseTime, token, objId) = BuildAuthenticator(timeProvider, tokenExpiry: now.AddMinutes(10).UtcDateTime);
            using var _ = coarseTime;

            ClassicAssert.IsTrue(auth.Authenticate(Encoding.UTF8.GetBytes(token), Encoding.UTF8.GetBytes(objId)));
            ClassicAssert.IsTrue(auth.IsAuthenticated);
        }

        [Test]
        public void IsAuthenticated_GoesFalse_AfterTokenExpires()
        {
            var now = DateTimeOffset.UtcNow;
            var timeProvider = new FakeTimeProvider(now);

            var (auth, coarseTime, token, objId) = BuildAuthenticator(timeProvider, tokenExpiry: now.AddMinutes(5).UtcDateTime);
            using var _ = coarseTime;

            ClassicAssert.IsTrue(auth.Authenticate(Encoding.UTF8.GetBytes(token), Encoding.UTF8.GetBytes(objId)));
            ClassicAssert.IsTrue(auth.IsAuthenticated, "token should be valid immediately after authentication");

            // Advancing the FakeTimeProvider fires CoarseTimeProvider's internal Timer (registered via
            // TimeProvider.CreateTimer), refreshing the cached UTC ticks — same code path as production.
            // JwtSecurityToken truncates ValidTo to whole seconds, so step past the boundary.
            timeProvider.Advance(TimeSpan.FromMinutes(5) + TimeSpan.FromSeconds(2));

            ClassicAssert.IsFalse(auth.IsAuthenticated, "IsAuthenticated must observe the advanced cache");
        }

        [Test]
        public void IsAuthenticated_StaysTrue_WhileTimeStillInsideWindow()
        {
            var now = DateTimeOffset.UtcNow;
            var timeProvider = new FakeTimeProvider(now);

            var (auth, coarseTime, token, objId) = BuildAuthenticator(timeProvider, tokenExpiry: now.AddHours(1).UtcDateTime);
            using var _ = coarseTime;

            ClassicAssert.IsTrue(auth.Authenticate(Encoding.UTF8.GetBytes(token), Encoding.UTF8.GetBytes(objId)));

            for (var i = 0; i < 5; i++)
            {
                timeProvider.Advance(TimeSpan.FromMinutes(10));
                ClassicAssert.IsTrue(auth.IsAuthenticated, $"should still be authenticated after advancing {(i + 1) * 10} minutes");
            }
        }

        [Test]
        public void Authenticate_ReturnsFalse_OnMalformedToken()
        {
            var timeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow);

            var (auth, coarseTime, _, _) = BuildAuthenticator(timeProvider, tokenExpiry: DateTimeOffset.UtcNow.AddMinutes(10).UtcDateTime);
            using var _scope = coarseTime;

            ClassicAssert.IsFalse(auth.Authenticate(Encoding.UTF8.GetBytes("not.a.valid.jwt"), Encoding.UTF8.GetBytes("user")));
            ClassicAssert.IsFalse(auth.IsAuthenticated, "failed authentication must not leave the authenticator in an authorized state");
        }

        [Test]
        public void FailedReauthentication_ClearsPreviouslyAuthorizedState()
        {
            var now = DateTimeOffset.UtcNow;
            var timeProvider = new FakeTimeProvider(now);

            var (auth, coarseTime, token, objId) = BuildAuthenticator(timeProvider, tokenExpiry: now.AddHours(1).UtcDateTime);
            using var _ = coarseTime;

            ClassicAssert.IsTrue(auth.Authenticate(Encoding.UTF8.GetBytes(token), Encoding.UTF8.GetBytes(objId)));
            ClassicAssert.IsTrue(auth.IsAuthenticated);

            // Covers the catch-block reset of _validFromTicks/_validToTicks — a subsequent failed
            // Authenticate() must invalidate the previously cached authorized state.
            ClassicAssert.IsFalse(auth.Authenticate(Encoding.UTF8.GetBytes("garbage"), Encoding.UTF8.GetBytes(objId)));
            ClassicAssert.IsFalse(auth.IsAuthenticated);
        }
    }
}