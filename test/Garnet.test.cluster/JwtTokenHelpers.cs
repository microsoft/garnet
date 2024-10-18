// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Garnet.server.Auth.Aad;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;

namespace Garnet.test.cluster
{
    internal class MockIssuerSigningTokenProvider : IssuerSigningTokenProvider
    {
        internal MockIssuerSigningTokenProvider(IReadOnlyCollection<SecurityKey> signingTokens, ILogger logger = null) : base(string.Empty, signingTokens, false, logger)
        { }

    }

    internal class JwtTokenGenerator
    {
        private readonly string Issuer;

        private readonly string Audience;

        // Our random signing key - used to sign and validate the tokens
        public SecurityKey SecurityKey { get; }

        // the signing credentials used by the token handler to sign tokens
        public SigningCredentials SigningCredentials { get; }

        // the token handler we'll use to actually issue tokens
        public readonly JwtSecurityTokenHandler JwtSecurityTokenHandler = new();

        internal JwtTokenGenerator(string issuer, string audience)
        {
            Issuer = issuer;
            Audience = audience;
            SecurityKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("Test secret key for authentication and signing the token to be generated"));
            SigningCredentials = new SigningCredentials(SecurityKey, SecurityAlgorithms.HmacSha256);
        }

        internal string CreateToken(List<Claim> claims, DateTime expiryTime)
        {
            return JwtSecurityTokenHandler.WriteToken(new JwtSecurityToken(Issuer, Audience, claims, expires: expiryTime, signingCredentials: SigningCredentials));
        }

    }
}