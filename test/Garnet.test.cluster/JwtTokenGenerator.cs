// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;

namespace Garnet.test.cluster
{
    internal class JwtTokenGenerator
    {
        private readonly string Issuer;

        // Our random signing key - used to sign and validate the tokens
        public SecurityKey SecurityKey { get; }

        // the signing credentials used by the token handler to sign tokens
        public SigningCredentials SigningCredentials { get; }

        // the token handler we'll use to actually issue tokens
        public readonly JwtSecurityTokenHandler JwtSecurityTokenHandler = new();

        internal JwtTokenGenerator(string issuer)
        {
            Issuer = issuer;
            SecurityKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes("Test secret key for authentication and signing the token to be generated"));
            SigningCredentials = new SigningCredentials(SecurityKey, SecurityAlgorithms.HmacSha256);
        }

        internal string CreateToken(List<Claim> claims, DateTime expiryTime)
        {
            return JwtSecurityTokenHandler.WriteToken(new JwtSecurityToken(Issuer, Issuer, claims, expires: expiryTime, signingCredentials: SigningCredentials));
        }

    }
}