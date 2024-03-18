// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using Garnet.server.Auth.Aad;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;

namespace Garnet.server.Auth
{
    class GarnetAadAuthenticator : IGarnetAuthenticator
    {
        private static JwtSecurityTokenHandler _tokenHandler = new JwtSecurityTokenHandler();

        private const string _appIdAcrClaim = "appidacr";
        private const string _scopeClaim = "http://schemas.microsoft.com/identity/claims/scope";
        private const string _appIdClaim = "appid";

        public bool IsAuthenticated => IsAuthorized();

        public bool CanAuthenticate => true;

        public bool HasACLSupport => false;

        private bool _authorized;
        private DateTime _validFrom;
        private DateTime _validateTo;
        private readonly IReadOnlyCollection<string> _authorizedAppIds;
        private readonly IReadOnlyCollection<string> _audiences;
        private readonly IReadOnlyCollection<string> _issuers;
        private readonly IssuerSigningTokenProvider _signingTokenProvider;

        private readonly ILogger _logger;

        public GarnetAadAuthenticator(
            IReadOnlyCollection<string> authorizedAppIds,
            IReadOnlyCollection<string> audiences,
            IReadOnlyCollection<string> issuers,
            IssuerSigningTokenProvider signingTokenProvider,
            ILogger logger)
        {
            _authorizedAppIds = authorizedAppIds;
            _signingTokenProvider = signingTokenProvider;
            _audiences = audiences;
            _issuers = issuers;
            _logger = logger;
        }

        public bool Authenticate(ReadOnlySpan<byte> password, ReadOnlySpan<byte> username)
        {
            try
            {
                var parameters = new TokenValidationParameters
                {
                    ValidateAudience = true,
                    ValidIssuers = _issuers,
                    ValidAudiences = _audiences,
                    IssuerSigningKeys = _signingTokenProvider.SigningTokens
                };

                var identity = _tokenHandler.ValidateToken(Encoding.UTF8.GetString(password), parameters, out var token);

                _validFrom = token.ValidFrom;
                _validateTo = token.ValidTo;

                _authorized = IsIdentityAuthorized(identity);
                _logger?.LogInformation($"Authentication successful. Token valid from {_validFrom} to {_validateTo}");

                return IsAuthorized();
            }
            catch (Exception ex)
            {
                _authorized = false;
                _validFrom = DateTime.MinValue;
                _validateTo = DateTime.MinValue;
                _logger?.LogError(ex, "Authentication failed");
                return false;
            }
        }

        private bool IsIdentityAuthorized(ClaimsPrincipal identity)
        {
            var claims = identity.Claims
                .GroupBy(claim => claim.Type)
                .ToDictionary(group => group.Key, group => string.Join(',', group.Select(c => c.Value)), StringComparer.OrdinalIgnoreCase);

            return IsApplicationPrincipal(claims) && IsApplicationAuthorized(claims);
        }

        private bool IsApplicationAuthorized(IDictionary<string, string> claims)
        {
            return claims.TryGetValue(_appIdClaim, out var appId) && _authorizedAppIds.Contains(appId);
        }

        private bool IsAuthorized()
        {
            var now = DateTime.UtcNow;
            return _authorized && now >= _validFrom && now <= _validateTo;
        }

        private static bool IsApplicationPrincipal(IDictionary<string, string> claims)
        {
            // appidacr ndicates authentication method of the client. For a public client, the value is 0. When you use the client ID and client secret, the value is 1.
            // When you use a client certificate for authentication, the value is 2.
            if (claims.TryGetValue(_appIdAcrClaim, out var appIdAcr) &&
                !string.IsNullOrEmpty(appIdAcr) &&
                string.Compare(appIdAcr, "0", StringComparison.OrdinalIgnoreCase) > 0)
            {
                return !claims.TryGetValue(_scopeClaim, out var scope) ||
                    string.IsNullOrWhiteSpace(scope);
            }

            return false;
        }
    }
}