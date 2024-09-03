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
using Microsoft.IdentityModel.Validators;

namespace Garnet.server.Auth
{
    class GarnetAadAuthenticator : IGarnetAuthenticator
    {
        private static JwtSecurityTokenHandler _tokenHandler = new JwtSecurityTokenHandler();

        private const string _appIdAcrClaim = "appidacr";
        private const string _scopeClaim = "http://schemas.microsoft.com/identity/claims/scope";
        private const string _appIdClaim = "appid";
        private const string _oidClaim = "http://schemas.microsoft.com/identity/claims/objectidentifier";
        private const string _groupsClaim = "groups";

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
        private readonly bool _validateUsername;

        private readonly ILogger _logger;

        public GarnetAadAuthenticator(
            IReadOnlyCollection<string> authorizedAppIds,
            IReadOnlyCollection<string> audiences,
            IReadOnlyCollection<string> issuers,
            IssuerSigningTokenProvider signingTokenProvider,
            bool validateUsername,
            ILogger logger)
        {
            _authorizedAppIds = authorizedAppIds;
            _signingTokenProvider = signingTokenProvider;
            _audiences = audiences;
            _issuers = issuers;
            _validateUsername = validateUsername;
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
                parameters.EnableAadSigningKeyIssuerValidation();
                var identity = _tokenHandler.ValidateToken(Encoding.UTF8.GetString(password), parameters, out var token);

                _validFrom = token.ValidFrom;
                _validateTo = token.ValidTo;

                _authorized = IsIdentityAuthorized(identity, username);
                _logger?.LogInformation("Authentication successful. Token valid from {validFrom} to {validateTo}", _validFrom, _validateTo);

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

        private bool IsIdentityAuthorized(ClaimsPrincipal identity, ReadOnlySpan<byte> userName)
        {
            var claims = identity.Claims
                .GroupBy(claim => claim.Type)
                .ToDictionary(group => group.Key, group => string.Join(',', group.Select(c => c.Value)), StringComparer.OrdinalIgnoreCase);

            bool isValid = IsApplicationPrincipal(claims) && IsApplicationAuthorized(claims);
            return !_validateUsername ? isValid : _validateUsername && IsUserNameAuthorized(claims, userName);
        }
        private bool IsApplicationAuthorized(IDictionary<string, string> claims)
        {
            return claims.TryGetValue(_appIdClaim, out var appId) && _authorizedAppIds.Contains(appId);
        }

        /// <summary>
        /// Validates the username for OID or Group claim. A given token issued to client object maybe part of a
        /// AAD Group or an ObjectID incase of Application. We validate for OID first and then all groups.
        /// </summary>
        /// <param name="claims"> token claims mapping </param>
        /// <param name="userName"> input username </param>
        private bool IsUserNameAuthorized(IDictionary<string, string> claims, ReadOnlySpan<byte> userName)
        {
            var userNameStr = Encoding.UTF8.GetString(userName);
            if (claims.TryGetValue(_oidClaim, out var oid) && oid.Equals(userNameStr, StringComparison.InvariantCultureIgnoreCase))
            {
                return true;
            }
            if (claims.TryGetValue(_groupsClaim, out var groups))
            {
                var splitGroups = groups.Split(",");
                foreach (var group in splitGroups)
                {
                    if (group.Equals(userNameStr, StringComparison.InvariantCultureIgnoreCase))
                    {
                        return true;
                    }
                }
            }
            return false;
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