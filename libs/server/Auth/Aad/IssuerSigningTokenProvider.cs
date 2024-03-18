// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Garnet.server.Auth.Aad
{
    /// <summary>
    /// Provides signing tokens from AAD authority for token validation purposes.
    /// </summary>
    public class IssuerSigningTokenProvider : IDisposable
    {
        private const string OpenIdConfigurationAddressFormat = "{0}/common/.well-known/openid-configuration";

        /// <summary>
        /// The issuer signing keys
        /// </summary>
        public IReadOnlyCollection<SecurityKey> SigningTokens => _signingTokens;

        private readonly string _authority;
        private IReadOnlyCollection<SecurityKey> _signingTokens;
        private bool _disposed;
        private Timer _refreshTimer;

        private readonly ILogger _logger;

        private IssuerSigningTokenProvider(string authority, IReadOnlyCollection<SecurityKey> signingTokens, ILogger logger)
        {
            _authority = authority;
            _refreshTimer = new Timer(RefreshSigningTokens, null, TimeSpan.Zero, TimeSpan.FromDays(1));
            _signingTokens = signingTokens;

            _logger = logger;
        }

        private void RefreshSigningTokens(object _)
        {
            try
            {
                _signingTokens = RetrieveSigningTokens(_authority);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to retrieve issuer signing tokens.");
                // Ignore
            }
        }

        private static IReadOnlyCollection<SecurityKey> RetrieveSigningTokens(string authority)
        {
            var configUrl = string.Format(OpenIdConfigurationAddressFormat, authority);

            var configManager = new ConfigurationManager<OpenIdConnectConfiguration>(configUrl, new OpenIdConnectConfigurationRetriever(), new HttpDocumentRetriever());
            var doc = configManager.GetConfigurationAsync().GetAwaiter().GetResult();

            return doc.SigningKeys.ToList();
        }

        /// <summary>
        /// Dispose
        /// </summary>
        /// <param name="disposing">Flag to run disposal logic</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _refreshTimer?.Dispose();
                    _refreshTimer = null;
                }

                _disposed = true;
            }
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Creates an instance of IssuerSigningTokenProvider.
        /// </summary>
        /// <param name="authority">The AAD authority</param>
        /// <param name="logger">The logger</param>
        public static IssuerSigningTokenProvider Create(string authority, ILogger logger)
        {
            if (string.IsNullOrEmpty(authority))
            {
                throw new Exception("Authority cannot be null");
            }

            var signingTokens = RetrieveSigningTokens(authority);
            return new IssuerSigningTokenProvider(authority, signingTokens, logger);
        }
    }
}