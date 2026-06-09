// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.server.Auth.Aad;

namespace Garnet.server.Auth.Settings
{
    /// <summary>
    /// AAD auth settings
    /// </summary>
    public class AadAuthenticationSettings : IAuthenticationSettings
    {
        private readonly IReadOnlyCollection<string> _authorizedAppIds;
        private readonly IReadOnlyCollection<string> _audiences;
        private readonly IReadOnlyCollection<string> _issuers;
        private IssuerSigningTokenProvider _signingTokenProvider;
        private bool _validateUsername;
        private bool _disposed;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="authorizedAppIds">Allowed app Ids</param>
        /// <param name="audiences">Allowed audiences</param>
        /// <param name="issuers">Allowed issuers</param>
        /// <param name="signingTokenProvider">Signing token provider</param>
        /// <param name="validateUsername"> whether to validate username or not. </param>
        public AadAuthenticationSettings(string[] authorizedAppIds, string[] audiences, string[] issuers, IssuerSigningTokenProvider signingTokenProvider, bool validateUsername = false)
        {
            if (authorizedAppIds == null || authorizedAppIds.Length == 0)
            {
                throw new Exception("Authorized app Ids cannot be empty.");
            }

            if (audiences == null || audiences.Length == 0)
            {
                throw new Exception("Audiences cannot be empty.");
            }

            if (issuers == null || issuers.Length == 0)
            {
                throw new Exception("Issuers cannot be empty.");
            }

            if (signingTokenProvider == null)
            {
                throw new Exception("Signing token provider cannot be null.");
            }

            _authorizedAppIds = new HashSet<string>(authorizedAppIds, StringComparer.OrdinalIgnoreCase);
            _audiences = new HashSet<string>(audiences, StringComparer.OrdinalIgnoreCase);
            _issuers = new HashSet<string>(issuers, StringComparer.OrdinalIgnoreCase);
            _signingTokenProvider = signingTokenProvider;
            _validateUsername = validateUsername;
        }

        /// <summary>
        /// Creates an AAD auth authenticator
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with.</param>
        public IGarnetAuthenticator CreateAuthenticator(StoreWrapper storeWrapper)
        {
            return new GarnetAadAuthenticator(_authorizedAppIds, _audiences, _issuers, _signingTokenProvider, _validateUsername, storeWrapper.logger);
        }

        /// <summary>
        /// Dispose impl
        /// </summary>
        /// <param name="disposing">Flag to run disposal logic</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _signingTokenProvider?.Dispose();
                    _signingTokenProvider = null;
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
    }
}