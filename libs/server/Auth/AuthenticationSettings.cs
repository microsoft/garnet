// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.server.Auth.Aad;

namespace Garnet.server.Auth
{
    /// <summary>
    /// Authentication mode
    /// </summary>
    public enum GarnetAuthenticationMode
    {
        /// <summary>
        /// No auth - Garnet accepts any and all connections
        /// </summary>
        NoAuth,

        /// <summary>
        /// Password - Garnet accepts connections with correct connection string
        /// </summary>
        Password,

        /// <summary>
        /// AAD - Garnet accepts connection with correct AAD principal
        /// In AAD mode, token may expire. Clients are expected to periodically refresh token with Garnet by running AUTH command.
        /// </summary>
        Aad,

        /// <summary>
        /// ACL - Garnet validates new connections and commands against configured ACL users and access rules.
        /// </summary>
        ACL
    }

    /// <summary>
    /// Authentication settings
    /// </summary>
    public interface IAuthenticationSettings : IDisposable
    {
        /// <summary>
        /// Create an authenticator using the current settings.
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with.</param>
        IGarnetAuthenticator CreateAuthenticator(StoreWrapper storeWrapper);
    }

    /// <summary>
    /// No auth settings
    /// </summary>
    public class NoAuthSettings : IAuthenticationSettings
    {
        /// <summary>
        /// Creates a no auth authenticator
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with.</param>
        public IGarnetAuthenticator CreateAuthenticator(StoreWrapper storeWrapper)
        {
            return new GarnetNoAuthAuthenticator();
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            // No-op
        }
    }

    /// <summary>
    /// Password auth settings
    /// </summary>
    public class PasswordAuthenticationSettings : IAuthenticationSettings
    {
        private readonly byte[] _pwd;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pwd">The password</param>
        public PasswordAuthenticationSettings(string pwd)
        {
            if (string.IsNullOrEmpty(pwd))
            {
                throw new Exception("Password cannot be null.");
            }
            _pwd = System.Text.Encoding.ASCII.GetBytes(pwd);
        }

        /// <summary>
        /// Creates a password auth authenticator
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with.</param>
        public IGarnetAuthenticator CreateAuthenticator(StoreWrapper storeWrapper)
        {
            return new GarnetPasswordAuthenticator(_pwd);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            // No op
        }
    }

    /// <summary>
    /// AAD auth settings
    /// </summary>
    public class AadAuthenticationSettings : IAuthenticationSettings
    {
        private readonly IReadOnlyCollection<string> _authorizedAppIds;
        private readonly IReadOnlyCollection<string> _audiences;
        private readonly IReadOnlyCollection<string> _issuers;
        private IssuerSigningTokenProvider _signingTokenProvider;
        private bool _disposed;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="authorizedAppIds">Allowed app Ids</param>
        /// <param name="audiences">Allowed audiences</param>
        /// <param name="issuers">Allowed issuers</param>
        /// <param name="signingTokenProvier">Signing token provider</param>
        public AadAuthenticationSettings(string[] authorizedAppIds, string[] audiences, string[] issuers, IssuerSigningTokenProvider signingTokenProvier)
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

            if (signingTokenProvier == null)
            {
                throw new Exception("Signing token provider cannot be null.");
            }

            _authorizedAppIds = new HashSet<string>(authorizedAppIds, StringComparer.OrdinalIgnoreCase);
            _audiences = new HashSet<string>(audiences, StringComparer.OrdinalIgnoreCase);
            _issuers = new HashSet<string>(issuers, StringComparer.OrdinalIgnoreCase);
            _signingTokenProvider = signingTokenProvier;
        }

        /// <summary>
        /// Creates an AAD auth authenticator
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with.</param>
        public IGarnetAuthenticator CreateAuthenticator(StoreWrapper storeWrapper)
        {
            return new GarnetAadAuthenticator(_authorizedAppIds, _audiences, _issuers, _signingTokenProvider, storeWrapper.logger);
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

    /// <summary>
    /// ACL authentication settings
    /// </summary>
    public class AclAuthenticationSettings : IAuthenticationSettings
    {
        /// <summary>
        /// Location of a the ACL configuration file to load users from
        /// </summary>
        public readonly string AclConfigurationFile;

        /// <summary>
        /// Default user password, in case aclConfiguration file is undefined or does not specify default password
        /// </summary>
        public readonly string DefaultPassword;

        /// <summary>
        /// Creates and initializes new ACL authentication settings
        /// </summary>
        /// <param name="aclConfigurationFile">Location of the ACL configuration file</param>
        /// <param name="defaultPassword">Optional default password, if not defined through aclConfigurationFile</param>
        public AclAuthenticationSettings(string aclConfigurationFile, string defaultPassword = "")
        {
            AclConfigurationFile = aclConfigurationFile;
            DefaultPassword = defaultPassword;
        }

        /// <summary>
        /// Creates an ACL authenticator
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with.</param>
        public IGarnetAuthenticator CreateAuthenticator(StoreWrapper storeWrapper)
        {
            return new GarnetACLAuthenticator(storeWrapper.accessControlList, storeWrapper.logger);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            // No-op
        }
    }
}