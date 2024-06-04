// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Auth.Settings
{
    /// <summary>
    /// ACL authentication settings
    /// </summary>
    public abstract class AclAuthenticationSettings : IAuthenticationSettings
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
            return CreateAuthenticatorInternal(storeWrapper);
        }

        /// <summary>
        /// Creates the internal implementation specific ACL authenticator.
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with. </param>
        /// <returns> IGarnetAuthenticator instance </returns>
        protected abstract IGarnetAuthenticator CreateAuthenticatorInternal(StoreWrapper storeWrapper);


        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            // No-op
        }
    }
}