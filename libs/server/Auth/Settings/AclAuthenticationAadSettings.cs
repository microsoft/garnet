// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server.Auth.Settings
{
    /// <summary>
    /// ACL authentication with AAD settings
    /// </summary>
    public class AclAuthenticationAadSettings : AclAuthenticationSettings
    {

        AadAuthenticationSettings _aadAuthenticationSettings;

        /// <summary>
        /// Creates and initializes new ACL authentication settings
        /// </summary>
        /// <param name="aclConfigurationFile">Location of the ACL configuration file</param>
        /// <param name="defaultPassword">Optional default password, if not defined through aclConfigurationFile</param>
        /// <param name="aadAuthenticationSettings"> AAD settings used for authentication </param>
        public AclAuthenticationAadSettings(string aclConfigurationFile, string defaultPassword = "", AadAuthenticationSettings aadAuthenticationSettings = null) : base(aclConfigurationFile, defaultPassword)
        {
            _aadAuthenticationSettings = aadAuthenticationSettings;
        }

        /// <summary>
        /// Creates an ACL authenticator
        /// </summary>
        /// <param name="storeWrapper">The main store the authenticator will be associated with.</param>

        protected override IGarnetAuthenticator CreateAuthenticatorInternal(StoreWrapper storeWrapper)
        {
            return new GarnetAclWithAadAuthenticator(storeWrapper.accessControlList, _aadAuthenticationSettings.CreateAuthenticator(storeWrapper), storeWrapper.logger);
        }
    }
}